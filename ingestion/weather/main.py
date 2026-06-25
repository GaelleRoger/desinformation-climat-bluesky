#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job d'ingestion météo : récupère la météo courante de Paris depuis OpenWeatherMap
et écrit la réponse brute dans gs://<PROJECT_ID>-bronze-weather/dt=YYYY-MM-DD/weather.json.

Conçu pour s'exécuter comme un Cloud Run Job planifié (1×/jour via Cloud Scheduler).

Variables d'environnement attendues :
- PROJECT_ID : identifiant du projet GCP (injecté par le déploiement)

Secret lu via Secret Manager :
- owm-api-key : clé API OpenWeatherMap

Principes :
- pas de secret dans le code (Secret Manager)
- couche bronze immuable (on écrit la réponse brute, sans transformation)
- partitionnement Hive-style (dt=YYYY-MM-DD/) pour les tables externes BigQuery
- idempotence par horodatage à la seconde (deux runs le même jour cohabitent sans s'écraser)
- échec explicite si quelque chose ne va pas (le Workflow doit pouvoir détecter l'erreur)
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests
from google.cloud import secretmanager, storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ID = os.environ.get("PROJECT_ID")
BUCKET_NAME = f"{PROJECT_ID}-bronze-weather" if PROJECT_ID else None

# Paris (centre-ville). On utilise les coordonnées plutôt que le nom de ville
# pour éviter les ambiguïtés de géocodage côté OWM.
LATITUDE = 48.8566
LONGITUDE = 2.3522
LOCATION_LABEL = "Paris,FR"

OWM_URL = "https://api.openweathermap.org/data/2.5/weather"
OWM_SECRET_NAME = "owm-api-key"

REQUEST_TIMEOUT = 20  # secondes

# Configuration du logging pour Cloud Logging
# (Cloud Run capture stdout/stderr automatiquement)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Secret Manager
# ---------------------------------------------------------------------------

def get_secret(secret_name: str) -> str:
    """Lit la dernière version d'un secret depuis Secret Manager."""
    if not PROJECT_ID:
        raise RuntimeError("Variable d'environnement PROJECT_ID manquante.")
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("utf-8")


# ---------------------------------------------------------------------------
# Appel OpenWeatherMap
# ---------------------------------------------------------------------------

def fetch_current_weather(api_key: str) -> dict:
    """Récupère la météo courante de Paris depuis OpenWeatherMap.

    Lève une exception si l'API renvoie un code != 200 ; le Cloud Run Job
    sera marqué en échec, ce qui déclenchera l'alerte Cloud Monitoring.
    """
    params = {
        "lat": LATITUDE,
        "lon": LONGITUDE,
        "appid": api_key,
        "units": "metric",   # températures en °C
        "lang": "fr",        # descriptions en français
    }
    logger.info("Appel OpenWeatherMap pour %s", LOCATION_LABEL)
    response = requests.get(OWM_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()  # 4xx/5xx → exception → job en échec
    return response.json()


# ---------------------------------------------------------------------------
# Écriture GCS (bronze)
# ---------------------------------------------------------------------------

def write_to_gcs(payload: dict) -> str:
    """Écrit le payload brut dans GCS, partitionné par date.

    Format du chemin :
        dt=YYYY-MM-DD/weather_HHMMSS.json

    On enveloppe la réponse OWM dans un objet plus large qui inclut
    l'horodatage d'ingestion et la localisation interrogée — utile pour
    la traçabilité en couche silver.
    """
    now = datetime.now(timezone.utc)
    blob_path = f"dt={now:%Y-%m-%d}/weather_{now:%H%M%S}.json"

    envelope = {
        "ingested_at": now.isoformat(),
        "location": {
            "label": LOCATION_LABEL,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
        },
        "source": "openweathermap-current",
        "payload": payload,   # réponse OWM brute, conservée telle quelle
    }

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(envelope, ensure_ascii=False),
        content_type="application/json",
    )

    full_path = f"gs://{BUCKET_NAME}/{blob_path}"
    logger.info("Météo écrite dans %s", full_path)
    return full_path


# ---------------------------------------------------------------------------
# Programme principal
# ---------------------------------------------------------------------------

def main() -> None:
    if not PROJECT_ID:
        logger.error("PROJECT_ID non défini. Abandon.")
        sys.exit(1)

    try:
        api_key = get_secret(OWM_SECRET_NAME)
        weather_data = fetch_current_weather(api_key)
        gcs_path = write_to_gcs(weather_data)
        logger.info("Job météo terminé avec succès : %s", gcs_path)
    except requests.HTTPError as exc:
        logger.exception("Erreur HTTP côté OpenWeatherMap : %s", exc)
        sys.exit(1)
    except requests.RequestException as exc:
        logger.exception("Erreur réseau lors de l'appel OWM : %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Erreur inattendue : %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()