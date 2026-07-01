#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job de chargement des résultats Vertex Batch Prediction dans BigQuery.

Lit les fichiers JSONL produits par Vertex dans gs://<projet>-vertex-staging/output/,
parse les prédictions Gemini, et insère les classifications dans silver.disinfo_labels.

Idempotent : ré-exécutable sans risque (marquage .loaded des fichiers traités).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

from google.cloud import bigquery, storage

PROJECT_ID = os.environ["PROJECT_ID"]
STAGING_BUCKET = f"{PROJECT_ID}-vertex-staging"
TABLE_ID = f"{PROJECT_ID}.silver.disinfo_labels"
MODEL_VERSION = "gemini-2.5-flash"
PROCESSED_SUFFIX = ".loaded"   # marqueur d'idempotence

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)
logger = logging.getLogger(__name__)


def list_output_files() -> list:
    """Liste les fichiers JSONL de prédiction non encore chargés."""
    client = storage.Client()
    blobs = client.list_blobs(STAGING_BUCKET, prefix="output/")
    return [b for b in blobs
            if b.name.endswith(".jsonl") and not b.name.endswith(PROCESSED_SUFFIX)]


def parse_prediction_line(line: str) -> dict | None:
    """Parse une ligne JSONL Vertex et extrait la classification.
    Retourne None si la ligne est mal formée (on saute, on ne casse pas tout)."""
    try:
        obj = json.loads(line)
        post_id = obj.get("post_id")
        if not post_id:
            logger.warning("Ligne sans post_id, ignorée.")
            return None

        # Extraire le texte de la réponse Gemini (structure imbriquée)
        response_text = (
            obj["response"]["candidates"][0]["content"]["parts"][0]["text"]
        )
        # La réponse est elle-même du JSON (grâce à responseMimeType=application/json)
        verdict = json.loads(response_text)

        return {
            "post_id": post_id,
            "is_climate_disinfo": bool(verdict["is_climate_disinfo"]),
            "is_climate_related": bool(verdict["is_climate_related"]),
            "confidence": float(verdict["confidence"]),
            "model_version": MODEL_VERSION,
            "classified_at": datetime.now(timezone.utc).isoformat(),
        }
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Ligne non parseable, ignorée : %s", exc)
        return None


def insert_rows(rows: list) -> None:
    """Insère les classifications dans silver.disinfo_labels.
    Échoue bruyamment si BigQuery retourne des erreurs."""
    if not rows:
        return
    client = bigquery.Client()
    errors = client.insert_rows_json(TABLE_ID, rows)
    if errors:
        raise RuntimeError(f"Erreurs BigQuery : {errors}")
    logger.info("Insérées : %d classifications dans %s", len(rows), TABLE_ID)


def mark_as_processed(blob) -> None:
    """Renomme le fichier en ajoutant .loaded pour ne plus le retraiter."""
    bucket = blob.bucket
    new_name = blob.name + PROCESSED_SUFFIX
    bucket.rename_blob(blob, new_name)
    logger.info("Marqué traité : gs://%s/%s", bucket.name, new_name)


def run() -> None:
    blobs = list_output_files()
    if not blobs:
        logger.info("Aucun fichier de sortie à traiter.")
        return

    logger.info("Fichiers à traiter : %d", len(blobs))
    total_rows = 0

    for blob in blobs:
        logger.info("Traitement de gs://%s/%s", blob.bucket.name, blob.name)
        rows = []
        for line in blob.download_as_text().splitlines():
            if not line.strip():
                continue
            parsed = parse_prediction_line(line)
            if parsed:
                rows.append(parsed)
        insert_rows(rows)
        total_rows += len(rows)
        mark_as_processed(blob)

    logger.info("Terminé : %d classifications chargées au total.", total_rows)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Échec du chargement des résultats")
        sys.exit(1)