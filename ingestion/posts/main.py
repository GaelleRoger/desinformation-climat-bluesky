# ---------------------------------------------------------------------------
# Imports stdlib
# ---------------------------------------------------------------------------
import json
import os
import sys
import time
import datetime as dt
from datetime import datetime, timezone

# Imports tiers
import requests
from google.cloud import secretmanager, storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ["PROJECT_ID"]
BUCKET = f"{PROJECT_ID}-bronze-posts"

SEARCH_TERMS = [
    "climat", "réchauffement climatique", "dérèglement climatique",
    "GIEC", "gaz à effet de serre", "carbone", "canicule",
    "transition écologique", "climato", "ecolo", "changement climatique",
]
SEARCH_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
AUTH_URL   = "https://bsky.social/xrpc/com.atproto.server.createSession"

LANG            = "fr"
SORT            = "latest"
PAGE_SIZE       = 100
REQUEST_TIMEOUT = 20
RETRY_ON_429    = 3

HEADERS = {"User-Agent": "bluesky-covoiturage-fetcher/1.0"}

# ---------------------------------------------------------------------------
# Secrets & authentification
# ---------------------------------------------------------------------------

def get_secret(name: str) -> str:
    """Lit la dernière version d'un secret depuis Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    path = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=path).payload.data.decode("utf-8")


def authenticate(session) -> str:
    """Ouvre une session Bluesky et retourne le token JWT."""
    handle = get_secret("bsky-handle")
    password = get_secret("bsky-app-password")
    resp = session.post(AUTH_URL,
                        json={"identifier": handle, "password": password},
                        timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()["accessJwt"]

# ---------------------------------------------------------------------------
# Appels API Bluesky
# ---------------------------------------------------------------------------

def _get_with_retry(session, params):
    """GET avec gestion simple des erreurs et du rate-limit (HTTP 429)."""
    for attempt in range(1, RETRY_ON_429 + 1):
        try:
            resp = session.get(
                SEARCH_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException as exc:
            print(f"  [!] Erreur réseau ({params.get('q')}): {exc}", file=sys.stderr)
            return None

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            wait = 2 * attempt
            print(
                f"  [!] Rate-limit atteint, nouvelle tentative dans {wait}s "
                f"(essai {attempt}/{RETRY_ON_429})",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue

        print(
            f"  [!] HTTP {resp.status_code} pour '{params.get('q')}': "
            f"{resp.text[:200]}",
            file=sys.stderr,
        )
        return None

    return None


def search_posts_for_term(term, lang, target_count, session):
    """Récupère jusqu'à `target_count` posts pour un terme donné, avec pagination."""
    collected = []
    cursor = None

    while len(collected) < target_count:
        params = {
            "q": term,
            "lang": lang,
            "sort": SORT,
            "limit": min(PAGE_SIZE, target_count - len(collected)),
        }
        if cursor:
            params["cursor"] = cursor

        data = _get_with_retry(session, params)
        if data is None:
            break

        posts = data.get("posts", [])
        if not posts:
            break

        collected.extend(posts)

        cursor = data.get("cursor")
        if not cursor:
            break  # Plus de pages disponibles.

        time.sleep(0.3)  # Petite pause pour rester courtois avec l'API.

    return collected

# ---------------------------------------------------------------------------
# Traitement & normalisation
# ---------------------------------------------------------------------------

def _to_web_url(uri, handle):
    """Convertit un at:// URI en URL web bsky.app cliquable."""
    # Format URI : at://did:plc:xxxx/app.bsky.feed.post/<rkey>
    if not uri or not handle:
        return ""
    rkey = uri.rsplit("/", 1)[-1]
    return f"https://bsky.app/profile/{handle}/post/{rkey}"


def _parse_date(value):
    """Parse une date ISO 8601 ; renvoie datetime.min si invalide."""
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def extract_fields(post):
    """Extrait les champs utiles d'un post brut de l'API."""
    author = post.get("author", {}) or {}
    record = post.get("record", {}) or {}

    uri    = post.get("uri", "")
    handle = author.get("handle", "")

    return {
        "uri":          uri,
        "url":          _to_web_url(uri, handle),
        "auteur_handle": handle,
        "auteur_nom":   author.get("displayName", ""),
        "texte":        record.get("text", ""),
        "date":         record.get("createdAt", ""),
        "langues":      record.get("langs", []),
        "likes":        post.get("likeCount", 0),
        "reposts":      post.get("repostCount", 0),
        "reponses":     post.get("replyCount", 0),
        "citations":    post.get("quoteCount", 0),
    }


def merge_dedupe_sort(all_posts, limit):
    """Fusionne, dédoublonne par URI, trie par date décroissante, tronque à `limit`."""
    unique = {}
    for post in all_posts:
        uri = post.get("uri")
        if uri and uri not in unique:
            unique[uri] = post

    extracted = [extract_fields(p) for p in unique.values()]
    extracted.sort(key=lambda x: _parse_date(x["date"]), reverse=True)
    return extracted[:limit]

# ---------------------------------------------------------------------------
# Écriture dans GCS
# ---------------------------------------------------------------------------

def write_to_gcs(posts: list):
    """Écrit un post par ligne (JSONL) — format attendu par les tables externes BigQuery."""
    now = datetime.now(timezone.utc)
    blob_path = f"dt={now:%Y-%m-%d}/posts_{now:%H%M%S}.jsonl"

    # Une ligne par post, chacune enrichie de ses métadonnées d'ingestion
    lines = []
    for post in posts:
        line = {
            "ingested_at": now.isoformat(),
            "search_terms": SEARCH_TERMS,
            **post,
        }
        lines.append(json.dumps(line, ensure_ascii=False))
    content = "\n".join(lines)

    storage.Client().bucket(BUCKET).blob(blob_path).upload_from_string(
        content, content_type="application/jsonl"
    )
    print(f"Écrit gs://{BUCKET}/{blob_path} ({len(posts)} posts)")

# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run():
    session = requests.Session()
    token = authenticate(session)
    session.headers.update({"Authorization": f"Bearer {token}"})

    all_posts = []
    for term in SEARCH_TERMS:
        all_posts.extend(search_posts_for_term(term, LANG, 100, session))

    result = merge_dedupe_sort(all_posts, limit=10_000)
    if result:
        write_to_gcs(result)


if __name__ == "__main__":
    run()
