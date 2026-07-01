import os, json
import vertexai
from vertexai.preview.batch_prediction import BatchPredictionJob
from google.cloud import bigquery, storage

PROJECT = os.environ["PROJECT_ID"]
REGION  = os.environ.get("REGION", "europe-west1")
STAGING = f"{PROJECT}-vertex-staging"
MODEL   = "publishers/google/models/gemini-2.5-flash"

PROMPT_TEMPLATE = """Tu es un expert en désinformation climatique. \
Analyse le post suivant (en français) et réponds STRICTEMENT en JSON.

Définition : la "désinformation climatique" inclut la négation du \
réchauffement d'origine humaine, la minimisation trompeuse de ses effets, \
les fausses causes, ou la décrédibilisation infondée des sciences du climat. \
Une opinion, une émotion, ou une critique de politique publique ne sont PAS \
en soi de la désinformation.

Post : "{text}"

Réponds avec ce JSON exact, sans texte autour :
{{"is_climate_related": <true|false>, "is_climate_disinfo": <true|false>, "confidence": <nombre entre 0 et 1>}}"""

# ─── Étape A : récupérer les posts à classer ──────────────────────────────

def get_unclassified(limit=5000):  #modifiée pour initialiser disinfo label
    """Sélectionne les posts présents en silver mais absents de disinfo_labels.
    Garantit l'idempotence : on ne reclasse jamais un post déjà traité."""
    bq = bigquery.Client()
    q = f"""
        SELECT p.post_id, p.texte
        FROM `{PROJECT}.silver.posts_clean` p
        LEFT JOIN `{PROJECT}.silver.disinfo_labels` d USING (post_id)
        WHERE d.post_id IS NULL
        LIMIT {limit}
    """
    return list(bq.query(q, location=REGION).result())

# ─── Étape B : construire le JSONL au format Gemini ───────────────────────

def build_jsonl(rows) -> str:
    """Une ligne par post, au format attendu par Vertex Batch Prediction Gemini."""
    lines = []
    for r in rows:
        request = {
            "request": {
                "contents": [{
                    "role": "user",
                    "parts": [{"text": PROMPT_TEMPLATE.format(text=r.texte)}]
                }],
                "generationConfig": {
                    "temperature": 0,
                    "responseMimeType": "application/json",
                },
            },
            "post_id": r.post_id,  # on garde la clé pour relier la prédiction au post
        }
        lines.append(json.dumps(request, ensure_ascii=False))
    return "\n".join(lines)

# ─── Étape C : uploader le JSONL dans le staging GCS ──────────────────────

def upload_jsonl(content: str) -> str:
    """Écrit le JSONL dans gs://<projet>-vertex-staging/input/batch.jsonl."""
    blob_path = "input/batch.jsonl"
    storage.Client().bucket(STAGING).blob(blob_path).upload_from_string(
        content, content_type="application/jsonl"
    )
    uri = f"gs://{STAGING}/{blob_path}"
    print(f"JSONL uploadé : {uri}")
    return uri

# ─── Étape D : lancer le Batch Prediction (non bloquant) ──────────────────

def launch_batch_job(input_uri: str):
    """Soumet le job à Vertex et rend la main immédiatement.
    Le suivi du statut sera fait par Cloud Workflows (cf. 4.4)."""
    vertexai.init(project=PROJECT, location=REGION)
    job = BatchPredictionJob.submit(
        source_model=MODEL,
        input_dataset=input_uri,
        output_uri_prefix=f"gs://{STAGING}/output/",
    )
    print(f"Job lancé : {job.resource_name}")
    print(f"État initial : {job.state}")
    return job

# ─── Orchestration locale du script ───────────────────────────────────────

def run():
    rows = get_unclassified()
    if not rows:
        print("Rien à classer.")
        return
    print(f"{len(rows)} posts à classer.")
    jsonl_uri = upload_jsonl(build_jsonl(rows))
    launch_batch_job(jsonl_uri)

if __name__ == "__main__":
    run()