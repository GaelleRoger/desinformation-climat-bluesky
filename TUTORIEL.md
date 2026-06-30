# Tutoriel — Construire le pipeline Climat × Météo sur GCP

> Guide pas-à-pas pour monter en compétences en architecture de données. Chaque section explique le **pourquoi** avant le **comment**. Lis les encadrés « 🎓 Concept » : ils contiennent ce qui compte vraiment pour passer d'analyste à architecte.
>
> **Version 3.0** — ajout de la couche d'**industrialisation** qui fait la différence pour un poste de Lead/Architecte : orchestration via **Cloud Workflows** (avec polling du Batch Prediction), **observabilité** (log-based metrics + alertes Monitoring vers Discord et email), équivalents **Terraform** des ressources clés, et **CI/CD** via Cloud Build.
>
> **Versions précédentes :** v2.0 — ingestion micro-batch via `searchPosts`, secrets via Secret Manager, data lake GCS, classification de désinformation via Gemini Batch Prediction.

---

## Comment utiliser ce tutoriel

Ce guide suit les 5 étapes du plan. Tu peux le parcourir linéairement. Le code est donné à titre de référence pédagogique : prends le temps de le comprendre plutôt que de le copier-coller. À chaque étape, des questions « 🤔 Pour aller plus loin » t'aident à anticiper ce qu'un recruteur pourrait te demander.

**Convention :** remplace partout `$PROJECT_ID` par l'identifiant de ton projet GCP, `$REGION` par ta région (ex. `europe-west1` ou `europe-west9` Paris), et `$PROJECT_ID` dans les chemins GCS par le nom réel de tes buckets.

---

## Étape 0 — Préparer le terrain

### 0.1 Concepts de base à maîtriser avant de commencer

> **🎓 Concept — ELT vs ETL**
> En **ETL** (Extract-Transform-Load), tu transformes les données *avant* de les charger. En **ELT** (Extract-Load-Transform), tu charges d'abord le brut, puis tu transformes *dans* l'entrepôt. Le cloud a popularisé l'ELT car le stockage est bon marché et les entrepôts (comme BigQuery) sont puissants. Notre pipeline est un ELT : on dépose le brut dans GCS, BigQuery le lit, puis Dataform transforme.

> **🎓 Concept — Pourquoi des couches bronze/silver/gold ?**
> Si tu transformes directement le brut en tables finales, le jour où ta logique change tu as tout perdu : les données brutes ne sont plus là. En gardant une couche **bronze** immuable (ici dans GCS), tu peux toujours tout recalculer. La couche **silver** nettoie (déduplique, type, filtre). La couche **gold** agrège pour l'analyse. Cette séparation, c'est de la **maintenabilité** — un mot-clé d'architecte.

> **🎓 Concept — Lakehouse : le data lake ET l'entrepôt**
> On stocke le brut dans un **data lake** (GCS : du stockage objet, pas cher, qui avale n'importe quel fichier) et on le requête avec un **entrepôt** (BigQuery). BigQuery lit les fichiers GCS via des **tables externes** sans les copier. On combine le faible coût du lake et la puissance SQL de l'entrepôt : c'est le pattern **lakehouse**.

### 0.2 Outils à installer en local

```bash
# Le SDK Google Cloud (gcloud)  → https://cloud.google.com/sdk/docs/install
gcloud --version

# Python 3.11+ pour les jobs d'ingestion et de classification
python3 --version

# Git pour versionner (indispensable pour un portfolio)
git --version
```

### 0.3 Créer et configurer le projet GCP

```bash
gcloud auth login

# Créer le projet (l'ID doit être unique mondialement)
gcloud projects create climat-meteo-pipeline --name="Climat Meteo Pipeline"
gcloud config set project climat-meteo-pipeline

# Lier le compte de facturation
gcloud billing accounts list
gcloud billing projects link climat-meteo-pipeline --billing-account=XXXXXX-XXXXXX-XXXXXX
```

### 0.4 Le réflexe coût AVANT tout : l'alerte budget

> **🎓 Concept — Penser coût dès le jour zéro**
> Un architecte ne découvre pas la facture à la fin du mois. La toute première chose à faire sur un projet cloud, c'est de poser des garde-fous. On crée une alerte budget *avant* d'allumer quoi que ce soit.

```bash
gcloud services enable billingbudgets.googleapis.com
# Puis, plus simple via la console : Facturation > Budgets et alertes > Créer un budget
# Seuils recommandés : 50 €, 100 €, 200 € → email
```

### 0.5 Activer les APIs nécessaires

```bash
gcloud services enable \
  run.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  bigqueryconnection.googleapis.com \
  dataform.googleapis.com \
  cloudscheduler.googleapis.com \
  aiplatform.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

> Note les changements vs v1 : on ajoute `storage`, `secretmanager`, `aiplatform` (Vertex) et `bigqueryconnection` (pour les tables externes). On retire `pubsub` et `language` (plus de Pub/Sub ni de Cloud Natural Language).

> **🤔 Pour aller plus loin :** pourquoi faut-il « activer » des APIs sur GCP ? Par défaut tout est désactivé (moindre privilège, maîtrise des coûts). Activer une API ne coûte rien mais trace explicitement ce que ton projet utilise.

---

## Étape 1 — Cadrage & architecture (déjà fait !)

Cette étape correspond au document `ARCHITECTURE.md`. Son but : **réfléchir avant de coder**.

> **🎓 Concept — Le document d'architecture est un livrable, pas une formalité**
> En entreprise, on ne lance pas un projet data sans valider l'architecture. Ce document sert à : (1) aligner les parties prenantes, (2) anticiper coûts et risques, (3) justifier les choix. Pour ton portfolio, c'est LA pièce qui prouve que tu penses comme un architecte.

**Ce que tu dois pouvoir défendre à l'oral après cette étape :**

1. Pourquoi une architecture lakehouse en médaillon ?
2. Pourquoi du micro-batch plutôt que du streaming pur (firehose) ?
3. Pourquoi Secret Manager plutôt qu'un fichier `.env` ?
4. Pourquoi une classification plutôt qu'une analyse de sentiment ?
5. Pourquoi Batch Prediction plutôt que des appels temps réel à Gemini ?
6. Comment tu maîtrises les coûts ?
7. Quelles sont les limites méthodologiques (et éthiques) de classer de la « désinformation » ?
8. Comment ton pipeline est-il orchestré, et comment gères-tu un job asynchrone à durée variable ?
9. Comment saurais-tu que le pipeline a planté à 3h du matin ?
10. Comment garantis-tu la reproductibilité de ton infra entre environnements ?

Si tu sais répondre à ces questions, l'étape 1 est réussie. Le reste du tutoriel détaille l'implémentation.

### 1.1 Les principes d'industrialisation (le fil rouge du projet)

> **🎓 Concept — Ce qui sépare un Data Engineer d'un Architecte**
> Un bon pipeline qui marche, c'est du Data Engineering. Un pipeline **reproductible, orchestré, observable et déployé automatiquement**, c'est de l'architecture. La différence ne se voit pas dans ce que le pipeline *fait*, mais dans la façon dont il est *construit et exploité*. Ces quatre principes irriguent tout le reste du tutoriel — ils ne sont pas des chapitres tardifs, ce sont des réflexes à avoir dès la première ligne.

Les quatre piliers que ce projet démontre :

**1. Infrastructure-as-Code (IaC) — la reproductibilité.** On ne déploie pas une infra en cliquant dans une console : c'est impossible à reproduire, à auditer, à dupliquer. On la *décrit* en code (Terraform). Dans ce tutoriel, on garde les commandes `gcloud` comme support pédagogique (on voit ce qui se passe), mais on donne l'**équivalent Terraform** des ressources clés — pour que tu saches dire en entretien : « mon infra est déclarative, je la duplique en dev/prod en une commande ».

**2. Orchestration — la robustesse.** On ne compte pas sur la chance pour que la tâche A finisse avant la tâche B. Un **orchestrateur** (Cloud Workflows) enchaîne explicitement les étapes, attend la fin des tâches asynchrones, et gère les erreurs. C'est le correctif au syndrome « bouts de ficelle ».

**3. Observabilité — l'exploitabilité.** Un pipeline qui plante en silence est inutile. On instrumente : **log-based metrics** + **alertes** (Discord + email) pour être prévenu si un job échoue ou si une assertion qualité saute. On pense le *run*, pas seulement le *build*.

**4. CI/CD — l'automatisation du déploiement.** Le code ne se déploie pas à la main. À chaque `git push`, un pipeline (**Cloud Build**) reconstruit et redéploie. Reproductible, traçable, sans intervention manuelle.

> **🎓 Concept — « Build » vs « Run »**
> Construire un pipeline (le *build*) est la moitié du travail. Le faire tourner de façon fiable dans le temps (le *run*) est l'autre moitié — souvent la plus négligée par les juniors. IaC et CI/CD servent le build reproductible ; orchestration et observabilité servent le run fiable. Un architecte pense aux deux dès le départ.

> **Note de transparence pédagogique :** dans ce projet portfolio, le *core* (ingestion → stockage → transformation → classification → dashboard) est conçu pour tourner réellement. Les briques d'industrialisation (Terraform, Cloud Build) sont **documentées de façon crédible et déployable** sans nécessairement être toutes mises en production — un choix assumé pour éviter le *scope creep* tout en démontrant la maîtrise. À énoncer honnêtement en entretien : c'est une décision d'architecte (prioriser l'effort), pas un oubli.

---

## Étape 2 — Secrets, fondations & ingestion micro-batch

C'est le cœur de l'ingestion. On part de TON script `extract_blusky.py` et on le transforme en job cloud propre.

### 2.1 Les secrets dans Secret Manager

> **🎓 Concept — Ne jamais mettre un secret dans le code ou un `.env` versionné**
> Ton script lit `BSKY_HANDLE` / `BSKY_APP_PASSWORD` via `load_dotenv()`. En local c'est ok, mais en production cloud un `.env` finit par fuiter (commité par erreur, visible dans une image conteneur, loggé). La bonne pratique : stocker les secrets dans **Secret Manager**, chiffrés et versionnés, et les lire à l'exécution. Le code ne contient jamais le secret, seulement son *nom*.

```bash
# Créer les secrets (une valeur par secret)
printf "ton.handle.bsky.social" | gcloud secrets create bsky-handle --data-file=-
printf "xxxx-xxxx-xxxx-xxxx"     | gcloud secrets create bsky-app-password --data-file=-
printf "ta_cle_openweathermap"   | gcloud secrets create owm-api-key --data-file=-

# Vérifier
gcloud secrets list
```

> **🤔 Pour aller plus loin :** un *app password* Bluesky (réglages → confidentialité → mots de passe d'application) n'est pas ton vrai mot de passe : c'est un jeton révocable dédié aux applications. Si une app est compromise, tu révoques ce mot de passe sans changer ton compte. C'est déjà un réflexe de sécurité — à mentionner en entretien.

### 2.2 Comptes de service et IAM au moindre privilège

> **🎓 Concept — Un compte de service par usage, avec le minimum de droits**
> Chaque job s'exécute avec une **identité** (compte de service). On lui donne uniquement les permissions dont il a besoin : lire tel secret, écrire dans tel bucket. Si ce compte fuite, les dégâts sont circonscrits. C'est le **principe de moindre privilège**, fondamental en sécurité cloud.

```bash
# Compte de service pour l'ingestion
gcloud iam service-accounts create ingestion-sa \
  --display-name="Ingestion posts et meteo"

SA="ingestion-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Droit de lire UNIQUEMENT les secrets nécessaires
for secret in bsky-handle bsky-app-password owm-api-key; do
  gcloud secrets add-iam-policy-binding $secret \
    --member="serviceAccount:${SA}" \
    --role="roles/secretmanager.secretAccessor"
done

# Droit d'écrire dans les buckets bronze (accordé après création des buckets, cf. 2.3)
```

### 2.3 Créer le data lake (buckets GCS)

```bash
# Bronze posts, bronze météo, et staging Vertex
gcloud storage buckets create gs://${PROJECT_ID}-bronze-posts   --location=$REGION
gcloud storage buckets create gs://${PROJECT_ID}-bronze-weather --location=$REGION
gcloud storage buckets create gs://${PROJECT_ID}-vertex-staging --location=$REGION

# Le compte d'ingestion peut écrire dans les deux buckets bronze
for bucket in bronze-posts bronze-weather; do
  gcloud storage buckets add-iam-policy-binding gs://${PROJECT_ID}-${bucket} \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin"
done
```

> **🎓 Concept — Le partitionnement par dossier (Hive-style)**
> On écrit les fichiers sous `dt=YYYY-MM-DD/`. Cette convention (« Hive partitioning ») permet à BigQuery de ne lire que les dossiers utiles quand on filtre par date, et garde le lake lisible par un humain. Un détail qui sent l'expérience.

> **🏗️ Équivalent Terraform (IaC)**
> Voici la version déclarative de ces buckets. Tout le projet devrait à terme être décrit ainsi dans un dossier `/terraform`. On montre ici le pattern ; on le répétera en encadré pour les ressources clés.
> ```hcl
> # terraform/storage.tf
> variable "project_id" {}
> variable "region" { default = "europe-west9" }
>
> locals {
>   buckets = ["bronze-posts", "bronze-weather", "vertex-staging"]
> }
>
> resource "google_storage_bucket" "lake" {
>   for_each                    = toset(local.buckets)
>   name                        = "${var.project_id}-${each.key}"
>   location                    = var.region
>   uniform_bucket_level_access = true
>
>   lifecycle_rule {                      # garde-fou coût : archivage auto
>     condition { age = 90 }
>     action   { type = "SetStorageClass" storage_class = "NEARLINE" }
>   }
> }
> ```
> **Pourquoi c'est supérieur au `gcloud` :** idempotent (rejouable sans effet de bord), versionné dans Git, et `terraform apply` recrée l'infra identique dans un projet de dev ou de prod. La boucle `for_each` évite la répétition. La `lifecycle_rule` intègre le garde-fou coût directement dans la définition de l'infra — le FinOps est déclaratif, pas un geste manuel qu'on oublie.

> **🎓 Concept — Pourquoi garder `gcloud` ET montrer Terraform ?**
> Dans ce tutoriel, `gcloud` reste le fil principal car il est *pédagogique* : chaque commande montre une action atomique, tu comprends ce qui se crée. Terraform en regard montre la *maturité* : tu sais que la vraie production est déclarative. En entretien, pouvoir dire « voici le geste, et voici sa version industrialisée » est un signal fort. Le piège junior serait de ne connaître que l'un des deux.

### 2.4 Adapter ton script en job d'ingestion

On reprend la logique de `extract_blusky.py` en changeant trois choses : (1) thème **climat** au lieu de covoiturage, (2) secrets via **Secret Manager** au lieu de `.env`, (3) sortie = **fichier dans GCS** au lieu d'un fichier local. On conserve ta logique de pagination, dédup et tri, qui est bonne.

`ingestion/posts/main.py` (extrait des parties nouvelles) :
```python
import os, json, datetime as dt
from google.cloud import secretmanager, storage
import requests

PROJECT_ID = os.environ["PROJECT_ID"]
BUCKET = f"{PROJECT_ID}-bronze-posts"

SEARCH_TERMS = [
    "climat", "réchauffement climatique", "dérèglement climatique",
    "GIEC", "gaz à effet de serre", "carbone", "canicule",
    "transition écologique", "climato",
]
SEARCH_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
AUTH_URL   = "https://bsky.social/xrpc/com.atproto.server.createSession"

def get_secret(name: str) -> str:
    """Lit la dernière version d'un secret."""
    client = secretmanager.SecretManagerServiceClient()
    path = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=path).payload.data.decode("utf-8")

def authenticate(session) -> str:
    handle = get_secret("bsky-handle")
    password = get_secret("bsky-app-password")
    resp = session.post(AUTH_URL,
                        json={"identifier": handle, "password": password},
                        timeout=20)
    resp.raise_for_status()
    return resp.json()["accessJwt"]

# ... (search_posts_for_term, _get_with_retry, extract_fields,
#      merge_dedupe_sort : repris quasi tels quels de ton script) ...

def write_to_gcs(posts: list):
    """Écrit un fichier horodaté dans le bucket bronze (partition par date)."""
    now = dt.datetime.now(dt.timezone.utc)
    blob_path = f"dt={now:%Y-%m-%d}/posts_{now:%H%M%S}.json"
    payload = {
        "ingested_at": now.isoformat(),
        "terms": SEARCH_TERMS,
        "count": len(posts),
        "posts": posts,
    }
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False),
        content_type="application/json",
    )
    print(f"Écrit gs://{BUCKET}/{blob_path} ({len(posts)} posts)")

def run():
    session = requests.Session()
    token = authenticate(session)
    session.headers.update({"Authorization": f"Bearer {token}"})
    all_posts = []
    for term in SEARCH_TERMS:
        all_posts.extend(search_posts_for_term(term, "fr", 100, session))
    result = merge_dedupe_sort(all_posts, limit=10_000)  # pas de limite serrée
    if result:
        write_to_gcs(result)

if __name__ == "__main__":
    run()
```

> **🎓 Concept — Idempotence et déduplication à deux niveaux**
> Chaque exécution récupère les posts *récents*, donc deux exécutions voisines se chevauchent et ramènent des doublons. On déduplique une première fois dans le job (par URI, comme dans ton script), puis une seconde fois en couche silver (au cas où le même post apparaît dans deux fichiers d'exécutions différentes). La dédup finale en SQL est la garantie ultime — on y vient à l'étape 3.

### 2.5 Conteneuriser et déployer comme Cloud Run Job

> **🎓 Concept — Job vs Service**
> Un Cloud Run **service** répond à des requêtes en continu (et coûte tant qu'il tourne). Un Cloud Run **job** s'exécute une fois, fait son travail, et s'arrête. Pour de l'ingestion planifiée, le **job** est parfait : zéro coût au repos. C'est ce qui remplace l'« interrupteur » manuel de la v1.

> **🎓 Concept — Deux façons de packager : buildpacks vs Dockerfile**
> Cloud Run Jobs accepte deux modes de déploiement :
> - **Buildpacks** (mode simple) : tu fournis juste `requirements.txt` + `main.py`, et `gcloud run jobs deploy --source` demande à Cloud Build de construire le conteneur **pour toi**, automatiquement. Zéro Dockerfile à écrire. Idéal pour démarrer ou pour un job trivial.
> - **Dockerfile** (mode contrôlé) : tu écris explicitement le conteneur. Plus de travail initial, mais tu maîtrises tout (version de Python, paquets système, optimisation de l'image, reproductibilité totale).
>
> **Choix pour ce tutoriel : Dockerfile**, pour deux raisons. (1) *Pédagogique* — un architecte doit savoir lire et écrire un Dockerfile ; le voir explicitement aide à comprendre ce qui tourne. (2) *Prévoyance* — le jour où tu auras besoin d'un paquet système (lib C, outil CLI) ou de figer un point de version précis, le Dockerfile est obligatoire ; autant l'avoir dès le départ. Le piège junior est de ne connaître qu'une des deux options : en entretien, sache dire « j'ai choisi le Dockerfile pour la maîtrise, mais les buildpacks suffisaient ».

`ingestion/posts/Dockerfile` :
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
```
`requirements.txt` : `requests`, `google-cloud-secret-manager`, `google-cloud-storage`.

```bash
# Déployer le job (Cloud Build détecte le Dockerfile et l'utilise)
gcloud run jobs deploy ingest-posts \
  --source ./ingestion/posts \
  --region $REGION \
  --service-account $SA \
  --set-env-vars PROJECT_ID=$PROJECT_ID \
  --max-retries 2 \
  --task-timeout 300

# Test manuel
gcloud run jobs execute ingest-posts --region $REGION
```

> **🤔 Pour aller plus loin :** la commande `--source` est ambivalente — elle marche dans les deux modes. Si Cloud Build trouve un `Dockerfile` à la racine du dossier, il l'utilise ; sinon il bascule sur les buildpacks. Le même `gcloud` peut donc déployer un job avec ou sans Dockerfile, ce qui est pratique pour migrer plus tard.

### 2.6 Planifier le micro-batch (Cloud Scheduler)

```bash
# Toutes les 15 minutes (décision D1 : ajustable)
gcloud scheduler jobs create http ingest-posts-schedule \
  --location $REGION \
  --schedule "*/15 * * * *" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/ingest-posts:run" \
  --http-method POST \
  --oauth-service-account-email $SA
```

> **🤔 Pour aller plus loin :** pourquoi commencer à 15 min et pas 5 ? Parce qu'on ne connaît pas encore le volume réel de posts climat FR. On démarre prudemment, on observe le volume et les rate limits sur quelques jours, puis on ajuste. Dimensionner d'après des mesures plutôt que des suppositions est un réflexe d'architecte.

### 2.7 Ingestion météo (job quotidien)

Même structure de conteneur qu'en 2.5 (Dockerfile minimal + `requirements.txt` + `main.py`), juste un contenu Python différent. `ingestion/weather/main.py` appelle OpenWeatherMap (clé via Secret Manager) pour Paris et écrit un fichier dans `gs://${PROJECT_ID}-bronze-weather/dt=YYYY-MM-DD/weather_HHMMSS.jsonl`.

> **🎓 Concept — La cohérence opérationnelle est un asset architectural**
> On aurait pu se dire « le job météo est trivial, les buildpacks suffisent, pas la peine de Dockerfile ». C'est tentant — mais c'est un piège. **Avoir tous les jobs structurés pareil** (même layout `Dockerfile + requirements.txt + main.py`, même façon d'être déployés) coûte un peu plus à l'écriture initiale et rapporte énormément à la maintenance et au CI/CD. Quand le pipeline Cloud Build de l'étape 5 doit builder N jobs, une **règle unique** suffit s'ils ont tous la même structure. Mélanger buildpacks et Dockerfile pour des jobs de même nature complique le CI/CD sans bénéfice. L'**uniformité disciplinée** est une qualité d'architecte — un projet ne se juge pas seulement à ce qui tourne, mais à la facilité avec laquelle un nouveau venu peut le lire.

> **📄 Note de format : extension `.jsonl`**
> Même si le fichier météo n'a qu'**une seule ligne** (un appel = une mesure), on l'écrit avec l'extension `.jsonl` et le content-type `application/jsonl`. Raison : la table externe BigQuery `bronze.weather_ext` (cf. 3.1) attend du JSONL pour tous les fichiers qu'elle lit. Tout uniformiser dans la bronze (posts ET météo) au même format simplifie la configuration. Concrètement, c'est juste l'extension qui change — un `json.dumps(envelope)` sans `indent=` produit déjà du JSONL valide pour une ligne unique.

```python
def run():
    api_key = get_secret("owm-api-key")
    # Paris ; on récupère la météo courante (le plan gratuit la couvre)
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": 48.8566, "lon": 2.3522, "appid": api_key,
              "units": "metric", "lang": "fr"}
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    write_weather_to_gcs(resp.json())   # écrit .jsonl, content_type=application/jsonl
```

```bash
# Déploiement : exactement le même pattern qu'en 2.5
gcloud run jobs deploy ingest-weather --source ./ingestion/weather \
  --region $REGION --service-account $SA \
  --set-env-vars PROJECT_ID=$PROJECT_ID

gcloud scheduler jobs create http ingest-weather-schedule \
  --location $REGION --schedule "0 6 * * *" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/ingest-weather:run" \
  --http-method POST --oauth-service-account-email $SA
```

> **🎓 Concept — Le bon rythme pour chaque source**
> Les posts bougent en permanence → micro-batch. La météo du jour est figée → un appel quotidien suffit. Un bon architecte ne force pas un rythme unique : il adapte la cadence à la nature de chaque source. C'est plus simple ET moins cher.

> **⚠️ Rappel OpenWeatherMap :** on appelle la météo *courante* (gratuite). L'*historique* (jours passés) est payant chez OWM. Pour backfiller du passé, voir « évolutions futures » dans le doc d'architecture (Open-Meteo Archive est une alternative gratuite).

---

## Étape 3 — Tables externes & transformation Dataform

Le brut est dans GCS. On le rend interrogeable par BigQuery, puis on modélise.

### 3.1 Exposer le bronze GCS via des tables externes

> **🎓 Concept — Table externe : requêter sans copier**
> Une table externe est une « vue » BigQuery sur des fichiers GCS. BigQuery lit les fichiers à la volée au moment de la requête, sans dupliquer les données. Avantage : pas de coût de stockage BigQuery, le brut reste dans le lake. C'est le pont entre le data lake et l'entrepôt.

#### Pré-requis : tous les fichiers bronze sont en JSONL

> **🎓 Concept — Un seul format de fichier dans toute la bronze**
> BigQuery lit les tables externes selon **un seul format** par table (configuré au moment de la création). On choisit **JSONL** (`NEWLINE_DELIMITED_JSON`, une ligne = un objet JSON) pour deux raisons : (1) c'est le format standard du big data, attendu par BigQuery comme par Vertex Batch Prediction ; (2) en uniformisant **tous** les fichiers bronze à ce format (posts ET météo, même si la météo n'a qu'une ligne par fichier), on simplifie la configuration, la maintenance et le CI/CD. Comme pour le choix du Dockerfile (cf. 2.7), l'uniformité disciplinée prime sur l'optimisation unitaire.

**Ajustement à apporter au job d'ingestion des posts** (cf. 2.4) : la fonction `write_to_gcs` doit écrire **une ligne par post** au lieu d'un objet enveloppe avec un tableau. Voici la version JSONL :

```python
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
```

> **🎓 Concept — Le brut ne signifie pas « réponse de l'API telle quelle »**
> On est en bronze, donc on garde les champs Bluesky intacts (`**post`). Mais on ajoute les *métadonnées d'ingestion* (`ingested_at`, `search_terms`) à chaque ligne — elles sont précieuses en silver pour la traçabilité et la déduplication. Bronze ne veut pas dire « ne touche à rien » ; ça veut dire « ne perd rien et ne transforme pas la sémantique ».

**Côté météo** (cf. 2.7), même principe avec un changement minimal : le fichier s'écrit en `.jsonl` (extension + content_type), même s'il n'a qu'une ligne. Le `json.dumps(envelope)` sans indentation produit déjà du JSONL valide pour une ligne unique.

#### Création de la table externe sur les posts

```bash
# 1. Créer le dataset bronze (s'il n'existe pas)
bq mk --dataset --location=$REGION ${PROJECT_ID}:bronze

# 2. Générer la définition de la table (Hive partitioning par dossier dt=)
bq mkdef --source_format=NEWLINE_DELIMITED_JSON \
  --hive_partitioning_mode=AUTO \
  --hive_partitioning_source_uri_prefix=gs://${PROJECT_ID}-bronze-posts/ \
  "gs://${PROJECT_ID}-bronze-posts/*" > /tmp/posts_def.json

# 3. Créer effectivement la table à partir de cette définition
bq mk --external_table_definition=/tmp/posts_def.json \
  ${PROJECT_ID}:bronze.posts_ext
```

> **🎓 Concept — `mkdef` produit, `mk` crée**
> Deux commandes distinctes pour deux étapes distinctes. `mkdef` génère un fichier JSON décrivant **comment** BigQuery doit lire le bucket (format, partitionnement, schéma). `mk --external_table_definition` crée alors la table dans BigQuery en utilisant cette définition. Cette séparation permet d'inspecter et de versionner la définition avant la création.

#### Création de la table externe sur la météo

```bash
bq mkdef --source_format=NEWLINE_DELIMITED_JSON \
  --hive_partitioning_mode=AUTO \
  --hive_partitioning_source_uri_prefix=gs://${PROJECT_ID}-bronze-weather/ \
  "gs://${PROJECT_ID}-bronze-weather/*" > /tmp/weather_def.json

bq mk --external_table_definition=/tmp/weather_def.json \
  ${PROJECT_ID}:bronze.weather_ext
```

#### Requêter la bronze depuis BigQuery

Une fois les tables créées, tu les requêtes **exactement comme n'importe quelle table BigQuery** — c'est tout l'intérêt des tables externes : la complexité de lecture des fichiers GCS est invisible en SQL.

```sql
-- Aperçu rapide du contenu
SELECT * FROM `bronze.posts_ext` LIMIT 5;

-- Volume de posts ingérés aujourd'hui
SELECT COUNT(*) AS nb_posts
FROM `bronze.posts_ext`
WHERE dt = CURRENT_DATE();

-- Rythme d'ingestion par jour (sanity check du job)
SELECT dt, COUNT(*) AS nb_posts
FROM `bronze.posts_ext`
GROUP BY dt
ORDER BY dt DESC;

-- Météo des 7 derniers jours
SELECT
  dt,
  JSON_VALUE(payload, '$.main.temp')      AS temp_c,
  JSON_VALUE(payload, '$.weather[0].main') AS condition
FROM `bronze.weather_ext`
WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
```

Note la **colonne `dt` synthétisée automatiquement** par BigQuery à partir du nom de dossier (`dt=YYYY-MM-DD/`), grâce au `--hive_partitioning_mode=AUTO`. Tu n'as rien défini comme colonne dans le JSON ; c'est BigQuery qui l'expose à partir de la structure des dossiers.

> **🎓 Concept — Toujours filtrer sur la partition Hive**
> Une table externe sans filtre lit **tous les fichiers du bucket** à chaque requête, et BigQuery te facture l'intégralité des octets scannés. Avec un `WHERE dt = ...`, BigQuery ne lit que les dossiers concernés. C'est ce qui rend les tables externes économiques. Sans filtre, tu paies pour scanner ton historique entier à chaque requête. Réflexe non négociable.

> **🤔 Pour aller plus loin :** que se passe-t-il si tu ajoutes un nouveau champ JSON dans un post à partir de demain ? Réponse : BigQuery le détectera automatiquement à la prochaine requête (schéma inféré dynamiquement). C'est très flexible — mais aussi un piège : un changement de schéma silencieux peut casser tes transformations silver sans alerte. Pour de la prod, on figerait souvent le schéma explicitement via `bq update --schema`. Bon à mentionner en entretien comme limite assumée du choix « schéma inféré ».

### 3.2 Initialiser Dataform

> **🎓 Concept — Dataform = SQL + dépendances + tests**
> Dataform ajoute au SQL trois superpouvoirs : un **graphe de dépendances** (il exécute dans le bon ordre), des **tests qualité** (assertions), et le **versioning Git**. C'est ce qui transforme un tas de scripts en pipeline industrialisé. dbt fait la même chose — savoir le dire en entretien est un plus.

#### Comprendre la hiérarchie Dataform

Avant de cliquer, il faut distinguer trois niveaux qu'on confond souvent :

| Niveau | Nom | Qui le choisit ? |
|---|---|---|
| **Dépôt Dataform** (repository) | `climat-meteo` (ou ce que tu veux) | **Toi**, dans la console GCP |
| **Espace de travail** (workspace) | `default` par défaut, pointe sur la branche Git | Dataform crée `default` automatiquement ; tu peux en créer d'autres pour des branches de test |
| **Dossier `definitions/`** | `definitions` | **Imposé par Dataform** — c'est là que vivent les fichiers `.sqlx`, point |

Les **sous-dossiers** à l'intérieur de `definitions/` (comme `sources/`, `staging/`, `marts/`) sont en revanche **libres** — tu les crées toi-même pour organiser tes modèles par couche médaillon.

#### Arborescence complète du projet

Voici à quoi ressemble l'arbre côté GitHub (`https://github.com/GaelleRoger/desinformation-climat-bluesky/`), avec le sous-dossier `dataform/` que Dataform synchronisera :

```
desinformation-climat-bluesky/         ← dépôt GitHub (tu l'as déjà créé)
├── ingestion/
│   ├── posts/                         ← job Cloud Run ingestion Bluesky
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── main.py
│   └── weather/                       ← job Cloud Run météo
│       ├── Dockerfile
│       ├── requirements.txt
│       └── main.py
├── classification/                    ← prepare / load_results Vertex
├── orchestration/
│   └── pipeline.yaml                  ← Cloud Workflows
├── dataform/                          ← contenu synchronisé par Dataform
│   ├── workflow_settings.yaml         ← config du projet Dataform
│   └── definitions/                   ← NOM IMPOSÉ par Dataform
│       ├── sources/                   ← sous-dossiers d'organisation libre
│       │   ├── posts_ext.sqlx
│       │   └── weather_ext.sqlx
│       ├── staging/                   ← silver
│       │   ├── posts_clean.sqlx
│       │   └── weather_daily.sqlx
│       └── marts/                     ← gold
│           ├── daily_disinfo.sqlx
│           └── climate_x_weather.sqlx
├── terraform/                         ← infra déclarative
├── cloudbuild.yaml                    ← CI/CD
└── README.md
```

> **🎓 Concept — Pourquoi `dataform/` comme sous-dossier du dépôt GitHub ?**
> Dataform sait se brancher sur **un sous-dossier** d'un dépôt Git (paramètre « root directory » à la création). Conséquence pratique : ton dépôt GitHub peut contenir TOUT le projet (jobs Python, Terraform, Cloud Build…), et Dataform ne synchronise que ce qui le concerne (`dataform/`). Sans ce mécanisme, il faudrait un dépôt GitHub dédié juste à Dataform — fragmentation inutile.

#### Créer le dépôt Dataform

Console GCP : **BigQuery > Dataform > Créer un dépôt**.

| Champ | Valeur recommandée |
|---|---|
| ID du dépôt | `climat-meteo` |
| Région | la même que ton projet (ex. `europe-west9`) |
| Compte de service | celui de ton pipeline (`ingestion-sa@…`) ou un compte dédié Dataform |
| Connexion Git | URL : `https://github.com/GaelleRoger/desinformation-climat-bluesky.git` |
| Branche par défaut | `main` |
| **Sous-dossier (root directory)** | `dataform` |
| Jeton d'authentification | secret Secret Manager contenant un *Personal Access Token* GitHub avec scope `repo` |

> **🤔 Pour aller plus loin — l'authentification Git :** Dataform a besoin d'un token GitHub pour cloner et pousser. On stocke ce token dans **Secret Manager** (même réflexe que pour les clés Bluesky/OWM, cf. 2.1) et on donne au compte de service Dataform le rôle `secretmanager.secretAccessor` sur ce secret. Le projet entier respecte ainsi un seul principe : *aucun secret en clair nulle part*.

Une fois le dépôt créé, Dataform crée automatiquement un workspace `default` pointant sur `main`. Tu pourras alors ouvrir ce workspace dans la console et créer (ou voir, s'ils sont déjà dans `dataform/definitions/` sur GitHub) tes fichiers `.sqlx`.

#### Déclarer les sources bronze (déclarations)

Avant d'écrire les modèles silver, il faut indiquer à Dataform que `bronze.posts_ext` et `bronze.weather_ext` **existent** dans BigQuery, pour qu'on puisse y référer plus tard avec `${ref("posts_ext")}`.

> **🎓 Concept — Déclaration vs modèle**
> Dans Dataform, deux choses radicalement différentes vivent dans des fichiers `.sqlx` de même apparence :
> - Un **modèle** (`type: "table"` ou `"view"`) : Dataform exécute le SQL et crée/met à jour la table.
> - Une **déclaration** (`type: "declaration"`) : Dataform **n'exécute rien**. C'est juste une étiquette qui dit « cette table existe déjà, on peut y faire référence ». Aucune ressource n'est créée par cette ligne.
>
> Pour quoi faire ? Pour intégrer dans le graphe de dépendances Dataform des tables produites **en dehors** de Dataform — ici nos tables externes créées par `bq mk` à la section 3.1. Sans déclaration, `${ref("posts_ext")}` lèverait une erreur (« unknown reference ») et Dataform ne saurait pas que `posts_clean` dépend de `posts_ext`. Avec déclaration, le DAG est complet et la lineage est traçable de bout en bout.

`dataform/definitions/sources/posts_ext.sqlx` :
```sql
config {
  type: "declaration",
  database: "VOTRE_PROJECT_ID",
  schema: "bronze",
  name: "posts_ext",
  description: "Table externe BigQuery sur les fichiers JSONL bronze des posts Bluesky (créée par bq mk, cf. 3.1)."
}
```

`dataform/definitions/sources/weather_ext.sqlx` :
```sql
config {
  type: "declaration",
  database: "VOTRE_PROJECT_ID",
  schema: "bronze",
  name: "weather_ext",
  description: "Table externe BigQuery sur les fichiers JSONL bronze météo OpenWeatherMap (créée par bq mk, cf. 3.1)."
}
```

Trois remarques pratiques :

D'abord, `database` correspond au **projet GCP** dans la terminologie Dataform/BigQuery (`VOTRE_PROJECT_ID` ici — Dataform a hérité du vocabulaire SQL où la hiérarchie est *database → schema → table*). En production, on ne code pas en dur cette valeur : on la met dans `workflow_settings.yaml` (`defaultProject`) et Dataform l'injecte. Pour ce tutoriel, on l'écrit en clair pour rester lisible.

Ensuite, le champ `description` est facultatif mais précieux : il documente la déclaration directement dans le code, et apparaît dans la console Dataform et la documentation BigQuery. Mettre une description sur chaque déclaration et chaque modèle est un signal de maturité — la traçabilité (*lineage*) passe par là.

Enfin, **aucun SQL après le bloc `config`** : c'est ce qui distingue visuellement une déclaration d'un modèle. Si tu écris une requête en dessous, Dataform t'avertira (la déclaration ne l'exécutera pas de toute façon).

> **🤔 Pour aller plus loin — quand Dataform sait-il que la déclaration est valide ?** À la compilation. Si tu déclares `posts_ext` mais que la table n'existe pas dans BigQuery, la **compilation** Dataform passe (la déclaration n'est qu'une étiquette), mais l'**exécution** du premier modèle qui fait `${ref("posts_ext")}` échouera avec une erreur BigQuery « table not found ». Cette séparation compilation/exécution est typique de Dataform : la première vérifie la cohérence du graphe, la seconde valide l'existence réelle des tables.

### 3.3 Couche silver — posts nettoyés et dédupliqués

`definitions/staging/posts_clean.sqlx` :
```sql
config {
  type: "table",
  schema: "silver",
  description: "Posts climat FR nettoyés et dédoublonnés",
  assertions: {
    uniqueKey: ["post_id"],
    nonNull: ["post_id", "created_at"]
  }
}

WITH src AS (
  SELECT
    uri,
    SPLIT(uri, "/")[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(uri, "/")) - 1)] AS post_id,
    author_handle,
    text,
    created_at,
    langs,
    like_count, repost_count, reply_count,
    _ingested_file
  FROM ${ref("posts_ext")}
),
deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY _ingested_file) AS rn
  FROM src
  WHERE uri IS NOT NULL AND "fr" IN UNNEST(langs)
)
SELECT
  post_id, author_handle, text,
  TIMESTAMP(created_at) AS created_at,
  DATE(TIMESTAMP(created_at)) AS event_date,
  like_count, repost_count, reply_count
FROM deduped
WHERE rn = 1
```

> **🎓 Concept — Déduplication par ROW_NUMBER()**
> `ROW_NUMBER() OVER (PARTITION BY clé ORDER BY ...)` + `WHERE rn = 1` est LA technique standard pour ne garder qu'un exemplaire par clé. Comme nos micro-batchs se chevauchent, c'est ici qu'on neutralise définitivement les doublons. Mémorise ce pattern : on te le demandera en entretien.

> **🎓 Concept — Les assertions = tests de données par contrat**
> Le bloc `assertions` dit à Dataform : « vérifie que `post_id` est unique et non nul ». Si c'est faux, le pipeline échoue avec une erreur claire AVANT de propager des données corrompues. C'est de la **qualité de données par contrat** — un marqueur fort de maturité.

### 3.4 Couche silver — météo

`definitions/staging/weather_daily.sqlx` extrait les champs utiles du JSON OWM et dérive les flags :
```sql
config { type: "table", schema: "silver" }

SELECT
  DATE(TIMESTAMP_SECONDS(dt)) AS event_date,
  main.temp_max AS temp_max,
  main.temp_min AS temp_min,
  main.temp     AS temp_mean,
  COALESCE(rain.`1h`, 0) AS precipitation_mm,
  weather[SAFE_OFFSET(0)].main AS weather_main,
  main.temp_max >= 30 AS is_heatwave,   -- seuil canicule simplifié
  main.temp_min <= 0  AS is_coldsnap    -- seuil gel
FROM ${ref("weather_ext")}
```

### 3.5 Couche gold — tables analytiques

`definitions/marts/daily_disinfo.sqlx` — agrégation quotidienne en excluant le hors-sujet :
```sql
config { type: "table", schema: "gold" }

SELECT
  p.event_date,
  COUNT(*) AS post_count,
  COUNTIF(d.is_climate_disinfo) AS disinfo_count,
  SAFE_DIVIDE(COUNTIF(d.is_climate_disinfo), COUNT(*)) AS pct_disinfo,
  AVG(d.confidence) AS avg_confidence
FROM ${ref("posts_clean")} p
JOIN ${ref("disinfo_labels")} d USING (post_id)
WHERE d.is_climate_related = TRUE     -- on exclut les faux positifs hors-sujet
GROUP BY p.event_date
```

`definitions/marts/climate_x_weather.sqlx` — **la table-vedette** :
```sql
config {
  type: "table",
  schema: "gold",
  description: "Taux de désinformation climatique × météo Paris"
}

SELECT
  d.event_date,
  d.post_count, d.disinfo_count, d.pct_disinfo,
  w.temp_mean, w.temp_max, w.temp_min,
  w.is_heatwave, w.is_coldsnap
FROM ${ref("daily_disinfo")} d
LEFT JOIN ${ref("weather_daily")} w USING (event_date)
ORDER BY d.event_date
```

> **🤔 Pour aller plus loin :** pourquoi `LEFT JOIN` ? Pour garder tous les jours avec des posts même si la météo manque (robustesse). Et pourquoi `WHERE is_climate_related = TRUE` en gold et pas en silver ? Pour garder en silver toutes les classifications (traçabilité), et ne filtrer qu'au moment de l'analyse. Séparer « ce qu'on stocke » de « ce qu'on analyse » est une distinction d'architecte.

---

## Étape 4 — Classification de désinformation (Gemini Batch Prediction)

On a des posts propres en silver. On veut maintenant, pour chacun, savoir s'il relève de la désinformation climatique.

> **🎓 Concept — Classification zero-shot par LLM**
> On n'entraîne aucun modèle. On donne à Gemini une **consigne** (prompt) définissant ce qu'est la désinformation climatique, et on lui demande de classer chaque post. « Zero-shot » = sans exemples d'entraînement. Avantage : aucune donnée annotée requise. Limite : la qualité dépend entièrement du prompt et le résultat est une estimation faillible.

### 4.1 Pourquoi le Batch Prediction (et pas du temps réel)

> **🎓 Concept — « Ingest fast, enrich later »**
> Appeler Gemini à chaque post entrant serait coûteux, fragile (dépendance à la latence/quotas dans le chemin critique) et inutile (la classification n'a pas besoin d'être instantanée). On **accumule** les posts, et périodiquement on envoie un **lot** de quelques milliers à Vertex Batch Prediction. Gemini les traite en différé. Trois bénéfices : coût réduit (~-50 % de tokens en batch), découplage, robustesse. L'ingestion reste rapide ; l'enrichissement se fait tranquillement en arrière-plan.

### 4.2 Préparer le JSONL et lancer le Batch Prediction (un seul script)

Vertex Batch Prediction attend un fichier **JSONL** dans GCS : une ligne = une requête au modèle. On a deux étapes logiques — (a) **construire le fichier** à partir des posts non encore classés, (b) **lancer le job** Vertex qui va le traiter — qu'on regroupe dans un **seul script Python** parce qu'elles partagent les mêmes constantes (projet, bucket, chemin du fichier) et qu'elles sont indissociables : aucun intérêt à préparer sans lancer.

> **🎓 Concept — REST, gcloud ou SDK : choisir le bon mode d'appel**
> Vertex AI s'invoque par trois canaux : l'**API REST** (via `curl`, bas-niveau et toujours disponible), la **CLI `gcloud`** (qui ne couvre pas tous les services Vertex — la création de jobs Batch Prediction Gemini par exemple n'y est pas exposée), et le **SDK Python `google-cloud-aiplatform`** (idiomatique, intégré au code applicatif). Pour ce pipeline, on choisit le **SDK Python** : il s'intègre nativement au script qui construit le JSONL, l'objet `BatchPredictionJob` retourné expose un état exploitable par le polling de Cloud Workflows (4.4), et le code est portable d'un environnement à l'autre sans dépendance à `gcloud`. *Savoir arbitrer entre REST, CLI et SDK selon le contexte est typiquement une compétence d'architecte.*

#### Le script complet : `classification/prepare_and_launch.py`

```python
import os, json
from google.cloud import bigquery, storage, aiplatform

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

def get_unclassified(limit=5000):
    """Sélectionne les posts présents en silver mais absents de disinfo_labels.
    Garantit l'idempotence : on ne reclasse jamais un post déjà traité."""
    bq = bigquery.Client()
    q = f"""
        SELECT p.post_id, p.text
        FROM `{PROJECT}.silver.posts_clean` p
        LEFT JOIN `{PROJECT}.silver.disinfo_labels` d USING (post_id)
        WHERE d.post_id IS NULL
        LIMIT {limit}
    """
    return list(bq.query(q).result())

# ─── Étape B : construire le JSONL au format Gemini ───────────────────────

def build_jsonl(rows) -> str:
    """Une ligne par post, au format attendu par Vertex Batch Prediction Gemini."""
    lines = []
    for r in rows:
        request = {
            "request": {
                "contents": [{
                    "role": "user",
                    "parts": [{"text": PROMPT_TEMPLATE.format(text=r.text)}]
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
    aiplatform.init(project=PROJECT, location=REGION)
    job = aiplatform.BatchPredictionJob.submit(
        source_model=MODEL,
        job_display_name="disinfo-classification",
        gcs_source=input_uri,
        gcs_destination_prefix=f"gs://{STAGING}/output/",
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
```

À ajouter à `classification/requirements.txt` :
```
google-cloud-bigquery
google-cloud-storage
google-cloud-aiplatform>=1.71.0
```

> **🎓 Concept — Idempotence par « ce qui n'est pas encore classé »**
> Le `LEFT JOIN ... WHERE d.post_id IS NULL` ne sélectionne que les posts absents de la table de labels. On peut relancer autant qu'on veut sans reclasser ni repayer. C'est l'**idempotence**, principe central des pipelines robustes — et ici, une protection directe du budget Vertex.

> **🎓 Concept — `temperature: 0` pour une tâche de classification**
> On met la température à 0 pour rendre la sortie la plus déterministe possible : on veut une classification stable, pas de la créativité. Et `responseMimeType: application/json` force un JSON valide, plus facile à parser. Ces réglages montrent qu'on sait *piloter* un LLM, pas juste l'appeler.

> **🎓 Concept — `submit()` est non bloquant**
> `BatchPredictionJob.submit()` soumet le job à Vertex et rend la main immédiatement avec un objet `job` qui contient `resource_name` et `state`. Le job tourne ensuite en arrière-plan, et sa durée est variable (de quelques minutes à plusieurs heures selon le volume). Pour attendre la fin, le SDK propose `job.wait()` (bloquant) ou un polling sur `job.state` — c'est précisément ce que fera Cloud Workflows en 4.4, plutôt que de bloquer un script Python pendant des heures.

#### Test en local

Avant de conteneuriser, un test local est la façon la plus rapide de vérifier que tout fonctionne :

```bash
# Authentification application-default pour que le SDK utilise ton compte
gcloud auth application-default login

# Variables d'environnement
export PROJECT_ID="climat-desinformation-bluesky"
export REGION="europe-west1"

# Lancement
cd classification && python prepare_and_launch.py
```

Tu devrais voir successivement : le nombre de posts à classer, l'URI du JSONL uploadé, le `resource_name` du job soumis et son état initial (`JOB_STATE_PENDING` ou `JOB_STATE_RUNNING`).

#### Suivre le job une fois lancé

Le script rend la main immédiatement, mais le job continue côté Vertex. Pour suivre son avancement :

```bash
# Lister les jobs Batch Prediction récents
gcloud ai operations list --region=$REGION

# Ou via l'API directement
curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://${REGION}-aiplatform.googleapis.com/v1/${JOB_RESOURCE_NAME}"
```

Tu peux aussi suivre dans la console : **Vertex AI → Batch predictions**. Quand le job passe à `JOB_STATE_SUCCEEDED`, les fichiers de sortie apparaissent dans `gs://<projet>-vertex-staging/output/`. Tu peux alors passer à la 4.3 (chargement des résultats).

### 4.3 Récupérer les résultats et charger en silver

Vertex a écrit les prédictions Gemini dans des fichiers JSONL sous `gs://<projet>-vertex-staging/output/...`. Il faut maintenant lire ces fichiers et insérer les classifications dans `silver.disinfo_labels`. C'est le rôle du script `load_results.py`.

#### Pré-requis : créer la table `silver.disinfo_labels`

Contrairement à `posts_clean` ou `weather_daily`, **cette table n'est PAS créée par Dataform** — elle est peuplée par du code Python (`load_results.py`) qui fait des `INSERT`. Elle doit donc exister *avant* la première exécution du script, sinon `insert_rows_json` plantera avec « Not found: Table ... ».

```bash
PROJECT_ID="climat-desinformation-bluesky"

bq mk --table \
  --description="Classifications Gemini : désinformation climatique OUI/NON par post" \
  ${PROJECT_ID}:silver.disinfo_labels \
  post_id:STRING,is_climate_disinfo:BOOLEAN,is_climate_related:BOOLEAN,confidence:FLOAT,model_version:STRING,classified_at:TIMESTAMP
```

> **🎓 Concept — `sources/` Dataform regroupe par origine, pas par couche médaillon**
> Tu as deux notions distinctes qu'il ne faut pas confondre : la **couche médaillon** (bronze/silver/gold) classe les tables par leur niveau de transformation ; le dossier `sources/` de Dataform classe les tables par **qui les produit** (Dataform ou autre chose). Une table peut être en silver côté médaillon ET dans `dataform/definitions/sources/` côté Dataform — c'est précisément le cas de `disinfo_labels` (créée par `load_results.py`, donc hors Dataform). Pas de contradiction : ce sont deux axes orthogonaux.

Ajoute donc la déclaration `dataform/definitions/sources/disinfo_labels.sqlx` pour que les modèles gold puissent y référer :
```sql
config {
  type: "declaration",
  database: "climat-desinformation-bluesky",
  schema: "silver",
  name: "disinfo_labels",
  description: "Classifications désinfo produites par load_results.py (Gemini Batch Prediction). Créée hors Dataform, déclarée ici pour le graphe de dépendances."
}
```

#### Le script complet `classification/load_results.py`

```python
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
```

`requirements.txt` pour ce script :
```
google-cloud-bigquery
google-cloud-storage
```

> **🎓 Concept — Marquer les fichiers traités pour l'idempotence**
> Vertex écrit ses sorties sans nettoyer après lui. Si tu relances `load_results.py` deux fois, tu réinsérerais les mêmes lignes — pollution de la table. Le pattern de **renommage avec un suffixe `.loaded`** garantit qu'on ne retraite jamais ce qui est déjà chargé : à l'exécution suivante, ces fichiers sont filtrés. C'est simple, auditable (les `.loaded` restent visibles dans GCS), et ça ne nécessite aucune base de données annexe. Alternative possible : supprimer le fichier après traitement, ou tenir un registre BigQuery — le renommage est le compromis le plus pragmatique.

> **🎓 Concept — Tracer la version du modèle**
> On stocke `model_version` avec chaque classification. Le jour où Gemini change de version et que les résultats bougent, tu sauras quelles données ont été produites par quel modèle. La **traçabilité** (lineage) est une obsession saine d'architecte.

> **🎓 Concept — Échouer bruyant ou silencieux : selon la cause**
> Deux philosophies cohabitent dans ce script. **Bruyant sur ce qui doit alerter** : si `insert_rows_json` retourne des erreurs (table inexistante, schéma incompatible), on `raise` — Cloud Workflows verra l'erreur, l'alerte Monitoring se déclenche. **Silencieux sur ce qui se rattrape** : si une ligne Gemini est mal formée (sur des milliers ça arrive), on logue un warning et on saute — le post non traité reste avec `disinfo_labels.post_id IS NULL`, donc le prochain run de `prepare_and_launch.py` le reprendra naturellement. L'idempotence du pipeline absorbe les pannes locales. *Choisir où échouer fort et où échouer doux est une décision d'architecte.*

> **🎓 Concept — Forcer les types au parsing**
> Note les `bool(verdict["is_climate_disinfo"])` et `float(verdict["confidence"])`. Gemini *devrait* renvoyer du booléen et du nombre, mais peut parfois produire des chaînes `"true"` ou `"0.85"` selon les variations de prompt. Forcer les types évite des erreurs silencieuses d'insertion BigQuery, qui exige un schéma strict. Détail de robustesse à connaître.

#### Vérifier le chargement

```bash
# Vérifier qu'il y a bien des fichiers de sortie produits par Vertex
gcloud storage ls -r gs://${PROJECT_ID}-vertex-staging/output/

# Lancer le chargement (en local pour tester)
export PROJECT_ID="climat-desinformation-bluesky"
cd classification && python load_results.py

# Vérifier le résultat en BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS n, AVG(confidence) AS conf
   FROM \`${PROJECT_ID}.silver.disinfo_labels\`"

# Vérifier que les fichiers ont bien été marqués
gcloud storage ls -r gs://${PROJECT_ID}-vertex-staging/output/ | grep "\.loaded$"
```

### 4.4 Orchestrer proprement avec Cloud Workflows

> **🎓 Concept — Pourquoi un orchestrateur est non négociable**
> Jusqu'ici, chaque tâche est déclenchée par Cloud Scheduler à heure fixe. Le problème : ces tâches ont des **dépendances** (la classification a besoin des posts ingérés et transformés ; le gold a besoin des labels). Espérer que « l'ingestion de 6h00 soit finie avant la classification de 6h15 » est un pari — et un job de Batch Prediction dure un temps **variable** (5 min ou 2h selon le volume). Compter sur des horaires fixes pour gérer des dépendances, c'est un pipeline qui casse en production. La solution : un **orchestrateur** qui enchaîne explicitement, attend la fin réelle de chaque étape, et gère les erreurs.

> **🎓 Concept — Pourquoi Cloud Workflows et pas Cloud Composer (Airflow) ?**
> Cloud Composer = Airflow managé : puissant, standard, mais il fait tourner un cluster GKE en permanence → **plusieurs centaines d'€/mois même au repos**. Rédhibitoire avec 250 € de crédit. **Cloud Workflows** est serverless : tu ne paies qu'aux exécutions (les premières sont gratuites chaque mois), zéro coût au repos. Pour un pipeline de quelques étapes déclenché périodiquement, c'est le choix FinOps évident. *En entretien : savoir arbitrer Composer vs Workflows selon le volume et le budget est une vraie réflexion d'architecte.*

La séquence à orchestrer :

```
ingestion posts (Cloud Run Job)
   └─▶ Dataform : compilation + exécution silver
         └─▶ prépare le JSONL (Cloud Run Job)
               └─▶ lance Vertex Batch Prediction
                     └─▶ ⏳ POLLING : attend la fin du job (durée variable)
                           └─▶ charge les résultats (Cloud Run Job)
                                 └─▶ Dataform : exécution gold
```

> **🎓 Concept — Le polling : gérer l'asynchrone**
> Le cœur de la difficulté est là. Lancer un Batch Prediction retourne immédiatement un *job en cours*, pas un résultat. On ne peut pas enchaîner tout de suite. Le pattern **polling** consiste à : lancer le job, puis interroger son statut en boucle (« est-il fini ? ») avec une pause entre chaque vérification, jusqu'à `JOB_STATE_SUCCEEDED`. C'est exactement ce qu'un Scheduler ne sait pas faire et qu'un orchestrateur fait nativement. Savoir orchestrer une tâche asynchrone à durée inconnue est une compétence d'architecte distinctive.

Définition du Workflow (`orchestration/pipeline.yaml`, simplifié et commenté) :
```yaml
main:
  steps:
    - init:
        assign:
          - project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          - region: "europe-west9"

    # 1. Ingestion des posts (exécute le Cloud Run Job et attend sa fin)
    - ingest_posts:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: ${"namespaces/" + project + "/jobs/ingest-posts"}
          location: ${region}
        result: ingest_result

    # 2. Dataform : exécute le workflow silver
    - run_dataform_silver:
        call: http.post
        args:
          url: ${"https://dataform.googleapis.com/v1beta1/projects/" + project + "/locations/" + region + "/repositories/climat-meteo/workflowInvocations"}
          auth: { type: OAuth2 }
          body:
            compilationResult: ${compilation_id}   # tags: ["silver"]
        result: df_silver

    # 3. Prépare le fichier JSONL pour Vertex
    # ⚠ En orchestration Workflows, on n'exécute QUE la préparation côté Cloud Run.
    # Le lancement du batch se fait à l'étape suivante (4) par le Workflow lui-même,
    # qui récupère ainsi le resource_name nécessaire au polling (étape 5).
    # Cela correspond à utiliser une variante de prepare_and_launch.py qui ne fait
    # que la préparation (--no-launch, ou un module prepare_only.py).
    - prepare_jsonl:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: ${"namespaces/" + project + "/jobs/prepare-classification"}
          location: ${region}

    # 4. Lance le Batch Prediction (retourne un job EN COURS)
    - launch_batch:
        call: googleapis.aiplatform.v1.projects.locations.batchPredictionJobs.create
        args:
          parent: ${"projects/" + project + "/locations/" + region}
          body:
            displayName: "disinfo-classification"
            model: "publishers/google/models/gemini-2.5-flash"
            inputConfig: { ... }    # GCS JSONL, cf. étape 4.3
            outputConfig: { ... }
        result: batch_job

    # 5. ⏳ POLLING : boucle jusqu'à complétion du job
    - wait_for_batch:
        steps:
          - check_status:
              call: googleapis.aiplatform.v1.projects.locations.batchPredictionJobs.get
              args:
                name: ${batch_job.name}
              result: job_status
          - evaluate:
              switch:
                - condition: ${job_status.state == "JOB_STATE_SUCCEEDED"}
                  next: load_results
                - condition: ${job_status.state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]}
                  raise: ${"Batch job échoué : " + job_status.state}
          - pause:
              call: sys.sleep
              args: { seconds: 60 }     # attendre 1 min avant de re-vérifier
              next: check_status        # ↩ boucle

    # 6. Charge les résultats en silver
    - load_results:
        call: googleapis.run.v1.namespaces.jobs.run
        args:
          name: ${"namespaces/" + project + "/jobs/load-classification"}
          location: ${region}

    # 7. Dataform : exécute le workflow gold
    - run_dataform_gold:
        call: http.post
        args:
          url: ${"https://dataform.googleapis.com/.../workflowInvocations"}
          auth: { type: OAuth2 }
          body:
            compilationResult: ${compilation_id}   # tags: ["gold"]
```

> **🎓 Concept — `switch` + `sleep` + `next` = la boucle d'attente**
> Regarde l'étape `wait_for_batch` : elle vérifie le statut, et selon le résultat soit elle avance (`SUCCEEDED`), soit elle lève une erreur (`FAILED`), soit elle dort 60s et reboucle. Cette structure simple est exactement ce qui rend l'orchestration robuste : le pipeline *attend réellement* la fin du job, quelle que soit sa durée, au lieu de parier sur un horaire.

Déploiement et déclenchement :
```bash
# Déployer le workflow
gcloud workflows deploy climat-pipeline \
  --source orchestration/pipeline.yaml \
  --location $REGION \
  --service-account $SA

# Cloud Scheduler ne fait plus QU'UNE chose : déclencher le workflow
gcloud scheduler jobs create http trigger-pipeline \
  --location $REGION \
  --schedule "0 6 * * *" \
  --uri "https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/climat-pipeline/executions" \
  --http-method POST --oauth-service-account-email $SA
```

> **🎓 Concept — Le rôle réduit du Scheduler**
> Avant : N Schedulers lançaient N tâches en espérant le bon ordre. Maintenant : **1 seul** Scheduler déclenche le Workflow, qui gère lui-même tout l'enchaînement et les attentes. Le Scheduler ne sait plus rien des dépendances — c'est le Workflow qui porte la logique. Cette séparation (déclenchement vs orchestration) est propre et maintenable.

> **🏗️ Équivalent Terraform (IaC)**
> ```hcl
> # terraform/orchestration.tf
> resource "google_workflows_workflow" "pipeline" {
>   name            = "climat-pipeline"
>   region          = var.region
>   service_account = google_service_account.ingestion_sa.id
>   source_contents = file("${path.module}/../orchestration/pipeline.yaml")
> }
>
> resource "google_cloud_scheduler_job" "trigger" {
>   name      = "trigger-pipeline"
>   schedule  = "0 6 * * *"
>   region    = var.region
>   http_target {
>     uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.pipeline.id}/executions"
>     http_method = "POST"
>     oauth_token { service_account_email = google_service_account.ingestion_sa.email }
>   }
> }
> ```
> Note l'élégance : le Workflow et son déclencheur sont liés par référence (`google_workflows_workflow.pipeline.id`). Terraform comprend la dépendance et crée les ressources dans le bon ordre.

> **🤔 Pour aller plus loin :** que se passe-t-il si l'ingestion échoue à l'étape 1 ? Le Workflow s'arrête et les étapes suivantes ne s'exécutent pas — pas de classification sur des données absentes. Et grâce à l'idempotence (on ne classe que les posts non traités), relancer le Workflow le lendemain rattrape proprement. Orchestration + idempotence = pipeline auto-réparant.

---

## Étape 5 — Observabilité & CI/CD (industrialisation)

Le pipeline tourne et il est orchestré. Reste à le rendre **exploitable** (savoir quand il plante) et **déployable automatiquement** (ne plus toucher à la console). C'est ce qui fait passer le projet de « ça marche chez moi » à « plateforme industrialisée ».

### 5.1 Observabilité : ne jamais planter en silence

> **🎓 Concept — « Run » vs « Build »**
> Construire le pipeline était le *build*. Le faire tourner de façon fiable est le *run*. Un pipeline qui échoue à 3h du matin sans prévenir personne est, en pratique, cassé — même si le code est parfait. L'**observabilité** répond à une question simple : « comment je sais que ça va mal ? ». C'est souvent ce qui manque aux projets de portfolio, et donc ce qui te distingue si tu l'as.

On instrumente deux types d'échec : les **erreurs des jobs/Workflows**, et les **échecs d'assertions Dataform** (qualité de données).

**a) Log-based metric sur les erreurs**

> **🎓 Concept — Une métrique à partir des logs**
> Tous les services GCP écrivent des logs dans Cloud Logging. Une **log-based metric** compte les logs qui matchent un filtre (ex. les erreurs). On transforme un flux de texte en une métrique chiffrée sur laquelle on peut *alerter*. C'est le pont entre « il y a des logs quelque part » et « je suis prévenu ».

```bash
# Métrique comptant les exécutions de Workflow en échec
gcloud logging metrics create pipeline_failures \
  --description="Échecs du pipeline climat" \
  --log-filter='resource.type="workflows.googleapis.com/Workflow"
    AND severity=ERROR'

# Métrique comptant les échecs d'assertions Dataform (qualité de données)
gcloud logging metrics create dataform_assertion_failures \
  --description="Assertions Dataform en échec" \
  --log-filter='resource.type="dataform.googleapis.com/Repository"
    AND textPayload=~"assertion.*failed"'
```

**b) Canal de notification : Discord + email**

> **🎓 Concept — Alerter là où tu regardes**
> Une alerte qui arrive dans un canal que personne ne consulte ne sert à rien. On configure deux canaux : un **webhook Discord** (notification instantanée, idéale pour un projet perso/portfolio) et un **email** (trace, fallback). Doubler le canal est une bonne pratique : si l'un tombe, l'autre passe.

Le webhook Discord est exposé à Monitoring via un canal de type webhook. Côté Discord : *Paramètres du salon → Intégrations → Webhooks → Nouveau webhook*, copie l'URL.

```bash
# Canal email
gcloud beta monitoring channels create \
  --display-name="Email alertes" \
  --type=email \
  --channel-labels=email_address=ton.email@example.com

# Canal webhook Discord
gcloud beta monitoring channels create \
  --display-name="Discord alertes" \
  --type=webhook_tokenauth \
  --channel-labels=url=https://discord.com/api/webhooks/XXXX/YYYY
```

> **🤔 Pour aller plus loin :** Discord attend un payload JSON avec une clé `content`. Cloud Monitoring envoie son propre format. En pratique on intercale souvent une petite Cloud Function qui reformate le message Monitoring vers le format Discord (`{"content": "🚨 Pipeline en échec : ..."}`). À documenter comme tel — mentionner cette nuance d'intégration montre que tu as réfléchi au réel, pas juste recopié une doc.

**c) La politique d'alerte**

```bash
# Alerte : déclenche si pipeline_failures > 0 sur 5 min, notifie les 2 canaux
gcloud alpha monitoring policies create \
  --display-name="Pipeline climat en échec" \
  --condition-display-name="Au moins une erreur" \
  --condition-threshold-filter='metric.type="logging.googleapis.com/user/pipeline_failures"' \
  --condition-threshold-comparison=COMPARISON_GT \
  --condition-threshold-value=0 \
  --condition-threshold-duration=300s \
  --notification-channels=CHANNEL_ID_EMAIL,CHANNEL_ID_DISCORD
```

> **🏗️ Équivalent Terraform (IaC)**
> ```hcl
> # terraform/observability.tf
> resource "google_logging_metric" "pipeline_failures" {
>   name   = "pipeline_failures"
>   filter = "resource.type=\"workflows.googleapis.com/Workflow\" AND severity=ERROR"
>   metric_descriptor { metric_kind = "DELTA" value_type = "INT64" }
> }
>
> resource "google_monitoring_notification_channel" "discord" {
>   display_name = "Discord alertes"
>   type         = "webhook_tokenauth"
>   labels       = { url = var.discord_webhook_url }   # via variable secrète
> }
>
> resource "google_monitoring_alert_policy" "pipeline" {
>   display_name = "Pipeline climat en échec"
>   combiner     = "OR"
>   conditions {
>     display_name = "Au moins une erreur"
>     condition_threshold {
>       filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_failures.name}\""
>       comparison      = "COMPARISON_GT"
>       threshold_value = 0
>       duration        = "300s"
>     }
>   }
>   notification_channels = [google_monitoring_notification_channel.discord.id]
> }
> ```

### 5.2 CI/CD : déployer sans toucher à la console

> **🎓 Concept — Le code se déploie tout seul**
> Comment tes jobs Cloud Run passent-ils de ton éditeur à GCP ? Si la réponse est « je lance `gcloud run jobs deploy` à la main », ce n'est pas reproductible et c'est source d'erreurs. Un pipeline **CI/CD** automatise : à chaque `git push` sur `main`, le code est buildé, l'image poussée dans Artifact Registry, et le job Cloud Run mis à jour. Zéro intervention manuelle, traçable dans Git.

On utilise **Cloud Build** (natif GCP, cohérent avec le reste du projet). `cloudbuild.yaml` à la racine du dépôt :
```yaml
steps:
  # 1. Build l'image du job d'ingestion
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-t',
           '${_REGION}-docker.pkg.dev/$PROJECT_ID/climat/ingest-posts:$SHORT_SHA',
           './ingestion/posts']

  # 2. Push vers Artifact Registry
  - name: gcr.io/cloud-builders/docker
    args: ['push',
           '${_REGION}-docker.pkg.dev/$PROJECT_ID/climat/ingest-posts:$SHORT_SHA']

  # 3. Met à jour le Cloud Run Job avec la nouvelle image
  - name: gcr.io/google.com/cloudsdktool/cloud-sdk
    entrypoint: gcloud
    args: ['run', 'jobs', 'update', 'ingest-posts',
           '--image', '${_REGION}-docker.pkg.dev/$PROJECT_ID/climat/ingest-posts:$SHORT_SHA',
           '--region', '${_REGION}']

substitutions:
  _REGION: europe-west9

options:
  logging: CLOUD_LOGGING_ONLY
```

> **🎓 Concept — Le tag d'image par `$SHORT_SHA`**
> Chaque image est taguée avec le hash du commit Git (`$SHORT_SHA`). Avantage majeur : **traçabilité totale** — tu sais exactement quel commit tourne en production, et tu peux revenir à une version précise (rollback) en redéployant un ancien tag. Taguer `latest` partout est au contraire un anti-pattern (on ne sait plus ce qui tourne). Ce détail montre une vraie culture de déploiement.

Connexion à Git : un *trigger* Cloud Build qui écoute les push sur `main`.
```bash
gcloud builds triggers create github \
  --repo-name=desinformation-climat-bluesky \
  --repo-owner=GaelleRoger \
  --branch-pattern="^main$" \
  --build-config=cloudbuild.yaml
```

> **Note (conforme au périmètre du projet) :** comme convenu, cette brique CI/CD est **documentée comme voie de déploiement cible** plutôt que nécessairement branchée en continu pour le portfolio. Le `cloudbuild.yaml` est réel et fonctionnel ; le mettre en place est un geste simple le jour où tu veux l'activer. En entretien : « le déploiement est automatisable via ce pipeline Cloud Build, taguant chaque image par commit pour la traçabilité et le rollback ».

> **🤔 Pour aller plus loin :** où s'arrête le CI/CD du code et où commence celui de l'infra ? Le `cloudbuild.yaml` ci-dessus déploie le *code applicatif* (jobs). L'*infra* (buckets, IAM, Workflows) se déploierait par un `terraform plan` / `terraform apply` — souvent dans un pipeline séparé, car on ne veut pas recréer l'infra à chaque commit applicatif. Distinguer CI/CD applicatif et CI/CD d'infrastructure est une nuance d'architecte.

---

## Étape 6 — Visualisation & documentation finale

### 6.1 Le dashboard Looker Studio

> **🎓 Concept — Looker Studio vs Looker**
> **Looker Studio** (ex-Data Studio) : outil gratuit de dashboards, parfait ici. **Looker** (sans « Studio ») : plateforme BI d'entreprise payante avec son langage LookML. Pour un portfolio, Looker Studio suffit ; mais sache expliquer la différence en entretien.

Connexion : Looker Studio → Créer → Source de données → BigQuery → `gold.climate_x_weather`.

**Visualisations qui racontent l'histoire :**

1. **Série temporelle double-axe** : `temp_mean` (ligne) + `pct_disinfo` (ligne). LE graphique qui répond à la question — le taux de désinfo bouge-t-il avec la température ?
2. **Barres + marqueurs** : `disinfo_count` par jour, avec surlignage des jours `is_heatwave` / `is_coldsnap`.
3. **Nuage de points** : `temp_mean` (X) vs `pct_disinfo` (Y) — révèle une éventuelle corrélation.
4. **Scorecards** : volume total de posts climat, taux de désinfo global, confiance moyenne.
5. **Tableau** : jours au plus fort taux de désinfo, avec leur météo.

> **🎓 Concept — Un dashboard raconte une histoire**
> Ne balance pas 15 graphiques. Choisis-en 4-5 qui répondent à la question analytique. La narration (« lors des vagues de froid, voici ce qu'on observe sur la désinformation climatique ») distingue un dashboard pro d'un fouillis de métriques.

> **⚠️ Note de présentation responsable :** comme tu affiches de la « désinformation » estimée par un LLM, ajoute une note visible sur le dashboard : *classification automatique, estimation faillible, non vérifiée manuellement*. C'est honnête et ça te protège.

### 6.2 La documentation finale du dépôt

```
desinformation-climat-bluesky/
├── README.md              # vitrine : schéma, choix, captures du dashboard
├── ARCHITECTURE.md        # le doc d'architecture
├── ingestion/
│   ├── posts/             # job micro-batch (adapté de ton script)
│   └── weather/           # job météo quotidien
├── classification/        # prepare / batch / load_results (Gemini)
├── orchestration/         # pipeline.yaml (Cloud Workflows)
├── dataform/              # contenu synchronisé par Dataform (root dir = dataform)
│   ├── workflow_settings.yaml
│   └── definitions/       # sources, staging (silver), marts (gold)
├── terraform/             # infra déclarative (storage, iam, orchestration, observability)
├── cloudbuild.yaml        # pipeline CI/CD
└── docs/
    └── images/            # schéma d'archi, captures dashboard
```

> **🎓 Concept — Le README est ta première impression**
> Un recruteur passe 30 secondes sur ton README avant de décider s'il creuse. Ordre recommandé : (1) phrase d'accroche, (2) schéma d'architecture en image, (3) choix techniques en 3 lignes chacun, (4) captures du dashboard, (5) comment reproduire. Le visuel en haut.

### 6.3 Le « post-mortem » : ce qui fait la différence

Ajoute une section honnête « Limites & ce que je ferais différemment » :
- Limites méthodologiques : langue ≠ géo, termes de recherche imparfaits, classification LLM faillible et non validée humainement, météo Paris comme proxy.
- Limites éthiques : qualifier de la « désinformation » par un automate est délicat ; tu présentes une estimation, pas un verdict.
- Choix d'industrialisation assumés : Terraform montré en regard plutôt que 100 % déclaratif, CI/CD documenté plutôt que branché en continu — décisions de priorisation pour un portfolio, à énoncer comme telles.
- Ce que tu pousserais plus loin : infra 100 % Terraform avec backend distant + workspaces dev/prod, validation humaine d'un échantillon (precision/recall), classifieur supervisé entraîné sur les labels Gemini, backfill météo historique, tests d'intégration du Workflow.

> **🎓 Concept — L'humilité technique est un signal de séniorité**
> Les juniors présentent leur projet comme parfait. Les seniors en connaissent et énoncent les limites. Montrer que tu vois les angles morts de ta propre solution inspire confiance. Distinguer un *choix de priorisation* (« je n'ai pas tout déployé, volontairement ») d'une *lacune* (« j'ai oublié ») est précisément ce qui distingue un architecte.

---

## Récapitulatif des compétences démontrées

| Compétence | Où elle apparaît |
|---|---|
| Concevoir une architecture data end-to-end | Document d'architecture |
| Justifier des choix techniques | Tableaux comparatifs |
| Architecture lakehouse (GCS + BigQuery) | Étapes 2-3 |
| Ingestion micro-batch & pagination d'API | Étape 2 (basée sur ton script) |
| Gestion sécurisée des secrets | Secret Manager, étape 2 |
| IAM au moindre privilège | Comptes de service dédiés |
| Modélisation ELT en couches | Dataform bronze/silver/gold |
| Qualité de données par contrat | Assertions Dataform |
| Déduplication & idempotence | SQL silver + sélection des non-classés |
| Intégration d'un LLM en production | Gemini Batch Prediction, étape 4 |
| Pilotage d'un LLM (prompt, temperature, JSON) | Étape 4 |
| Maîtrise des coûts cloud | Jobs éphémères, batch -50 %, idempotence |
| FinOps : arbitrage Workflows vs Composer | Étape 4.4 |
| Orchestration & gestion de l'asynchrone (polling) | Cloud Workflows, étape 4.4 |
| Observabilité (run, pas seulement build) | Log-based metrics + alertes, étape 5 |
| Infrastructure-as-Code | Encadrés Terraform |
| CI/CD & traçabilité des déploiements | Cloud Build, tag par commit, étape 5 |
| Traçabilité / lineage | model_version, partitionnement |
| Restitution analytique narrative | Dashboard Looker Studio |
| Conteneurisation | Dockerfile + Cloud Run Jobs |
| Conscience éthique (sujet sensible) | Notes désinfo dans archi + dashboard |

---

## Annexe — Checklist de démarrage

- [ ] Projet GCP créé et facturation liée
- [ ] **Alerte budget configurée (à faire EN PREMIER)**
- [ ] APIs activées (run, storage, bigquery, dataform, aiplatform, secretmanager…)
- [ ] Secrets créés dans Secret Manager (bsky-handle, bsky-app-password, owm-api-key)
- [ ] Compte de service `ingestion-sa` + IAM moindre privilège
- [ ] Buckets GCS créés (bronze-posts, bronze-weather, vertex-staging)
- [ ] Job `ingest-posts` déployé + testé + planifié (15 min)
- [ ] Job `ingest-weather` déployé + planifié (quotidien)
- [ ] Tables externes BigQuery sur le bronze GCS
- [ ] Dépôt Dataform initialisé + connecté à Git
- [ ] Modèles silver/gold écrits avec assertions
- [ ] Pipeline de classification Gemini (prepare → batch → load) testé
- [ ] Workflow Cloud Workflows déployé (avec polling du Batch Prediction)
- [ ] Cloud Scheduler réduit à un simple déclencheur du Workflow
- [ ] Log-based metrics créées (échecs pipeline + assertions Dataform)
- [ ] Canaux d'alerte configurés (Discord + email) + politique d'alerte
- [ ] `cloudbuild.yaml` rédigé (CI/CD documenté)
- [ ] Encadrés Terraform des ressources clés rassemblés dans `/terraform`
- [ ] Dashboard Looker Studio construit (avec note « classification automatique »)
- [ ] README finalisé avec captures et post-mortem
