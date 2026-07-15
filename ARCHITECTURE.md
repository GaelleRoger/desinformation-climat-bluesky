# Climat × Météo — Architecture du pipeline de données

> Pipeline pseudo-temps-réel (micro-batch) sur Google Cloud Platform classant les posts Bluesky relatifs au climat (désinformation climatique : OUI/NON) et les croisant avec des données météo réelles.

**Auteur :** Gaëlle Roger
**Date :** Mai 2026
**Statut :** Document de conception

---

## 1. Contexte et objectif

### 1.1 Problème métier

Le discours climato-sceptique en ligne semble parfois réagir aux événements météorologiques : une vague de froid intense déclenche-t-elle une recrudescence de posts niant le réchauffement climatique (« et le réchauffement alors ? ») ? Une canicule provoque-t-elle au contraire des posts d'inquiétude climatique ?

Ce projet construit un pipeline de données permettant de **mesurer la relation entre les conditions météo réelles et la présence de désinformation climatique** sur le réseau social Bluesky.

### 1.2 Question analytique

> *Les épisodes météorologiques marquants (vagues de froid, canicules, tempêtes) influencent-ils le volume de posts climatiques et la proportion de désinformation climatique en français ?*

### 1.3 Objectif technique

Démontrer la conception et l'implémentation d'un **pipeline de données end-to-end** sur GCP, avec une ingestion pseudo-temps-réel (micro-batch), une architecture **lakehouse** (data lake GCS + entrepôt BigQuery), une modélisation ELT en couches, un enrichissement ML managé par **classification**, et une restitution analytique. L'accent est mis sur l' **architecture** : séparation des responsabilités, gestion des secrets, justification des choix, maîtrise des coûts, et qualité des données.

---

## 2. Périmètre et hypothèses

### 2.1 Dans le périmètre

- Ingestion micro-batch (toutes les 5-15 min) des posts Bluesky en français mentionnant le climat, via l'API `searchPosts`
- Stockage brut en data lake **GCS** (couche bronze : posts + météo)
- Modélisation en couches (bronze GCS → silver/gold BigQuery)
- Enrichissement par **classification binaire de désinformation climatique** via Gemini (Vertex AI Batch Prediction)
- Croisement avec données météo de Paris (OpenWeatherMap)
- Dashboard analytique (Looker Studio)

### 2.2 Hypothèses et limites assumées

| Hypothèse | Justification | Limite |
|---|---|---|
| **Posts FR = météo de Paris** | Les posts Bluesky n'ont quasiment jamais de géolocalisation. On utilise la langue comme proxy géographique grossier. | Un utilisateur francophone peut être au Québec, en Belgique, en Afrique. La météo de Paris n'est qu'une approximation du « ressenti météo » de l'audience. Simplification méthodologique. |
| **Détection du climat par termes de recherche** | L'API `searchPosts` filtre par mots-clés ; approche simple et transparente. | Risque de faux positifs (« climat social ») et faux négatifs (posts implicites). Atténué par le champ `is_climate_related` produit par le classifieur. |
| **Classification = jugement d'un LLM** | Gemini classe selon un prompt explicite, sans jeu d'entraînement annoté. | La sortie est une estimation, pas une vérité de terrain. On conserve un score de `confidence` et on documente le caractère faillible. Ce n'est pas un fact-checking certifié. |
| **« Désinformation » est défini par le prompt** | On encadre la définition dans la consigne donnée au modèle. | La frontière désinfo/opinion/ironie est intrinsèquement floue. On assume une définition opérationnelle, documentée et critiquable. |
| **Couverture non exhaustive** | `searchPosts` + filtrage par termes ne capturent qu'une partie des posts. | On raisonne en tendances relatives, pas en volumes absolus. |

---

## 3. Vue d'ensemble de l'architecture

### 3.1 Schéma de flux



### 3.2 Style architectural

Architecture **lakehouse en médaillon (medallion)**, entièrement orientée **batch / micro-batch** :

- **Lakehouse** : la donnée brute vit dans un data lake objet (GCS), interrogée par l'entrepôt (BigQuery) via des tables externes. 
- **Micro-batch (et non streaming pur)** : l'API `searchPosts` est interrogée fréquemment (toutes les 5-15 min) par un job planifié. On obtient un « quasi temps réel » sans la complexité ni le coût du streaming pur.
- **Médaillon (bronze/silver/gold)** : séparation stricte entre données brutes immuables (GCS), données nettoyées (silver), et données prêtes pour l'analyse (gold).

> **Pourquoi pas du streaming pur (firehose/Jetstream) ?** Considéré et écarté. Le firehose imposait un consumer WebSocket permanent (Cloud Run facturé en continu, principal poste de coût) pour un bénéfice marginal : à l'échelle d'un sujet de niche comme le climat en français, l'API `searchPosts` en micro-batch capture l'essentiel des posts avec une latence de quelques minutes, sans process permanent. Choix assumé de simplicité et de coût.

---

## 4. Choix techniques justifiés

### 4.1 Source posts : API `searchPosts` (micro-batch)

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **`searchPosts` en micro-batch (retenu)** | Filtrage par termes + langue côté serveur | Pas du temps réel strict (latence de quelques min), rate limits à gérer | ✅ |
| Firehose / Jetstream (WebSocket) | Vrai temps réel, exhaustif | Consumer permanent (coût continu), filtrage à faire soi-même, plus complexe | ❌ surdimensionné pour un sujet de niche |

**Décision :** `searchPosts` interrogé toutes les 5-15 min par un job planifié. Le filtrage (termes climat + `lang=fr`) se fait côté serveur Bluesky, donc on ne rapatrie que le pertinent. Plus de WebSocket à maintenir.

### 4.2 Ingestion : Cloud Run **Jobs** (planifiés)

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **Cloud Run Job + Cloud Scheduler (retenu)** | S'exécute, fait son travail, s'arrête (zéro coût au repos), conteneurisé, idéal pour du batch planifié | — | ✅ |
| Cloud Run **service** (WebSocket permanent) | Nécessaire pour du streaming | Facturé en continu | ❌ plus de streaming |

**Décision :** un **Cloud Run Job** pour les posts (déclenché toutes les 5-15 min par Cloud Scheduler) et un autre pour la météo (1×/jour). Un job s'exécute puis s'arrête : **pas de coût au repos**, ce qui remplace avantageusement l'ancien « interrupteur » manuel. C'est intrinsèquement économe.

### 4.3 Gestion des secrets : Secret Manager

Secret Manager stocke les identifiants API chiffrés, versionnés, avec accès contrôlé par IAM : chaque job lit ses secrets à l'exécution via son compte de service doté du rôle `roles/secretmanager.secretAccessor` (moindre privilège).

### 4.4 Couche bronze : Google Cloud Storage (lakehouse)

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **GCS (retenu)** | Stockage objet peu coûteux, immuable, indépendant du moteur de requête, classes de stockage + cycle de vie | Nécessite tables externes ou load vers BigQuery pour requêter | ✅ |
| BigQuery direct en bronze | Requêtable immédiatement | Stockage plus cher pour du brut peu interrogé, couplé au moteur | ⚠️ valable mais moins « lakehouse » |

**Décision :** bronze = fichiers JSON dans GCS, pour les posts comme pour la météo (`bronze-posts`, `bronze-weather`). Le job d'ingestion écrit un fichier horodaté par exécution (`dt=YYYY-MM-DD/posts_HHMM.json`), ce qui colle parfaitement au micro-batch. BigQuery lit le bronze via **tables externes** (pas de duplication de données). Un troisième bucket, `vertex-staging`, utilise la même techno (stockage objet) mais **n'appartient pas à la couche bronze** : c'est une zone de transit éphémère pour les échanges avec Vertex (voir §5.1bis). Cohérence : une seule technologie de stockage objet pour deux rôles distincts (donnée source immuable vs tampon de travail).

### 4.5 Entrepôt : BigQuery

Serverless, facturation au volume scanné, lecture native de GCS via tables externes, intégration directe avec Dataform, Vertex et Looker. L'alternative (Postgres/Cloud SQL) imposerait de gérer une instance et scalerait mal sur l'analytique.

### 4.6 Transformation : Dataform

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **Dataform (retenu)** | Natif GCP, gratuit, SQL + graphe de dépendances + tests d'assertions, intégré à BigQuery, versionné Git | Moins de plugins que dbt | ✅ |
| dbt Core | Standard de marché, large écosystème | À héberger/orchestrer soi-même | ❌ pour ce projet (mais bon à citer) |

**Décision :** Dataform fait le travail de dbt (modèles SQL versionnés, dépendances, tests) tout en étant intégré et gratuit dans GCP. *Savoir expliquer en entretien que Dataform et dbt résolvent le même problème est un atout.*

### 4.7 ML : classification binaire via Gemini (Vertex AI Batch Prediction)

**Pourquoi une classification et pas du sentiment ?** Le sentiment (positif/négatif) est trop flou pour la question posée : un post peut être négatif sans être de la désinformation, et inversement. On veut une **classification ciblée** : ce post relève-t-il de la désinformation climatique, OUI ou NON ?

**Pourquoi Gemini et pas un modèle entraîné (AutoML) ?** Parce qu'on n'a pas de jeu de données annoté. Entraîner un classifieur supervisé exigerait des milliers d'exemples étiquetés à la main. Gemini classe « zero-shot » à partir d'un prompt définissant la désinformation climatique — pas d'entraînement requis.

**Pourquoi Batch Prediction et pas des appels en temps réel ?** Trois raisons : (1) **coût** — Vertex applique une réduction de ~50 % sur les tokens en mode batch ; (2) **découplage** — on n'appelle pas un LLM dans le chemin critique de l'ingestion ; (3) **simplicité opérationnelle** — on accumule les posts, on envoie un fichier JSONL de quelques milliers d'entrées, Vertex traite en différé et écrit les résultats. C'est le pattern **« ingest fast, enrich later »**.

**Sortie du classifieur** (par post) :
- `is_climate_disinfo` (BOOL) — la cible : désinformation climatique OUI/NON
- `is_climate_related` (BOOL) — garde-fou : le post parle-t-il réellement de climat ? (filtre le hors-sujet avant calcul des taux)
- `confidence` (FLOAT 0-1) — niveau de confiance déclaré par le modèle

> **Note de rigueur :** la classification de désinformation est sensible et faillible. On la traite comme une **estimation automatique** (avec score de confiance), jamais comme un verdict. Les posts `is_climate_related = false` sont exclus du calcul du taux de désinfo.

### 4.8 Météo : OpenWeatherMap

Tu disposes déjà d'une clé API OpenWeatherMap : on l'utilise (lue via Secret Manager). Un appel quotidien à la météo **courante** de Paris suffit et reste dans le plan gratuit.

> **Point de vigilance documenté :** l'**historique** météo (backfill de jours passés) est un produit *payant* chez OpenWeatherMap. Pour notre besoin (croiser jour J posts × jour J météo), l'appel quotidien courant suffit. Un backfill historique nécessiterait soit un abonnement OWM, soit le recours à une source gratuite d'historique (ex. Open-Meteo Archive) — noté en évolutions futures.

### 4.9 Restitution : Looker Studio

Gratuit, connexion native à BigQuery, suffisant pour un dashboard de portfolio. *(Looker « entreprise », payant, avec LookML, serait surdimensionné ; on documentera la différence Looker Studio / Looker.)*

---

## 4bis. Industrialisation & exploitation

Cette section couvre ce qui distingue une plateforme de données industrialisée d'un simple pipeline qui fonctionne : la reproductibilité, l'orchestration robuste, l'observabilité et l'automatisation du déploiement. C'est le différenciateur clé pour un poste de Lead/Architecte.

### 4bis.1 Orchestration : Cloud Workflows

**Pourquoi un orchestrateur ?** Les étapes du pipeline ont des dépendances (la classification a besoin des posts ingérés et transformés ; le gold a besoin des labels). S'appuyer sur des horaires fixes de Cloud Scheduler pour respecter ces dépendances est fragile, surtout pour un job de Batch Prediction à **durée variable** (de quelques minutes à plusieurs heures). Un orchestrateur enchaîne explicitement, attend la fin réelle de chaque tâche (polling), et propage les erreurs.

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **Cloud Workflows (retenu)** | Serverless, zéro coût au repos, polling natif des jobs asynchrones, intégration GCP | Moins riche qu'Airflow pour des DAG très complexes | ✅ |
| Cloud Composer (Airflow managé) | Standard du marché, écosystème riche | Cluster permanent → plusieurs centaines d'€/mois même au repos | ❌ rédhibitoire avec 250 € |
| Schedulers chaînés (statu quo) | Simple | Dépendances non garanties, pas de polling → pipeline cassé en prod | ❌ red flag |

**Décision :** Cloud Workflows orchestre la séquence `ingestion → Dataform silver → préparation JSONL → Batch Prediction → polling → chargement → Dataform gold`. Cloud Scheduler est réduit à un unique déclencheur du Workflow. Le polling du job Vertex (boucle `switch`/`sleep` jusqu'à `JOB_STATE_SUCCEEDED`) est le mécanisme central qui gère l'asynchronisme.

### 4bis.2 Observabilité : Cloud Logging & Monitoring

**Pourquoi ?** Un pipeline qui échoue en silence est inutilisable en production. On instrumente deux types d'échec : les erreurs de Workflow/jobs, et les échecs d'assertions Dataform (qualité de données). Mécanisme : des **log-based metrics** transforment les logs d'erreur en métriques chiffrées, sur lesquelles une **politique d'alerte** Cloud Monitoring déclenche des notifications vers deux canaux — **webhook Discord** (instantané) et **email** (trace/fallback). On pense le *run*, pas seulement le *build*.

### 4bis.3 Infrastructure-as-Code : Terraform

**Pourquoi ?** Un déploiement par clics ou commandes manuelles n'est ni reproductible ni auditable. L'infra (buckets, datasets, secrets, IAM, Cloud Run Jobs, Workflows, ressources d'observabilité) doit être déclarative pour être dupliquée entre environnements (dev/prod) de façon idempotente.

**Périmètre dans ce projet :** par choix de priorisation (éviter le scope creep d'un portfolio), Terraform est présenté en **regard des ressources clés** plutôt qu'en déploiement 100 % déclaratif. Le message démontré : maîtrise du geste impératif (`gcloud`, pédagogique) *et* de son équivalent déclaratif (Terraform, industriel). Évolution naturelle documentée : backend distant + workspaces dev/prod.

### 4bis.4 CI/CD : Cloud Build

**Pourquoi ?** Le code applicatif (jobs Cloud Run) ne doit pas être déployé à la main. Un pipeline Cloud Build buildke l'image, la pousse dans Artifact Registry taguée par hash de commit (`$SHORT_SHA`, pour traçabilité et rollback), et met à jour le Cloud Run Job — déclenché à chaque push sur `main`.

**Périmètre dans ce projet :** documenté comme voie de déploiement cible (`cloudbuild.yaml` réel et fonctionnel) plutôt que branché en continu. Distinction notée : CI/CD applicatif (le code) vs CI/CD d'infrastructure (`terraform apply`), généralement séparés.

> **Note d'architecte :** ces quatre briques répondent directement aux angles morts classiques d'un projet « Data Engineer mid » : orchestration bouts-de-ficelle, déploiement clicodrome, pipeline aveugle. Les expliciter — y compris les choix de périmètre assumés — est ce qui positionne le projet au niveau Lead/Architecte.

---

## 5. Modèle de données

### 5.1 Couche Bronze — fichiers bruts immuables (GCS)

**Posts** — `gs://<projet>-bronze-posts/dt=YYYY-MM-DD/posts_HHMM.json`
Chaque exécution du job écrit un fichier contenant les posts récupérés (structure issue de `searchPosts` : `uri`, `author`, `record.text`, `record.createdAt`, `record.langs`, compteurs d'engagement…). On garde le brut tel quel.

**Météo** — `gs://<projet>-bronze-weather/dt=YYYY-MM-DD/weather.json`
Réponse OpenWeatherMap brute du jour pour Paris.

BigQuery lit ces fichiers via des **tables externes** :

**`bronze.posts_ext`** (table externe sur le bucket posts)
| Colonne | Type | Description |
|---|---|---|
| (schéma JSON auto / colonnes brutes) | — | Contenu des fichiers posts |
| `_FILE_NAME` (pseudo-colonne) | STRING | Permet de retrouver date/heure d'ingestion |

**`bronze.weather_ext`** (table externe sur le bucket météo)
| Colonne | Type | Description |
|---|---|---|
| (payload OWM) | — | Contenu brut météo |

> **Principe :** la couche bronze ne perd jamais d'information. Si la logique de parsing change, on recalcule tout depuis les fichiers GCS. C'est l'assurance-vie du pipeline — et avec GCS, c'est aussi l'option la moins chère pour conserver l'historique.

### 5.1bis Zone de staging Vertex (GCS, hors médaillon)

Le bucket `gs://<projet>-vertex-staging` est un **espace de transit**, à ne pas confondre avec la couche bronze. Il sert uniquement d'aire d'échange avec Vertex AI Batch Prediction :

- `input/batch.jsonl` — fichier généré par le job `prepare` : une ligne par post à classer (prompt + texte). Lu par Vertex au lancement du batch.
- `output/…` — fichiers de prédictions écrits par Vertex en fin de job. Lus par le job `load_results`, qui charge les résultats dans `silver.disinfo_labels`.

> **Distinction clé :** contrairement à la bronze (donnée source **immuable et non régénérable**), ces fichiers sont **éphémères et dérivés** — entièrement reconstructibles depuis `silver.posts_clean`. On peut les purger après chargement (règle de cycle de vie courte, ex. suppression à 7 jours). Les loger dans la couche bronze serait une erreur conceptuelle : la bronze est l'assurance-vie du pipeline, le staging n'est qu'un tampon de travail.

### 5.2 Couche Silver — données nettoyées et typées (BigQuery)

**`silver.posts_clean`**
| Colonne | Type | Description |
|---|---|---|
| `post_id` | STRING | Identifiant unique du post (clé de dédup, dérivé de l'URI) |
| `author_handle` | STRING | Handle de l'auteur |
| `text` | STRING | Texte du post |
| `lang` | STRING | Langue (filtrée sur `fr`) |
| `created_at` | TIMESTAMP | Date de création du post |
| `event_date` | DATE | Date (pour jointure météo) |
| `like_count`, `repost_count`, `reply_count` | INT | Engagement |
| `matched_terms` | ARRAY<STRING> | Termes de recherche ayant ramené le post |

**`silver.weather_daily`**
| Colonne | Type | Description |
|---|---|---|
| `event_date` | DATE | Jour |
| `temp_min`, `temp_max`, `temp_mean` | FLOAT | Températures (°C) |
| `precipitation_mm` | FLOAT | Précipitations |
| `weather_main` | STRING | Condition principale (OWM) |
| `is_heatwave`, `is_coldsnap` | BOOL | Flags d'épisode marquant (dérivés) |

**`silver.disinfo_labels`** — résultats de classification rapatriés
| Colonne | Type | Description |
|---|---|---|
| `post_id` | STRING | Clé de jointure |
| `is_climate_disinfo` | BOOL | Désinformation climatique OUI/NON |
| `is_climate_related` | BOOL | Le post parle-t-il vraiment de climat ? |
| `confidence` | FLOAT | Confiance déclarée (0-1) |
| `model_version` | STRING | Version du modèle Gemini utilisée (traçabilité) |
| `classified_at` | TIMESTAMP | Date de classification |

### 5.3 Couche Gold — tables analytiques (BigQuery)

**`gold.daily_disinfo`** — agrégation quotidienne
| Colonne | Type | Description |
|---|---|---|
| `event_date` | DATE | Jour |
| `post_count` | INT | Posts climat du jour (`is_climate_related = true`) |
| `disinfo_count` | INT | Posts classés désinfo |
| `pct_disinfo` | FLOAT | Taux de désinfo (disinfo_count / post_count) |
| `avg_confidence` | FLOAT | Confiance moyenne des classifications |

**`gold.climate_x_weather`** — la table-vedette, jointure finale
| Colonne | Type | Description |
|---|---|---|
| `event_date` | DATE | Clé de jointure |
| `post_count` | INT | Volume de posts climat |
| `disinfo_count` | INT | Volume de désinfo |
| `pct_disinfo` | FLOAT | Taux de désinfo du jour |
| `temp_mean`, `temp_max`, `temp_min` | FLOAT | Météo du jour |
| `is_heatwave`, `is_coldsnap` | BOOL | Épisode météo |

### 5.4 Graphe de dépendances Dataform

```
posts_ext ──▶ posts_clean ──┐
                            ├──▶ daily_disinfo ──▶ climate_x_weather
disinfo_labels ─────────────┘                            ▲
                                                         │
weather_ext ──▶ weather_daily ───────────────────────────┘
```

---

## 6. Stratégie de coûts (budget 250 €, crédit d'essai)

### 6.1 Postes de coût et garde-fous

| Service | Risque de coût | Garde-fou |
|---|---|---|
| **Cloud Run Jobs** | Facturé seulement pendant l'exécution | Jobs courts qui s'arrêtent → **zéro coût au repos**. Pas de process permanent. |
| **GCS (bronze)** | Très faible à ce volume | Classe Standard pour le récent ; règle de cycle de vie vers Nearline/Coldline + suppression au-delà de N jours. |
| **BigQuery requêtes** | Facturé au volume scanné | Tables externes lues de façon ciblée ; partitionnement par `event_date` ; clustering ; Dataform `incremental`. |
| **Vertex Batch Prediction** | Facturé aux tokens | **Mode batch (-50 %)** ; classification uniquement des posts pas encore classés ; lots de quelques milliers. |
| **Secret Manager** | Négligeable | Quelques secrets, peu d'accès. |
| **Cloud Workflows** | Quasi nul | Serverless, premières exécutions gratuites/mois, zéro coût au repos. **Évite Composer** (cluster permanent, plusieurs centaines d'€/mois). |
| **Cloud Logging / Monitoring** | Faible | Logs et métriques dans les quotas gratuits à ce volume. |
| **Cloud Build** | Faible | Minutes de build gratuites/jour ; CI/CD documenté, déclenché aux push. |
| **OpenWeatherMap** | Gratuit (météo courante) | Éviter l'API History (payante). |
| **Looker Studio** | Gratuit | — |

### 6.2 Filets de sécurité globaux

1. **Budget alert** GCP à 50 €, 100 €, 200 € (notification email) — à configurer en tout premier.
2. **Fréquence des jobs** : commencer à 15 min, ajuster selon le volume réel observé.
3. **Idempotence** : classification uniquement des posts non encore traités → aucun double-paiement Vertex.
4. **Rappel** : le crédit d'essai expire ~90 jours après ouverture → planifier la démo avant l'échéance.

### 6.3 Estimation grossière

Avec des jobs courts, un sujet de niche (faible volume) et le batch Vertex à -50 %, le coût mensuel réel devrait rester **très en-dessous de 10-20 €/mois**. L'abandon du WebSocket permanent supprime le poste de coût le plus élevé de la v1.

---

## 7. Décisions ouvertes (à trancher en cours de route)

| # | Décision | Quand | Critères |
|---|---|---|---|
| D1 | Fréquence exacte des micro-batchs (5 / 10 / 15 min) | Étape 3 | Volume réel de posts climat FR observé vs coût/rate limits. |
| D2 | Tables externes BigQuery **vs** load vers tables natives | Étape 2 | Externes = pas de duplication, plus simple ; natives = requêtes moins chères si gros volume. On part sur externes. |
| D3 | Liste exacte des termes de recherche climat | Étape 3 | À calibrer empiriquement (faux positifs/négatifs). |
| D4 | Formulation exacte du prompt de classification désinfo | Étape 4 | Définition opérationnelle de la désinformation climatique ; tests sur échantillon. |
| D5 | Seuil de `confidence` minimal pour retenir une classification | Étape 5 | Compromis précision/rappel selon observation. |

---

## 8. Évolutions futures possibles

- **Backfill historique météo** : via Open-Meteo Archive (gratuit) ou abonnement OWM History, pour étendre la fenêtre d'analyse vers le passé.
- **Généralisation multi-sujets** : paramétrer les termes de recherche pour suivre d'autres thèmes.
- **Géolocalisation fine** : inférer la localisation pour croiser avec la météo locale réelle.
- **Classifieur supervisé** : une fois assez de posts classés par Gemini, entraîner un modèle léger (moins cher à l'inférence) sur ces étiquettes — avec relecture humaine.
- **Infrastructure-as-Code complète** : passer des encadrés Terraform à un provisionnement 100 % déclaratif avec backend distant (état partagé) et workspaces dev/prod.
- **CI/CD branché en continu** : activer le trigger Cloud Build et ajouter un pipeline séparé `terraform plan/apply` pour l'infra.
- **Validation humaine** : interface de revue d'un échantillon de classifications pour mesurer la qualité réelle (precision/recall).
- **Tests d'intégration** : tester le Workflow de bout en bout sur un jeu de données synthétique.

---

## 9. Glossaire

| Terme | Définition |
|---|---|
| **Lakehouse** | Architecture combinant un data lake (stockage objet brut, ici GCS) et un entrepôt (BigQuery) qui le requête, souvent via tables externes. |
| **Micro-batch** | Traitement par petits lots fréquents (ici toutes les 5-15 min), à mi-chemin entre le batch et le streaming. |
| **Architecture médaillon** | Organisation des données en couches bronze (brut), silver (nettoyé), gold (analytique). |
| **ELT** | Extract-Load-Transform : on charge le brut d'abord, on transforme ensuite dans l'entrepôt. |
| **Table externe** | Table BigQuery dont les données restent dans GCS ; BigQuery lit les fichiers à la requête sans les copier. |
| **Batch Prediction** | Mode d'inférence Vertex AI où l'on soumet un fichier de nombreuses entrées traitées en différé, à coût réduit (~-50 % de tokens vs temps réel). |
| **Zero-shot** | Classification par un LLM sans exemples d'entraînement, uniquement guidée par la consigne (prompt). |
| **Idempotence** | Propriété d'une opération qu'on peut rejouer sans effet de bord (ici : ne classer que les posts non encore classés). |
| **Secret Manager** | Service GCP de stockage chiffré et versionné des secrets (clés API, mots de passe), avec accès contrôlé par IAM. |
| **Moindre privilège** | Principe de sécurité : n'accorder que les permissions strictement nécessaires (ici via comptes de service dédiés). |
| **Orchestration** | Coordination explicite des étapes d'un pipeline et de leurs dépendances (ici Cloud Workflows). |
| **Polling** | Interroger en boucle le statut d'une tâche asynchrone jusqu'à sa complétion, avec une pause entre chaque vérification. |
| **Infrastructure-as-Code (IaC)** | Décrire l'infrastructure en code déclaratif (Terraform) pour la rendre reproductible, versionnée et auditable. |
| **Observabilité** | Capacité à savoir ce que fait le système en production (logs, métriques, alertes). « Penser le run, pas seulement le build ». |
| **Log-based metric** | Métrique chiffrée dérivée d'un filtre sur les logs, permettant d'alerter sur des événements (ex. erreurs). |
| **CI/CD** | Intégration et déploiement continus : automatiser build, test et déploiement à chaque modification du code. |
| **FinOps** | Discipline de maîtrise et d'optimisation des coûts cloud (ici : arbitrages Workflows vs Composer, batch -50 %, jobs éphémères). |
