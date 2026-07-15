# Pipeline Climat × Météo — Détection de désinformation climatique sur Bluesky

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Run%20·%20BigQuery%20·%20Vertex%20AI-4285F4?logo=google-cloud&logoColor=white)
![Dataform](https://img.shields.io/badge/Dataform-ELT-1A73E8)
![Gemini](https://img.shields.io/badge/Gemini-Batch%20Prediction-8E75B2)
![Status](https://img.shields.io/badge/status-portfolio-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

> **Pipeline de données pseudo-temps-réel sur GCP qui classe la désinformation climatique en français sur Bluesky et la croise avec les conditions météo réelles, pour outiller les associations de fact-checking climat dans leur planification.**

---

## Démo

*Captures du dashboard Looker Studio — à venir une fois l'ingestion consolidée.*

<!-- Emplacements pour tes captures :
![Vue d'ensemble](docs/images/dashboard-overview.png)
![Corrélation météo × désinfo](docs/images/dashboard-correlation.png)
![Top jours désinfo](docs/images/dashboard-top-days.png)
-->

*Le dashboard n'est volontairement pas hébergé publiquement (données publiques mais classification automatique, sans validation humaine — voir section « Résultats & limites »).*

---

## Problématique

Les vagues de discours climato-sceptique en ligne semblent réagir aux événements météorologiques : une vague de froid intense déclenche-t-elle une recrudescence de posts niant le réchauffement climatique (« et le réchauffement alors ? ») ? Une canicule provoque-t-elle au contraire des posts d'inquiétude climatique ?

**Enjeux :**
- Les associations de fact-checking climat comme [Quota Climat](https://quotaclimat.org/) ou [Les Shifters](https://www.theshifters.org/) mobilisent des bénévoles pour analyser et répondre à la désinformation climatique sur les réseaux sociaux. Anticiper les pics de volume permettrait de mieux planifier ces mobilisations.
- Il n'existe pas d'outil grand public reliant les conditions météorologiques réelles au volume et à la nature du discours climatique en ligne — les études existantes sont ponctuelles, académiques et pas actionnables opérationnellement.
- Les APIs sociales fermées (X, Reddit payant) rendent difficile ce type d'analyse à l'échelle. Bluesky, réseau ouvert avec un firehose public, ouvre une opportunité méthodologique.

## Solution

Un pipeline ELT industrialisé sur GCP qui ingère les posts Bluesky en français mentionnant le climat, les classe en désinformation climatique OUI/NON via Gemini (Vertex AI Batch Prediction), et les croise quotidiennement avec la météo de Paris pour produire une table analytique et un dashboard.

**Positionnement :** projet d'**architecture de données** (pas d'affirmations scientifiques sur la corrélation elle-même). Le livrable est le pipeline reproductible, industrialisé et documenté — le dashboard est un artefact démonstratif de ce que ce pipeline permettrait à un partenaire associatif de suivre en continu.

---

## Fonctionnalités

- **Ingestion micro-batch (15 min)** : consumer Cloud Run Jobs sur l'API Bluesky `searchPosts`, filtrage `lang=fr` + termes climat côté serveur, dédoublonnage.
- **Data lake GCS partitionné** : posts et météo bruts en JSONL immuable, tables externes BigQuery pour requêter sans dupliquer.
- **Modélisation ELT en couches médaillon** : Dataform orchestre bronze → silver → gold avec assertions qualité (unicité, non-nullité).
- **Classification de désinformation via Gemini Batch Prediction** : SDK Python, prompt de classification zero-shot, mode batch (-50 % tokens vs temps réel), champs garde-fous (`is_climate_related`, `confidence`).
- **Orchestration robuste par Cloud Workflows** : polling du job Vertex à durée variable, gestion des dépendances entre étapes, un seul déclencheur Cloud Scheduler.
- **Observabilité** : log-based metrics sur les erreurs Workflow et les échecs d'assertions Dataform, alertes Cloud Monitoring vers webhook Discord + email.
- **Secrets sous contrôle** : identifiants Bluesky et OpenWeatherMap dans Secret Manager, comptes de service au moindre privilège.

---

## Stack technique

| Couche | Technologies |
| --- | --- |
| **Sources** | Bluesky API (`searchPosts`), OpenWeatherMap |
| **Ingestion** | Cloud Run Jobs (Python 3.11, Docker), Cloud Scheduler |
| **Stockage brut** | Cloud Storage (data lake bronze, JSONL partitionné Hive-style) |
| **Entrepôt & transformations** | BigQuery (tables externes), Dataform (SQL + assertions) |
| **ML managé** | Vertex AI Batch Prediction, Gemini 2.5 Flash |
| **Orchestration** | Cloud Workflows (polling asynchrone) |
| **Observabilité** | Cloud Logging (log-based metrics), Cloud Monitoring, Discord webhook |
| **Sécurité** | Secret Manager, IAM comptes de service dédiés |
| **Restitution** | Looker Studio |
| **Industrialisation documentée** | Terraform (encadrés IaC), Cloud Build (CI/CD) |

---

## Architecture

Vue d'ensemble :

![Architecture](docs/images/architecture_v3.png)

<details>
<summary><b>Diagrammes détaillés (Mermaid)</b></summary>

Deux vues complémentaires, versionnées dans `docs/diagrams/` :

- [`data-flow.mmd`](docs/diagrams/data-flow.mmd) — flux de données : buckets GCS, tables BigQuery, jobs individuels et échanges JSONL avec Vertex.
- [`industrialization.mmd`](docs/diagrams/industrialization.mmd) — orchestration Cloud Workflows (polling), sécurité transverse, chaîne d'observabilité.

</details>

Pour la conception détaillée (choix techniques justifiés, modèle de données, stratégie FinOps) : [`ARCHITECTURE.md`](ARCHITECTURE.md).
Pour la démarche pas-à-pas : [`TUTORIEL.md`](TUTORIEL.md).

---

## Résultats & limites

### Ce qui est mesurable

| Indicateur | Valeur | Contexte |
| --- | --- | --- |
| Posts climat FR / jour | *à consolider après période d'observation* | Volume dépendant de l'actualité climatique |
| Coût mensuel réel | *à consolider* | Objectif : < 20 €/mois grâce au batch et aux jobs éphémères |
| Latence bout-en-bout | ~15-20 min | De la publication d'un post à sa présence en silver |
| Réduction coût ML | -50 % tokens | Batch Prediction vs appels temps réel Gemini |

### Ce qui ne l'est pas (honnêteté méthodologique)

- **Pas de precision/recall du classifieur** : aucun jeu de test annoté humainement. La qualité de la classification est estimée par le modèle lui-même (champ `confidence`), pas validée.
- **Pas de « désinformation » au sens fact-checké** : la sortie du modèle est une **estimation automatique** selon une définition opérationnelle donnée en prompt, pas un verdict de véracité.
- **Proxy géographique grossier** : les posts en français sont croisés avec la météo de Paris. Un francophone peut poster depuis le Québec ou la Belgique.
- **Couverture non exhaustive** : l'API `searchPosts` avec filtrage par termes ne capture qu'une fraction du discours climatique — on raisonne en tendances relatives, pas en volumes absolus.

Ces limites sont **assumées et documentées** dans [`ARCHITECTURE.md`](ARCHITECTURE.md) — un pipeline honnête sur ses angles morts est plus utile qu'un chiffre inventé.

---

## Cas d'usage cible

Ce pipeline est conçu comme un **outil d'aide à la planification** pour des acteurs qui suivent la désinformation climatique en ligne, notamment :

- **Associations de fact-checking climat** (type Quota Climat, Les Shifters) : anticiper les pics de mobilisation bénévole en corrélant les épisodes météo à venir avec les hausses historiques de désinformation.
- **Chercheurs en communication scientifique** : disposer d'un jeu de données structuré (posts × classification × météo) pour étudier les mécanismes de réaction au climat.
- **Cellules de veille éditoriale** (médias, communication publique) : détecter en quasi temps réel les pics de discours climato-sceptique pour ajuster la communication.

Le projet ne prétend pas remplacer une revue humaine — il produit une **couche de préqualification** que ces acteurs peuvent ensuite exploiter.

---

## Quick start

```bash
# 1. Cloner le repository
git clone https://github.com/GaelleRoger/desinformation-climat-bluesky.git
cd desinformation-climat-bluesky

# 2. Prérequis
# - Un projet GCP avec facturation active
# - gcloud CLI authentifié : gcloud auth login
# - Une clé API OpenWeatherMap (plan gratuit suffit)
# - Un app password Bluesky (créé dans les paramètres du compte)

# 3. Provisionnement minimal (résumé, détail dans TUTORIEL.md)
export PROJECT_ID="votre-projet-gcp"
gcloud config set project $PROJECT_ID

# Activer les APIs
gcloud services enable run.googleapis.com storage.googleapis.com \
  bigquery.googleapis.com dataform.googleapis.com aiplatform.googleapis.com \
  secretmanager.googleapis.com workflows.googleapis.com cloudscheduler.googleapis.com

# Stocker les secrets
printf "votre.handle.bsky.social" | gcloud secrets create bsky-handle --data-file=-
printf "xxxx-xxxx-xxxx-xxxx"      | gcloud secrets create bsky-app-password --data-file=-
printf "votre_cle_owm"             | gcloud secrets create owm-api-key --data-file=-

# 4. Suivre TUTORIEL.md pour la mise en place complète des jobs, Dataform et Workflows
```

Prêt en environ **30 minutes** pour un premier run d'ingestion (le tutoriel détaille chaque étape avec les concepts sous-jacents).

---

## Roadmap

- [x] Ingestion micro-batch Bluesky + météo quotidienne (Cloud Run Jobs)
- [x] Data lake GCS bronze avec tables externes BigQuery
- [x] Modélisation médaillon Dataform (silver + gold)
- [x] Classification de désinformation via Gemini Batch Prediction
- [x] Orchestration Cloud Workflows avec polling asynchrone
- [x] Observabilité Cloud Monitoring (Discord + email)
- [x] Dashboard Looker Studio (structure)
- [ ] Infrastructure-as-Code complète en Terraform (backend distant + workspaces dev/prod)
- [ ] CI/CD branché en continu (Cloud Build applicatif + Terraform)
- [ ] Validation humaine sur échantillon (precision/recall du classifieur)
- [ ] Backfill météo historique via Open-Meteo Archive
- [ ] Tests d'intégration du Workflow sur jeu synthétique
- [ ] Classifieur supervisé entraîné sur les labels Gemini (coût inférieur à l'inférence)

---

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — document d'architecture détaillé (choix techniques justifiés, modèle de données, FinOps, industrialisation, glossaire).
- [`TUTORIEL.md`](TUTORIEL.md) — guide pas-à-pas expliquant chaque brique avec les concepts d'architecte qui la sous-tendent.
- [`docs/diagrams/`](docs/diagrams/) — diagrammes Mermaid interactifs (data flow et industrialisation).

---

## Contact

**Gaëlle Roger** — Data Analyst en transition vers Data Architect
[LinkedIn](https://www.linkedin.com/in/VOTRE_PROFIL) · [Email](mailto:VOTRE_EMAIL)

Les issues et pull requests sont les bienvenues pour toute suggestion d'amélioration architecturale ou méthodologique.

---

*Projet réalisé dans le cadre d'une transition professionnelle vers un poste d'architecte de données. Objectif : démontrer la conception, l'industrialisation et l'exploitation d'un pipeline de bout en bout sur GCP.*
