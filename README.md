# Music Recommendation & Festival RAG — PE2

Projet académique M2 composé de deux systèmes complémentaires déployés sur Google Cloud Platform :

1. **Système de recommandation musicale** — Filtrage collaboratif ALS (Alternating Least Squares) entraîné sur des données ListenBrainz réelles, exposé via une API FastAPI.
2. **Agent RAG Festivals 2026** — Agent conversationnel LangChain + Gemini répondant aux questions sur les festivals de musique français, s'appuyant sur une base vectorielle Pinecone.

Un **dashboard de monitoring** Cloud Run permet de suivre l'état du pipeline de données et de l'infrastructure GCP en temps réel.

---

## Table des matières

- [Architecture globale](#architecture-globale)
- [Prérequis](#prérequis)
- [Installation locale](#installation-locale)
- [Variables d'environnement](#variables-denvironnement)
- [Déploiement infrastructure avec Terraform](#déploiement-infrastructure-avec-terraform)
- [CI/CD — GitHub Actions](#cicd--github-actions)
- [Structure du projet](#structure-du-projet)
- [Pipeline de données](#pipeline-de-données)
- [API de recommandation](#api-de-recommandation)
- [Agent RAG Festivals](#agent-rag-festivals)
- [Dashboard de monitoring](#dashboard-de-monitoring)
- [Coûts GCP estimés](#coûts-gcp-estimés)

---

## Architecture globale

```
┌─────────────────────────────────────────────────────────────┐
│                        GitHub Actions                        │
│  CI (lint/type-check) │ Deploy (build+push) │ Terraform     │
└──────────────┬──────────────────┬────────────────┬──────────┘
               │                  │                │
               ▼                  ▼                ▼
┌──────────────────┐  ┌────────────────────┐  ┌──────────────────┐
│  GCP Artifact    │  │   Cloud Run        │  │  Terraform state  │
│  Registry        │  │  ┌──────────────┐  │  │  GCS bucket       │
│  (Docker images) │  │  │  music-api   │  │  │  (tfstate)        │
└──────────────────┘  │  │  :8000       │  │  └──────────────────┘
                       │  ├──────────────┤  │
                       │  │  music-      │  │
                       │  │  dashboard   │  │
                       │  │  :8080       │  │
                       │  └──────────────┘  │
                       └──────────┬─────────┘
                                  │ lit les données
                     ┌────────────┴──────────────┐
                     ▼                           ▼
         ┌───────────────────┐     ┌──────────────────────┐
         │   GCS Buckets     │     │   Secret Manager     │
         │  brainz-raw-*     │     │  PINECONE_API_KEY     │
         │  brainz-processed │     │  OPENAI_API_KEY       │
         │  projet-etude-m2  │     │  GOOGLE_API_KEY       │
         └───────────────────┘     └──────────────────────┘
                     │
         ┌───────────┴─────────────┐
         ▼                         ▼
┌─────────────────┐     ┌──────────────────────┐
│  GCE VM         │     │  Pinecone (externe)  │
│  (vm-spotify)   │     │  Index: "festival"   │
│  Airflow +      │     │  512 dimensions      │
│  FastAPI local  │     │  (text-embedding-    │
└─────────────────┘     │   3-small)           │
                         └──────────────────────┘
```

### Flux de données résumé

```
ListenBrainz dumps (120+ GB)
    └─► GCS brainz-raw-listenbrainz
            └─► scripts/parse_listens.py  ──► user/item matrix (.npz)
                    └─► src/train.py       ──► als_model.pkl
                            └─► GCS brainz-processed/models/
                                    └─► Cloud Run music-api ──► GET /recommend/{user_id}

offi.fr (scraping)
    └─► GCS projet-etude-m2/data_musique/festival/festivals_2026.json
            └─► Pinecone index "festival" (via OpenAI embeddings)
                    └─► Cloud Run music-api ──► POST /chat (agent Gemini)
```

---

## Prérequis

| Outil | Version minimale | Usage |
|---|---|---|
| Python | 3.10+ | Exécution locale |
| [uv](https://docs.astral.sh/uv/) | latest | Gestionnaire de paquets Python |
| [Terraform](https://www.terraform.io/) | 1.5+ | Provisionnement infra GCP |
| [gcloud CLI](https://cloud.google.com/sdk) | latest | Auth GCP + déploiement |
| Docker | latest | Build des images Cloud Run |
| Git | — | Cloner le repo |

**Comptes externes requis :**
- Compte **Google Cloud Platform** avec un projet créé et la facturation activée
- Compte **Pinecone** (tier gratuit suffit) — index `festival` à créer (512 dimensions, cosine)
- Clé API **OpenAI** — pour les embeddings (`text-embedding-3-small`)
- Clé API **Google** — pour le LLM Gemini (`gemini-2.5-flash`)

---

## Installation locale

```bash
# 1. Cloner le dépôt
git clone https://github.com/Thomas-Brvn/Recommandation_musique.git
cd Recommandation_musique

# 2. Installer uv (si pas déjà installé)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Installer les dépendances Python
uv sync

# Avec Airflow (optionnel, lourd)
uv sync --extra airflow

# Avec Spark (optionnel)
uv sync --extra spark

# 4. Copier et remplir les variables d'environnement
cp .env.example .env
# Éditer .env avec vos clés (voir section suivante)
```

### Lancer les APIs localement

```bash
# API de recommandation musicale (port 8000)
uv run uvicorn src.api.main:app --reload

# Agent RAG Festivals (port 8001, depuis src/app/ obligatoire pour les imports relatifs)
cd src/app && uv run uvicorn api:app --reload --port 8001

# Dashboard de monitoring (port 8080)
uv run uvicorn dashboard:app --reload --port 8080
```

---

## Variables d'environnement

Copiez `.env.example` en `.env` et renseignez toutes les valeurs :

```bash
# ── AWS (données brutes ListenBrainz historiques) ───────────────────────────
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_DEFAULT_REGION=eu-north-1          # bucket listen-brainz-data

# ── GCP ─────────────────────────────────────────────────────────────────────
# Authentification via Application Default Credentials :
#   gcloud auth application-default login
# ou via service account JSON :
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

GCS_BUCKET_PROCESSED=brainz-processed  # bucket GCS des données traitées
GCP_PROJECT_ID=projetetude-497218

# ── Festival RAG Agent ───────────────────────────────────────────────────────
PINECONE_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
GOOGLE_API_KEY=AIzaxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ── Airflow (optionnel) ──────────────────────────────────────────────────────
AIRFLOW_HOME=/opt/Recommandation_musique/.airflow
```

> **Sécurité :** Ne committez jamais `.env`. Les secrets en production sont gérés via **GCP Secret Manager** (voir `infra/secrets.tf`).

---

## Déploiement infrastructure avec Terraform

Tout le provisionnement GCP est défini dans `infra/`. Un seul `terraform apply` crée :

- **GCS buckets** : `brainz-raw-listenbrainz`, `brainz-raw-musicbrainz`, `brainz-processed`, `projet-etude-m2`, et le bucket pour le tfstate
- **GCE VM** `vm-spotify` : Debian 12, e2-medium, avec un startup script qui installe `uv`, clone le repo, et démarre Airflow et l'API FastAPI via systemd
- **Cloud Run** `music-api` (2 CPU, 8 Gi RAM) et `music-dashboard` (1 CPU, 512 Mi), en scale-to-zero
- **Artifact Registry** `music-api` pour stocker les images Docker
- **Règles firewall** : SSH restreint à votre IP, Airflow :8080, Portainer :9443, API :8000 (public)
- **Service account** `cloudrun-api` avec accès lecture GCS et Secret Manager

### Étapes de déploiement initial

**1. Préparer GCP**

```bash
# Se connecter
gcloud auth application-default login
gcloud config set project <YOUR_PROJECT_ID>

# Activer les APIs nécessaires
gcloud services enable \
  compute.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com
```

**2. Créer les secrets dans Secret Manager**

Les secrets sont *référencés* par Terraform (source `data`, pas créés). Créez-les manuellement avant le premier `apply` :

```bash
echo -n "votre-cle-pinecone" | gcloud secrets create PINECONE_API_KEY \
  --data-file=- --replication-policy=automatic

echo -n "sk-votre-cle-openai" | gcloud secrets create OPENAI_API_KEY \
  --data-file=- --replication-policy=automatic

echo -n "votre-cle-google" | gcloud secrets create GOOGLE_API_KEY \
  --data-file=- --replication-policy=automatic
```

**3. Créer le bucket Terraform state manuellement** (bootstrap one-time)

```bash
gsutil mb -l US-CENTRAL1 gs://tfstate-<YOUR_PROJECT_ID>
gsutil versioning set on gs://tfstate-<YOUR_PROJECT_ID>
```

Mettre à jour `infra/main.tf` avec le nom du bucket :

```hcl
backend "gcs" {
  bucket = "tfstate-<YOUR_PROJECT_ID>"
  prefix = "vm-spotify"
}
```

**4. Configurer les variables Terraform**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Éditer `terraform.tfvars` :

```hcl
project_id        = "mon-projet-gcp"
region            = "us-central1"
zone              = "us-central1-b"
vm_name           = "vm-spotify"
machine_type      = "e2-medium"
disk_size_gb      = 10
disk_image        = "debian-cloud/debian-12"
ssh_user          = "mon-utilisateur"
allowed_ssh_cidrs = ["<MON_IP>/32"]   # obtenir son IP : curl ifconfig.me
```

**5. Appliquer l'infrastructure**

```bash
cd infra
terraform init
terraform plan
terraform apply
```

Outputs utiles après l'apply :

```
vm_external_ip  = "x.x.x.x"
ssh_command     = "ssh mon-utilisateur@x.x.x.x"
api_url         = "https://music-api-xxxx-ew.a.run.app"
dashboard_url   = "https://music-dashboard-xxxx-ew.a.run.app"
```

**6. Se connecter à la VM et vérifier les services**

```bash
ssh mon-utilisateur@<vm_external_ip>

# Vérifier que les services systemd tournent bien
sudo systemctl status airflow fastapi

# Airflow UI accessible sur :
# http://<vm_external_ip>:8080
```

### Mettre à jour l'infrastructure

Toute modification dans `infra/` poussée sur `main` déclenche automatiquement `terraform apply` via GitHub Actions (`terraform.yml`).

---

## CI/CD — GitHub Actions

Quatre workflows automatisés gèrent le cycle de vie complet du projet :

### `ci.yml` — Lint & Import checks
Déclenché sur chaque push/PR vers `main`.
- Lint avec `ruff` (règles E, F, W)
- Type check avec `pyright` (non bloquant)
- Vérification des imports pour `festival RAG` et le module `recommandation`

### `deploy.yml` — Build & Deploy Cloud Run
Déclenché sur push vers `main` si `src/`, `Dockerfile*`, `dashboard.py`, `pyproject.toml` ou `uv.lock` sont modifiés.
1. Authentification GCP via **Workload Identity Federation** (sans clé de service account)
2. Build et push des deux images Docker vers Artifact Registry (`europe-north1`)
3. Déploiement de `music-api` et `music-dashboard` sur Cloud Run

### `terraform.yml` — Infra as Code
Déclenché sur push/PR vers `main` si `infra/` est modifié.
- **Sur PR** : `terraform plan` avec résultat posté en commentaire sur la PR
- **Sur push main** : `terraform apply` automatique

### `festival_update.yml` — Mise à jour hebdomadaire des festivals
Planifié tous les lundis à 6h UTC + déclenchement manuel via `workflow_dispatch`.
1. Scrape `offi.fr` pour les festivals de l'année cible
2. Valide le JSON produit (exit si vide)
3. Commite les changements dans le repo
4. (Optionnel) Ré-indexation ChromaDB sur runner self-hosted si `SELF_HOSTED_RUNNER=true`

### Secrets GitHub requis

| Secret | Valeur |
|---|---|
| `GCP_PROJECT_ID` | ID du projet GCP (ex: `projetetude-497218`) |
| `GCP_PROJECT_NUMBER` | Numéro du projet GCP (ex: `126997656473`) |

L'authentification utilise le **Workload Identity Federation** — aucune clé JSON de service account n'est stockée dans GitHub. Le service account `terraform-ci@<PROJECT_ID>.iam.gserviceaccount.com` doit avoir les rôles `Editor` + `roles/run.admin` + `roles/iam.serviceAccountUser`.

---

## Structure du projet

```
Recommandation_musique/
├── infra/                          # Terraform — infrastructure GCP complète
│   ├── main.tf                     # Provider Google + backend GCS
│   ├── variables.tf                # Déclaration des variables Terraform
│   ├── terraform.tfvars.example    # Template de configuration (à copier)
│   ├── cloudrun.tf                 # Services Cloud Run + Artifact Registry
│   ├── vm.tf                       # GCE VM avec startup script systemd complet
│   ├── network.tf                  # IP statique + règles firewall
│   ├── storage.tf                  # Buckets GCS (raw, processed, festival, tfstate)
│   ├── secrets.tf                  # Accès Secret Manager pour le service account Cloud Run
│   └── outputs.tf                  # Outputs : IP VM, commande SSH, URLs Cloud Run
│
├── src/
│   ├── app/                        # Festival RAG Agent
│   │   ├── agent/
│   │   │   ├── agent.py            # AgentExecutor LangChain (LLM Gemini ou Ollama local)
│   │   │   ├── tools.py            # Outil LangChain search_festival_store (requête Pinecone)
│   │   │   └── prompt.py           # Prompt système de l'agent RAG
│   │   ├── load_festival/
│   │   │   ├── get_festival.py     # Scraping offi.fr → JSON → GCS
│   │   │   └── festival_to_vectorstore.py  # GCS JSON → embeddings OpenAI → Pinecone
│   │   └── api.py                  # FastAPI : POST /chat, GET /sessions/{id}/history
│   │
│   ├── api/                        # API de recommandation musicale (Cloud Run)
│   │   ├── main.py                 # FastAPI : /recommend, /similar, /catalog, /library, /chat
│   │   ├── recommender.py          # Chargement modèle ALS depuis GCS + inférence
│   │   ├── catalog.py              # Service catalogue (artistes, titres)
│   │   ├── library.py              # Bibliothèque personnelle utilisateur (library.json)
│   │   └── cover_service.py        # Récupération des pochettes d'album
│   │
│   ├── models/
│   │   └── als_model.py            # Classe ALSRecommender (wrapper implicit library)
│   ├── train.py                    # Entraînement du modèle ALS
│   └── evaluate.py                 # Évaluation : precision@k, recall@k
│
├── scripts/
│   ├── download_to_s3_via_ec2.py   # Lance une EC2 AWS pour télécharger les dumps → S3
│   ├── parse_listens.py            # Parsing des dumps bruts ListenBrainz
│   ├── build_matrix.py             # Construction de la matrice user-item sparse (scipy CSR)
│   └── update_festival_db.py       # Scraping festivals + ré-indexation Pinecone
│
├── dags/
│   └── listenbrainz_pipeline.py    # DAG Airflow : ingestion → parsing → matrice → modèle
│
├── dashboard.py                    # Dashboard FastAPI + WebSocket (monitoring GCP temps réel)
├── templates/                      # Templates HTML Jinja2 du dashboard
│
├── Dockerfile                      # Image Docker music-api (python:3.11-slim + uv)
├── Dockerfile.dashboard            # Image Docker music-dashboard
├── docker-compose.yml              # Stack de développement local
│
├── pyproject.toml                  # Dépendances Python (uv), groupes optionnels airflow/spark
├── uv.lock                         # Lockfile uv (reproductibilité exacte)
├── .env.example                    # Template variables d'environnement
└── .github/workflows/              # Pipelines CI/CD GitHub Actions
    ├── ci.yml                      # Lint + import checks
    ├── deploy.yml                  # Build Docker + déploiement Cloud Run
    ├── terraform.yml               # Plan/Apply Terraform
    └── festival_update.yml         # Scraping hebdomadaire des festivals
```

---

## Pipeline de données

### 1. Acquisition des données brutes

Les dumps MusicBrainz et ListenBrainz sont volumineux (120+ GB). Le script lance une instance EC2 AWS éphémère qui télécharge directement vers GCS, sans passer par votre machine locale.

```bash
# MusicBrainz uniquement (~20 GB)
uv run python scripts/download_to_s3_via_ec2.py 1

# ListenBrainz uniquement (~120 GB)
uv run python scripts/download_to_s3_via_ec2.py 2

# Les deux en parallèle
uv run python scripts/download_to_s3_via_ec2.py 3
```

Les données atterrissent dans :
- `brainz-raw-listenbrainz/` — historiques d'écoutes utilisateurs (Parquet/tar.zst)
- `brainz-raw-musicbrainz/` — métadonnées musicales (JSON tar.xz)

### 2. Construction de la matrice user-item

```bash
# Parsing des dumps → écoutes structurées
uv run python scripts/parse_listens.py

# Construction de la matrice sparse (scipy CSR)
uv run python scripts/build_matrix.py
```

Produit dans `brainz-processed/processed/` :
- `user_item_matrix.npz` — matrice sparse utilisateur × piste
- `user_mapping.json` — mapping user_id → index entier
- `item_mapping.json` — mapping recording_id → index entier
- `mappings.json` — mappings consolidés (utilisé par l'API)

### 3. Entraînement du modèle ALS

```bash
uv run python src/train.py --matrix data/processed/user_item_matrix.npz
```

L'algorithme ALS (bibliothèque `implicit`) factorise la matrice d'écoutes implicites en vecteurs latents. Paramètres par défaut : 128 facteurs, régularisation 0.01. Le modèle est sauvegardé dans `brainz-processed/models/als_model.pkl`.

### 4. Évaluation

```bash
uv run python src/evaluate.py
```

Calcule precision@k et recall@k sur un split train/test. Résultats écrits dans `brainz-processed/models/evaluation_results.json`.

### Pipeline Airflow (optionnel)

Le DAG `dags/listenbrainz_pipeline.py` orchestre l'ensemble des étapes ci-dessus. La VM GCE le fait tourner en continu via systemd (`sudo systemctl status airflow`). Interface web sur `http://<vm_external_ip>:8080`.

---

## API de recommandation

`music-api` est une API FastAPI containerisée, déployée sur Cloud Run (2 CPU, 8 Gi). Le modèle ALS est chargé depuis GCS au démarrage du conteneur.

### Endpoints principaux

| Méthode | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Healthcheck Cloud Run |
| `GET` | `/recommend/{user_id}` | Recommandations personnalisées ALS |
| `GET` | `/similar/{track_id}` | Pistes similaires (item-item similarity) |
| `GET` | `/catalog/search?q=...` | Recherche dans le catalogue |
| `POST` | `/library/add` | Ajouter une piste à sa bibliothèque |
| `GET` | `/library` | Consulter sa bibliothèque |
| `POST` | `/chat` | Agent RAG Festivals (délégué à `src/app/`) |

### Exemple d'appel

```bash
curl https://music-api-xxxx-ew.a.run.app/recommend/12345
```

```json
{
  "user_id": "12345",
  "recommendations": [
    {"track_id": "abc123", "artist": "Radiohead", "title": "Karma Police", "score": 0.94},
    {"track_id": "def456", "artist": "Portishead", "title": "Glory Box", "score": 0.91}
  ]
}
```

---

## Agent RAG Festivals

L'agent répond aux questions en langage naturel sur les festivals de musique français 2026 (données scrappées depuis `offi.fr`).

### Architecture RAG

```
Question utilisateur
    └─► Agent LangChain (Gemini 2.5 Flash via GOOGLE_API_KEY)
            └─► Outil search_festival_store
                    └─► Pinecone index "festival" (512 dims, cosine)
                            └─► Top-k documents pertinents
                                    └─► Réponse contextuelle LLM
```

### Indexation des festivals

```bash
# 1. Scraper offi.fr et uploader le JSON sur GCS
uv run python src/app/load_festival/get_festival.py

# 2. Créer les embeddings OpenAI (text-embedding-3-small) et indexer dans Pinecone
uv run python src/app/load_festival/festival_to_vectorstore.py
```

L'index Pinecone `festival` doit être créé manuellement dans la console Pinecone avec **512 dimensions** et la métrique **cosine** avant de lancer l'indexation.

### Utilisation de l'API Festival

```bash
# Démarrer depuis src/app/ (imports relatifs obligatoires)
cd src/app && uv run uvicorn api:app --reload --port 8001

# Poser une question
curl -X POST http://localhost:8001/chat \
  -H "Content-Type: application/json" \
  -d '{"session_id": "session-1", "message": "Quels festivals jazz en juillet 2026 ?"}'

# Consulter l'historique d'une session
curl http://localhost:8001/sessions/session-1/history
```

L'historique de conversation est stocké en mémoire (`_sessions` dict) — il est perdu au redémarrage du serveur.

---

## Dashboard de monitoring

`music-dashboard` est une interface web FastAPI + WebSocket permettant de suivre en temps réel :

- **Buckets GCS** : taille et contenu de `brainz-raw-*` et `brainz-processed`
- **VM GCE** : instances actives et leur état
- **Cloud Run** : état des services déployés
- **Logs pipeline** : sortie en streaming du pipeline de données

Accessible sur l'URL Cloud Run du dashboard (output Terraform `dashboard_url`) ou en local sur `http://localhost:8080`.

---

## Coûts GCP estimés

| Ressource | Configuration | Coût estimé |
|---|---|---|
| GCE VM e2-medium | ~0.033 $/h, à arrêter quand inutilisé | ~24 $/mois max |
| Cloud Run music-api | 2 CPU, 8 Gi, scale-to-zero | Pay-per-request |
| Cloud Run music-dashboard | 1 CPU, 512 Mi, scale-to-zero | Quasi gratuit |
| GCS brainz-raw (~140 GB) | 0.02 $/GB/mois (EU) | ~2.8 $/mois |
| GCS brainz-processed (~5 GB) | 0.02 $/GB/mois (EU) | ~0.10 $/mois |
| Artifact Registry | Stockage images Docker | ~0.10 $/mois |
| Pinecone | Tier gratuit (1 index, 100k vecteurs) | 0 $ |
| OpenAI embeddings | Indexation ponctuelle des festivals | < 0.50 $ |

> **Conseil coût :** Arrêter la VM GCE quand le pipeline ne tourne pas :
> ```bash
> gcloud compute instances stop vm-spotify --zone us-central1-b
> ```

---

## Licence

MIT
