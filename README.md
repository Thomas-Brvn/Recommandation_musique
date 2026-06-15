# Système de Recommandation Musicale & Agent RAG Festivals 2026

Projet académique M2 — deux systèmes d'intelligence artificielle complémentaires, déployés sur Google Cloud Platform et exposés via une API unifiée.

1. **Système de recommandation musicale** — Filtrage collaboratif par factorisation matricielle ALS (Alternating Least Squares) entraîné sur les données réelles de ListenBrainz (plus de 120 Go d'historiques d'écoute).
2. **Agent RAG Festivals 2026** — Agent conversationnel basé sur LangChain et Ollama (LLM local), capable de répondre en langage naturel à des questions sur les festivals de musique français de l'été 2026, en s'appuyant sur une base vectorielle ChromaDB.

Un **dashboard de monitoring** Cloud Run permet de suivre l'état du pipeline de données et de l'infrastructure GCP en temps réel.

---

## Table des matières

- [Architecture globale](#architecture-globale)
- [Algorithme ALS — comment ça marche](#algorithme-als--comment-ça-marche)
- [Agent RAG — comment ça marche](#agent-rag--comment-ça-marche)
- [Prérequis](#prérequis)
- [Installation locale](#installation-locale)
- [Variables d'environnement](#variables-denvironnement)
- [Pipeline de données complet](#pipeline-de-données-complet)
- [Entraînement du modèle](#entraînement-du-modèle)
- [Évaluation du modèle](#évaluation-du-modèle)
- [API de recommandation — référence complète](#api-de-recommandation--référence-complète)
- [Agent RAG Festivals](#agent-rag-festivals)
- [Dashboard de monitoring](#dashboard-de-monitoring)
- [Infrastructure GCP avec Terraform](#infrastructure-gcp-avec-terraform)
- [CI/CD — GitHub Actions](#cicd--github-actions)
- [Docker et déploiement local](#docker-et-déploiement-local)
- [Structure du projet](#structure-du-projet)
- [Coûts GCP estimés](#coûts-gcp-estimés)

---

## Architecture globale

```
┌──────────────────────────────────────────────────────────────────────┐
│                          GitHub Actions                               │
│   ci.yml (lint)  │  deploy.yml (Cloud Run)  │  terraform.yml (infra) │
│                  │  festival_update.yml (hebdo scraping)             │
└────────┬─────────────────────┬──────────────────┬────────────────────┘
         │                     │                  │
         ▼                     ▼                  ▼
┌──────────────────┐  ┌─────────────────────┐  ┌───────────────────┐
│  Artifact        │  │    Cloud Run        │  │  Terraform state  │
│  Registry        │  │ ┌─────────────────┐ │  │  (GCS bucket)     │
│  (Docker images) │  │ │  music-api      │ │  └───────────────────┘
└──────────────────┘  │ │  :8000          │ │
                       │ │  2 CPU / 8 Gi   │ │
                       │ ├─────────────────┤ │
                       │ │  music-dashboard│ │
                       │ │  :8080          │ │
                       │ │  1 CPU / 512 Mi │ │
                       │ └─────────────────┘ │
                       └──────────┬──────────┘
                                  │
               ┌──────────────────┼──────────────────┐
               ▼                  ▼                   ▼
   ┌────────────────────┐ ┌──────────────┐ ┌────────────────────┐
   │   GCS Buckets      │ │  Secret Mgr  │ │   GCE VM           │
   │  brainz-raw-lb     │ │  (clés API)  │ │   vm-spotify       │
   │  brainz-raw-mb     │ └──────────────┘ │   Airflow + API    │
   │  brainz-processed  │                  └────────────────────┘
   │  projet-etude-m2   │
   └────────────────────┘
```

### Flux de données résumé

```
ListenBrainz dumps (120+ Go)
    └─► GCS brainz-raw-listenbrainz
            └─► scripts/parse_listens.py  ──► user_item_matrix.npz
                    └─► src/train.py       ──► als_model.pkl
                            └─► GCS brainz-processed/models/
                                    └─► music-api  ──► GET /recommend/{user_id}
                                                   └─► GET /similar/{track_id}

MusicBrainz dumps (métadonnées)
    └─► GCS brainz-raw-musicbrainz
            └─► Dataproc (Spark)  ──► track_dedup_map.json
                    └─► GCS brainz-processed/processed/
                            └─► music-api  ──► GET /catalog/search

offi.fr (scraping hebdomadaire)
    └─► data/festivals_2026.json
            └─► ChromaDB (sentence-transformers MiniLM)
                    └─► music-api  ──► POST /festival/chat (agent Ollama)
```

---

## Algorithme ALS — comment ça marche

### Principe du filtrage collaboratif

Le filtrage collaboratif part d'une observation simple : si deux utilisateurs ont aimé les mêmes chansons par le passé, ils ont probablement des goûts similaires et l'un d'eux appréciera ce que l'autre a écouté.

On modélise cela sous forme d'une **matrice user-item** :

```
                  Radiohead  Portishead  Massive Attack  Björk   ...
Utilisateur 1  [    5,         0,              3,          0    ]
Utilisateur 2  [    0,         4,              0,          2    ]
Utilisateur 3  [    3,         5,              4,          0    ]
         ...
```

Chaque cellule représente le nombre d'écoutes (une interaction implicite — l'utilisateur n'a pas noté explicitement). La matrice est **extrêmement creuse** (sparse) : la plupart des utilisateurs n'ont écouté qu'une infime fraction du catalogue.

### Factorisation matricielle (ALS)

ALS décompose cette grande matrice **M** (n_users × n_items) en deux matrices de faible rang :

```
M  ≈  U  ×  V^T
     (n_users × k)   (n_items × k)^T
```

- **U** : matrice des vecteurs latents utilisateurs — chaque ligne est un "profil" en k dimensions
- **V** : matrice des vecteurs latents items — chaque ligne est un "profil" du morceau en k dimensions
- **k** : nombre de facteurs latents (128 dans ce projet)

Ces dimensions latentes capturent des concepts musicaux implicites (style, tempo, genre...) sans qu'on ait à les définir manuellement.

### La formule de score

La **prédiction d'affinité** d'un utilisateur `u` pour un morceau `i` est simplement le **produit scalaire** de leurs vecteurs latents :

```
score(u, i) = U[u] · V[i]
```

Plus ce score est élevé, plus l'utilisateur est susceptible d'apprécier ce morceau.

### L'optimisation alternée

"Alternating" signifie que l'algorithme optimise alternativement :

1. **Fixer V, optimiser U** : pour chaque utilisateur, calculer analytiquement le vecteur qui minimise l'erreur de reconstruction
2. **Fixer U, optimiser V** : pour chaque item, même chose

Cette alternance converge vers un minimum local en un nombre fixe d'itérations (15 dans ce projet). La régularisation L2 (λ = 0.01) évite l'overfitting.

### Données implicites

ListenBrainz ne fournit pas des notes (1-5 étoiles) mais des **comptages d'écoutes**. La bibliothèque `implicit` transforme ces données avec une pondération de confiance :

```
confidence(u, i) = 1 + α × count(u, i)
```

Plus un utilisateur a écouté un morceau souvent, plus le modèle est "confiant" que c'est une vraie préférence.

### Paramètres du modèle

| Paramètre | Valeur | Rôle |
|---|---|---|
| `factors` | 128 | Nombre de dimensions latentes |
| `regularization` | 0.01 | Pénalisation L2 anti-overfitting |
| `iterations` | 15 | Nombre de passes d'optimisation alternée |
| `use_gpu` | False | Calcul CPU (GPU possible avec CUDA) |
| `random_state` | 42 | Reproductibilité |

### Similarité item-item

En plus des recommandations personnalisées, le modèle permet de trouver des morceaux similaires à un morceau donné. La similarité entre deux morceaux est calculée via le **cosinus** entre leurs vecteurs latents V :

```
similarité(i, j) = cos(V[i], V[j]) = (V[i] · V[j]) / (‖V[i]‖ × ‖V[j]‖)
```

---

## Agent RAG — comment ça marche

### Principe du RAG (Retrieval-Augmented Generation)

Un LLM (Large Language Model) seul ne peut pas connaître le programme des festivals de l'été 2026 — ces données n'étaient pas dans son corpus d'entraînement. Le RAG résout ce problème en deux phases :

1. **Retrieval** (récupération) : chercher les documents pertinents dans une base de connaissances
2. **Generation** (génération) : passer ces documents au LLM comme contexte pour qu'il génère une réponse informée

```
Question : "Y a-t-il Orelsan dans un festival en juillet ?"
    │
    ▼
[Embedding de la question]
    │  paraphrase-multilingual-MiniLM-L12-v2
    ▼
[Recherche par similarité vectorielle dans ChromaDB]
    │  Top-5 festivals les plus proches sémantiquement
    ▼
[Contexte injecté dans le prompt système]
    │
    ▼
[LLM Ollama - llama3.2:3b]
    │  Génère une réponse en français basée uniquement sur les faits trouvés
    ▼
Réponse structurée avec nom du festival, dates, lieu, artistes, billetterie
```

### Construction de la base vectorielle (ChromaDB)

1. Les données des festivals sont scrappées depuis `offi.fr` et stockées en JSON
2. Pour chaque festival, un **texte représentatif** est créé (liste des artistes)
3. Ce texte est converti en vecteur de 384 dimensions via le modèle `paraphrase-multilingual-MiniLM-L12-v2` (sentence-transformers, multilingue, fonctionne hors ligne)
4. Ces vecteurs sont stockés dans ChromaDB avec les métadonnées (nom, dates, lieu, billetterie)

L'index ChromaDB utilise la métrique **cosinus** et un index **HNSW** (Hierarchical Navigable Small World) pour une recherche approximative des plus proches voisins très rapide.

### L'agent LangChain

L'agent est construit avec `create_tool_calling_agent` de LangChain. Il dispose d'un seul outil : `search_festival_store`. Le prompt système lui impose de **toujours** appeler cet outil avant de répondre — garantissant que les réponses sont toujours basées sur des données réelles.

L'historique de conversation est maintenu en mémoire par session (`session_id`), permettant des échanges multi-tours cohérents.

---

## Prérequis

| Outil | Version minimale | Usage |
|---|---|---|
| Python | 3.10+ | Exécution locale |
| [uv](https://docs.astral.sh/uv/) | latest | Gestionnaire de paquets Python |
| [Ollama](https://ollama.ai/) | latest | LLM local pour l'agent festivals |
| [Terraform](https://www.terraform.io/) | 1.5+ | Provisionnement infra GCP |
| [gcloud CLI](https://cloud.google.com/sdk) | latest | Auth GCP + déploiement |
| Docker | latest | Build des images Cloud Run |

**Comptes et services requis :**

| Service | Usage | Coût |
|---|---|---|
| Google Cloud Platform | Hébergement, stockage, calcul | Voir section coûts |
| GitHub | CI/CD via GitHub Actions | Gratuit |

**Aucune clé API payante n'est requise** pour faire tourner le système en local — le LLM (Ollama) et les embeddings (sentence-transformers) fonctionnent entièrement en local.

---

## Installation locale

### 1. Cloner le dépôt

```bash
git clone https://github.com/Thomas-Brvn/Recommandation_musique.git
cd Recommandation_musique
```

### 2. Installer uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Installer les dépendances Python

```bash
# Installation standard
uv sync

# Avec Airflow (pipeline orchestration — lourd, ~500 Mo)
uv sync --extra airflow

# Avec Spark (traitement distribué — lourd)
uv sync --extra spark
```

`uv` lit `pyproject.toml` et installe exactement les versions définies dans `uv.lock` pour une reproductibilité parfaite.

### 4. Installer et configurer Ollama

L'agent festivals nécessite Ollama pour faire tourner le LLM localement :

```bash
# Installer Ollama (macOS)
brew install ollama

# Télécharger le modèle (1.9 Go)
ollama pull llama3.2:3b

# Démarrer le serveur Ollama (en arrière-plan)
ollama serve
```

### 5. Configurer les variables d'environnement

```bash
cp .env.example .env
# Éditer .env selon votre configuration
```

### 6. Lancer les services localement

```bash
# API principale (recommandation + agent festival) — port 8000
uv run uvicorn src.api.main:app --reload

# Dashboard de monitoring — port 8080
uv run uvicorn dashboard:app --reload --port 8080
```

L'interface web du player est accessible sur `http://localhost:8000/player`.
La documentation interactive (Swagger) est sur `http://localhost:8000/docs`.

---

## Variables d'environnement

Copiez `.env.example` en `.env` :

```bash
# ── GCP ─────────────────────────────────────────────────────────────────────
# Authentification via Application Default Credentials :
#   gcloud auth application-default login
GCP_PROJECT_ID=projetetude-497218

# Buckets GCS
GCS_BUCKET_PROCESSED=brainz-processed
GCS_MODEL_KEY=models/als_model.pkl
GCS_MATRIX_KEY=processed/user_item_matrix.npz
GCS_MAPPINGS_KEY=processed/mappings.json
GCS_CATALOG_KEY=processed/track_dedup_map.json

# ── AWS (données historiques brutes — optionnel si déjà sur GCS) ─────────────
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_DEFAULT_REGION=eu-north-1

# ── Ollama (agent festival) ───────────────────────────────────────────────────
OLLAMA_MODEL=llama3.2:3b                # modèle Ollama à utiliser
OLLAMA_BASE_URL=http://localhost:11434  # URL du serveur Ollama

# ── ChromaDB (vector store des festivals) ────────────────────────────────────
CHROMA_PATH=./data/chroma               # stockage local (défaut)
CHROMA_HTTP=false                       # true = mode serveur distant
CHROMA_HOST=localhost                   # si CHROMA_HTTP=true
CHROMA_PORT=8000                        # si CHROMA_HTTP=true
FESTIVALS_FILE=data/festivals_2026.json # source JSON des festivals

# ── Chemins locaux (fallback si GCS non configuré) ───────────────────────────
MODEL_PATH=models/als_model.pkl
MATRIX_PATH=data/processed/user_item_matrix.npz
MAPPINGS_PATH=data/processed/mappings.json

# ── Secrets injectés par Secret Manager en production ────────────────────────
PINECONE_API_KEY=
OPENAI_API_KEY=
GOOGLE_API_KEY=
```

> **Sécurité :** Ne committez jamais `.env`. Les secrets en production sont gérés via **GCP Secret Manager** (voir `infra/secrets.tf`). Le `.gitignore` exclut déjà `.env`.

---

## Pipeline de données complet

Le pipeline transforme ~120 Go de dumps bruts en un modèle de recommandation prêt à servir. Voici chaque étape en détail.

### Étape 1 — Acquisition des données brutes

Les dumps ListenBrainz (historiques d'écoutes) et MusicBrainz (métadonnées musicales) sont trop volumineux pour être téléchargés directement sur un poste de travail. Le script lance une instance EC2 AWS éphémère qui télécharge directement vers GCS, sans transiter par votre machine.

```bash
# MusicBrainz uniquement (artistes, enregistrements, releases — ~20 Go)
uv run python scripts/download_to_s3_via_ec2.py 1

# ListenBrainz uniquement (historiques d'écoutes — ~120 Go)
uv run python scripts/download_to_s3_via_ec2.py 2

# Les deux en parallèle
uv run python scripts/download_to_s3_via_ec2.py 3
```

**Ce que fait le script :**
1. Crée une instance EC2 t3.small temporaire dans le compte AWS configuré
2. Y exécute via SSH les scripts de téléchargement
3. Les données sont uploadées directement de EC2 vers GCS (transfert rapide inter-cloud)
4. L'instance EC2 est automatiquement terminée à la fin

**Données produites :**
- `gs://brainz-raw-listenbrainz/` — Dumps Parquet/tar.zst des historiques d'écoute, partitionnés par date
- `gs://brainz-raw-musicbrainz/` — Tables JSON tar.xz : `artist`, `recording`, `release`, `release-group`

### Étape 2 — Parsing des écoutes ListenBrainz

```bash
uv run python scripts/parse_listens.py
```

Les dumps ListenBrainz sont au format zstandard compressé (`.tar.zst`). Ce script :
1. Décompresse les archives avec la bibliothèque `zstandard`
2. Parse les enregistrements JSON ligne par ligne
3. Extrait les triplets `(user_id, recording_mbid, listen_count)`
4. Filtre les données invalides (user_id manquant, recording_mbid invalide)
5. Produit des fichiers intermédiaires structurés

### Étape 3 — Construction de la matrice user-item

```bash
uv run python scripts/build_matrix.py
```

Ce script construit une matrice **sparse scipy CSR** (Compressed Sparse Row) :

- Chaque **ligne** = un utilisateur (indexé de 0 à N_users)
- Chaque **colonne** = un morceau/recording (indexé de 0 à N_items)
- Chaque **valeur non nulle** = nombre d'écoutes (confidence weight)

**Fichiers produits dans `gs://brainz-processed/processed/` :**

| Fichier | Description |
|---|---|
| `user_item_matrix.npz` | Matrice sparse scipy (format NPZ compressé) |
| `user_mapping.json` | `{index: "username"}` — mapping entier → nom d'utilisateur |
| `item_mapping.json` | `{index: "Artist - Track"}` — mapping entier → nom du morceau |
| `mappings.json` | Mapping consolidé utilisé par l'API |
| `track_dedup_map.json` | Catalogue dédupliqué pour la recherche |

### Étape 4 — Déduplication des morceaux

```bash
uv run python scripts/deduplicate_tracks.py
```

MusicBrainz contient de nombreux doublons (remixes, versions live, éditions différentes du même morceau). Ce script fusionne les enregistrements identiques pour améliorer la qualité des recommandations.

### Étape 5 — Agrégation et features

```bash
uv run python scripts/aggregate_data.py
```

Crée des statistiques agrégées par utilisateur et par morceau (popularité, diversité des genres...) utilisées par le catalogue et le service de couvertures.

### Pipeline Airflow (orchestration automatisée)

Le DAG `dags/listenbrainz_pipeline.py` orchestre toutes ces étapes automatiquement, selon un calendrier bimensuel (1er et 15 de chaque mois à 2h UTC) :

```
download_musicbrainz ──┐
                        ├──► create_dataproc_cluster
download_listenbrainz ─┘           │
                                    ├──► extract_musicbrainz (Spark)
                                    ├──► extract_listenbrainz (Spark)
                                    │           │
                                    │           ▼
                                    └──► process_data (Spark)
                                                │
                                                ▼
                                         generate_features (Spark)
                                                │
                                                ▼
                                        delete_dataproc_cluster
```

**Spark sur Dataproc** est utilisé pour les étapes de traitement massivement parallèles (extraction, jointures, agrégations) sur les 120+ Go de données brutes. Le cluster Dataproc (1 master n1-standard-4 + 2 workers) est créé à la volée et détruit après chaque run.

La VM GCE `vm-spotify` fait tourner Airflow en continu via systemd. L'interface web Airflow est accessible sur `http://<vm_ip>:8080`.

### Indexation des festivals (agent RAG)

```bash
# 1. Scraper offi.fr → JSON local
uv run python src/app/load_festival/get_festival.py

# 2. Créer les embeddings et indexer dans ChromaDB
uv run python src/app/load_festival/festival_to_vectorstore.py
```

**Ce que fait le scraper (`get_festival.py`) :**
1. Télécharge la page HTML d'`offi.fr` (liste des festivals 2026)
2. Parse les balises `<h3>` pour extraire nom et dates de chaque festival
3. Extrait lieu, artistes (via liens `/artiste/`) et URL de billetterie
4. Exporte en JSON (`data/festivals_2026.json`)
5. Upload vers `gs://projet-etude-m2/data_musique/festival/festivals_2026.json`

**Ce que fait l'indexeur (`festival_to_vectorstore.py`) :**
1. Charge le JSON local (`data/festivals_2026.json`)
2. Pour chaque festival, crée un texte représentatif = liste des artistes
3. Calcule les embeddings avec `paraphrase-multilingual-MiniLM-L12-v2` (384 dimensions)
4. Upsert dans la collection ChromaDB `festival` (métrique cosinus, index HNSW)

Cette indexation ne prend que quelques secondes et tourne entièrement hors ligne (pas d'API externe).

---

## Entraînement du modèle

```bash
uv run python src/train.py --matrix data/processed/user_item_matrix.npz
```

**Options disponibles :**

```bash
uv run python src/train.py \
  --matrix data/processed/user_item_matrix.npz \
  --user-mapping data/processed/user_mapping.json \
  --item-mapping data/processed/item_mapping.json \
  --output models/als_model.pkl \
  --factors 128 \
  --regularization 0.01 \
  --iterations 15 \
  --gpu  # optionnel, nécessite CUDA
```

**Ce que fait le script :**
1. Charge la matrice sparse depuis le fichier `.npz`
2. Initialise le modèle `ALSRecommender` avec les hyperparamètres choisis
3. Charge les mappings (index entier → noms lisibles)
4. Lance l'entraînement ALS avec barre de progression
5. Affiche les dimensions de la matrice et le temps d'entraînement
6. Sauvegarde le modèle complet (modèle + mappings) en pickle : `models/als_model.pkl`
7. Effectue un test rapide en affichant les recommandations pour l'utilisateur le plus actif

Le modèle sauvegardé inclut : l'objet `AlternatingLeastSquares` de la bibliothèque `implicit`, les mappings user/item, et les hyperparamètres. Il est ensuite uploadé vers `gs://brainz-processed/models/als_model.pkl` pour être consommé par l'API Cloud Run.

---

## Évaluation du modèle

```bash
uv run python src/evaluate.py \
  --model models/als_model.pkl \
  --train data/processed/train_matrix.npz \
  --test data/processed/test_matrix.npz \
  --k 5 10 20 \
  --sample 1000 \
  --output models/evaluation_results.json
```

L'évaluation utilise un split **temporel** : les écoutes récentes constituent le test set (ground truth), les écoutes plus anciennes constituent le train set. Pour chaque utilisateur du test set, on génère des recommandations et on mesure leur pertinence.

### Métriques calculées

**Precision@K** — Quelle fraction des K recommandations sont pertinentes ?

```
Precision@K = |recommandés ∩ pertinents| / K
```

*Exemple : sur 10 recommandations, 3 sont des morceaux que l'utilisateur a réellement écouté → Precision@10 = 0.30*

**Recall@K** — Quelle fraction des morceaux pertinents ont été trouvés ?

```
Recall@K = |recommandés ∩ pertinents| / |pertinents|
```

*Exemple : l'utilisateur a écouté 20 nouveaux morceaux, et 3 d'entre eux sont dans nos 10 recommandations → Recall@10 = 0.15*

**NDCG@K** (Normalized Discounted Cumulative Gain) — Mesure la **qualité du ranking** : un item pertinent en position 1 vaut plus qu'en position 10.

```
DCG@K = Σ 1 / log2(position + 1)   pour chaque item pertinent dans le top-K
NDCG@K = DCG@K / IDCG@K            (normalisé par le ranking idéal possible)
```

**MAP** (Mean Average Precision) — Moyenne de la Precision pour chaque position où une recommandation est pertinente.

**Coverage** — Proportion du catalogue total recommandée au moins une fois :

```
Coverage = |items recommandés au moins 1 fois| / |items total|
```

*Indique si le modèle recommande diversement ou se concentre sur quelques hits.*

**Novelty** — Mesure à quel point les recommandations sont peu populaires (non-mainstream) :

```
Novelty = -log2(popularité moyenne des items recommandés)
```

*Un score élevé signifie que le modèle recommande des morceaux peu connus — potentiellement plus intéressants pour l'utilisateur.*

---

## API de recommandation — référence complète

`music-api` est une FastAPI containerisée déployée sur Cloud Run. Le modèle ALS est chargé depuis GCS au démarrage du conteneur, avec fallback sur les fichiers locaux. La documentation interactive est disponible sur `/docs` (Swagger UI) et `/redoc`.

### Informations générales

| Méthode | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Page d'accueil — infos sur l'API |
| `GET` | `/health` | Healthcheck (statut, modèle chargé, N users/items) |
| `GET` | `/stats` | Statistiques du modèle (dimensions, sparsité, hyperparamètres) |

### Recommandations

| Méthode | Endpoint | Description |
|---|---|---|
| `GET` | `/recommend/{user_id}` | Recommandations personnalisées ALS |
| `GET` | `/similar/{track_id}` | Morceaux similaires (similarité item-item) |
| `GET` | `/history/{user_id}` | Historique d'écoute d'un utilisateur |

**`GET /recommend/{user_id}`**

```bash
curl "https://music-api-xxxx.a.run.app/recommend/12345?n=10&filter_liked=true"
```

Paramètres :
- `n` (int, 1–100, défaut 10) : nombre de recommandations
- `filter_liked` (bool, défaut true) : exclure les morceaux déjà écoutés

```json
{
  "user_id": "12345",
  "recommendations": [
    {"track": "Karma Police", "artist": "Radiohead", "score": 0.9412, "item_id": 4821},
    {"track": "Glory Box",    "artist": "Portishead", "score": 0.9104, "item_id": 2934}
  ]
}
```

**`GET /similar/{track_id}`**

```bash
curl "https://music-api-xxxx.a.run.app/similar/4821?n=5"
```

Paramètres :
- `n` (int, 1–50, défaut 10) : nombre de morceaux similaires

### Catalogue

| Méthode | Endpoint | Description |
|---|---|---|
| `GET` | `/catalog/tracks` | Liste paginée de tous les morceaux |
| `GET` | `/catalog/search` | Recherche full-text par artiste ou titre |
| `GET` | `/catalog/artist` | Tous les morceaux d'un artiste |
| `GET` | `/catalog/artists` | Recherche d'artistes avec comptage |
| `GET` | `/catalog/cover` | URL de la pochette d'album (iTunes API, mise en cache) |

**`GET /catalog/search`**

```bash
curl "https://music-api-xxxx.a.run.app/catalog/search?q=radiohead&limit=10&sort=popularity"
```

Paramètres :
- `q` (string, requis) : texte à rechercher (artiste ou titre)
- `limit` (int, 1–200, défaut 100) : nombre de résultats
- `sort` (string, défaut "relevance") : `relevance` ou `popularity`

La recherche utilise **RapidFuzz** pour la correspondance floue (tolère les fautes de frappe).

### Bibliothèque personnelle

La bibliothèque persiste en JSON (`data/library.json`). Elle permet à chaque utilisateur de gérer ses likes et ses playlists.

| Méthode | Endpoint | Description |
|---|---|---|
| `POST` | `/library/{user_id}/likes` | Liker un morceau |
| `DELETE` | `/library/{user_id}/likes/{item_id}` | Retirer un like |
| `GET` | `/library/{user_id}/likes` | Liste des morceaux likés |
| `GET` | `/library/{user_id}/likes/{item_id}` | Vérifier si un morceau est liké |
| `POST` | `/library/{user_id}/playlists` | Créer une playlist |
| `GET` | `/library/{user_id}/playlists` | Lister les playlists |
| `GET` | `/library/{user_id}/playlists/{playlist_id}` | Détail d'une playlist |
| `PATCH` | `/library/{user_id}/playlists/{playlist_id}` | Renommer une playlist |
| `DELETE` | `/library/{user_id}/playlists/{playlist_id}` | Supprimer une playlist |
| `POST` | `/library/{user_id}/playlists/{playlist_id}/tracks` | Ajouter un morceau |
| `DELETE` | `/library/{user_id}/playlists/{playlist_id}/tracks/{item_id}` | Retirer un morceau |

### Administration

| Méthode | Endpoint | Description |
|---|---|---|
| `POST` | `/reload` | Lance le rechargement du modèle en arrière-plan |
| `GET` | `/reload/status` | État du rechargement en cours |

Le rechargement (`POST /reload`) est asynchrone : il retourne immédiatement un `{"status": "loading"}` et charge le modèle en background task. Utile après un réentraînement sans redémarrer le conteneur.

### Agent Festival

| Méthode | Endpoint | Description |
|---|---|---|
| `POST` | `/festival/chat` | Poser une question à l'agent RAG |
| `DELETE` | `/festival/sessions/{session_id}` | Supprimer une session de conversation |

**`POST /festival/chat`**

```bash
curl -X POST "https://music-api-xxxx.a.run.app/festival/chat" \
  -H "Content-Type: application/json" \
  -d '{"question": "Quels festivals jazz en juillet 2026 ?", "session_id": "session-abc"}'
```

```json
{
  "answer": "J'ai trouvé plusieurs festivals avec une programmation jazz pour juillet 2026...",
  "session_id": "session-abc"
}
```

Si `session_id` n'est pas fourni, un UUID est généré automatiquement. L'historique de la session est maintenu en mémoire (perdu au redémarrage du service).

### Frontend statique

Si le dossier `src/static/` existe, l'interface web (player) est servie sur `/player` et `/app`. Cette interface JavaScript permet de découvrir le catalogue, d'obtenir des recommandations et de discuter avec l'agent festival.

---

## Agent RAG Festivals

### Architecture détaillée

```
Question utilisateur
    │
    ▼  POST /festival/chat
[main.py]  ──── appelle ────► [agent.py]
                                    │
                                    ▼ LangChain AgentExecutor
                               [Ollama llama3.2:3b]
                                    │
                          ┌─────────┴─────────┐
                          │  Tool calling      │
                          ▼                   │
                    [tools.py]               │
                    search_festival_store    │
                          │                  │
                          ▼                  │
                    [ChromaDB]               │
                    collection "festival"    │
                          │                  │
                          ▼                  │
                    Top-5 festivals          │
                    + métadonnées            │
                          │                  │
                          └─────────►────────┘
                                    │
                                    ▼
                             Réponse en français
                             structurée avec nom,
                             dates, lieu, artistes
```

### Indexation et mise à jour

L'index ChromaDB est mis à jour automatiquement chaque lundi via le workflow GitHub Actions `festival_update.yml` :

1. Scraping de `offi.fr` pour les données les plus récentes
2. Validation du JSON produit (abandon si vide)
3. Commit des changements dans le dépôt
4. Ré-indexation ChromaDB si un runner self-hosted est disponible

Pour une mise à jour manuelle :

```bash
uv run python src/app/load_festival/get_festival.py
uv run python src/app/load_festival/festival_to_vectorstore.py
```

### Modèle d'embeddings

Le modèle `paraphrase-multilingual-MiniLM-L12-v2` (sentence-transformers) :
- **384 dimensions** de vecteurs
- Fonctionne entièrement **hors ligne** — pas d'appel API
- **Multilingue** — comprend aussi bien "jazz" que "musique électronique" ou "rock"
- **Petit et rapide** — ~120 Mo, inférence en moins de 50ms sur CPU
- Téléchargé automatiquement par sentence-transformers au premier lancement

### Changer de LLM

L'agent utilise Ollama par défaut (local, gratuit). Pour utiliser un autre modèle :

```bash
# Via variable d'environnement
OLLAMA_MODEL=mistral:7b uv run uvicorn src.api.main:app --reload

# Ou dans .env
OLLAMA_MODEL=llama3.1:8b
```

---

## Dashboard de monitoring

`music-dashboard` est une interface web FastAPI + JavaScript permettant de suivre en temps réel :

- **Buckets GCS** : taille, nombre de fichiers, date de dernière modification
- **Services Cloud Run** : état, nombre d'instances, URL
- **VM GCE** : instances actives et leur statut
- **Logs pipeline** : sortie en streaming du pipeline de données

```bash
# Lancer le dashboard en local
uv run uvicorn dashboard:app --reload --port 8080
```

---

## Infrastructure GCP avec Terraform

Tout le provisionnement GCP est décrit sous forme de code dans `infra/`. Un seul `terraform apply` crée et configure l'ensemble de l'infrastructure.

### Ressources créées

**GCS Buckets (`infra/storage.tf`)**
- `brainz-raw-listenbrainz` — Dumps bruts ListenBrainz
- `brainz-raw-musicbrainz` — Dumps bruts MusicBrainz
- `brainz-processed` — Données traitées (matrices, modèles, catalogue)
- `projet-etude-m2` — Données festivals et bucket Terraform state

**GCE VM (`infra/vm.tf`)**
- Instance `vm-spotify` (Debian 12, e2-medium, 10 Go SSD)
- Startup script systemd qui installe `uv`, clone le repo, démarre Airflow et l'API locale
- IP statique externe

**Cloud Run (`infra/cloudrun.tf`)**
- Service `music-api` : 2 CPU, 8 Gi RAM, scale-to-zero (0 à 3 instances), port 8000
- Service `music-dashboard` : 1 CPU, 512 Mi RAM, scale-to-zero (0 à 2 instances), port 8080
- Les deux services sont publics (invoker allUsers)
- Variables d'environnement injectées via Secret Manager

**Artifact Registry (`infra/cloudrun.tf`)**
- Dépôt Docker `music-api` dans la région configurée
- Stocke les images Docker buildées par le CI/CD

**Réseau (`infra/network.tf`)**
- IP statique pour la VM
- Règles firewall : SSH restreint à votre IP, port 8080 (Airflow), port 8000 (API)

**Secret Manager (`infra/secrets.tf`)**
- Références aux secrets `PINECONE_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`
- Accès accordé au service account `cloudrun-api` via IAM

**Service Account (`infra/cloudrun.tf`)**
- `cloudrun-api` avec rôle `roles/storage.objectViewer` (lecture GCS) et accès Secret Manager

### Déploiement initial

**Étape 1 — Préparer GCP**

```bash
gcloud auth application-default login
gcloud config set project <YOUR_PROJECT_ID>

gcloud services enable \
  compute.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  dataproc.googleapis.com
```

**Étape 2 — Créer les secrets dans Secret Manager** (une seule fois, avant le premier `apply`)

```bash
printf "votre-cle-pinecone" | gcloud secrets create PINECONE_API_KEY \
  --data-file=- --replication-policy=automatic

printf "sk-votre-cle-openai" | gcloud secrets create OPENAI_API_KEY \
  --data-file=- --replication-policy=automatic

printf "AIza-votre-cle-google" | gcloud secrets create GOOGLE_API_KEY \
  --data-file=- --replication-policy=automatic
```

**Étape 3 — Créer le bucket Terraform state** (bootstrap one-time)

```bash
gsutil mb -l US-CENTRAL1 gs://tfstate-<YOUR_PROJECT_ID>
gsutil versioning set on gs://tfstate-<YOUR_PROJECT_ID>
```

Mettre à jour le backend dans `infra/main.tf` :

```hcl
backend "gcs" {
  bucket = "tfstate-<YOUR_PROJECT_ID>"
  prefix = "vm-spotify"
}
```

**Étape 4 — Configurer les variables Terraform**

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

**Étape 5 — Appliquer**

```bash
cd infra
terraform init
terraform plan    # vérifier les changements prévus
terraform apply   # confirmer avec "yes"
```

**Outputs après `apply` :**

```
vm_external_ip  = "x.x.x.x"
ssh_command     = "ssh mon-utilisateur@x.x.x.x"
api_url         = "https://music-api-xxxx-ew.a.run.app"
dashboard_url   = "https://music-dashboard-xxxx-ew.a.run.app"
```

**Étape 6 — Vérifier la VM**

```bash
ssh mon-utilisateur@<vm_external_ip>
sudo systemctl status airflow fastapi
# Airflow UI : http://<vm_external_ip>:8080
```

### Arrêter/démarrer la VM pour économiser

```bash
# Arrêter (stop le compteur de facturation compute)
gcloud compute instances stop vm-spotify --zone us-central1-b

# Redémarrer
gcloud compute instances start vm-spotify --zone us-central1-b
```

---

## CI/CD — GitHub Actions

Quatre workflows automatisés gèrent le cycle de vie complet du projet.

### `ci.yml` — Qualité du code

Déclenché sur chaque push et pull request vers `main`.

```
push/PR → ruff lint (E, F, W) → pyright type check → vérification imports
```

- **ruff** : lint rapide (en Rust), vérifie le style et les erreurs courantes
- **pyright** : vérification de types (non bloquant — avertissements uniquement)
- **Import checks** : vérifie que `src/app/` et `src/` s'importent correctement

### `deploy.yml` — Build & Deploy Cloud Run

Déclenché sur push vers `main` uniquement si les fichiers `src/`, `Dockerfile*`, `dashboard.py`, `pyproject.toml` ou `uv.lock` sont modifiés.

```
push main → auth GCP (Workload Identity) → docker build music-api
                                         → docker build music-dashboard
                                         → push vers Artifact Registry
                                         → deploy music-api sur Cloud Run
                                         → deploy music-dashboard sur Cloud Run
```

L'authentification GCP utilise le **Workload Identity Federation** — aucune clé JSON de service account n'est stockée dans GitHub Secrets. C'est la méthode recommandée par GCP pour la sécurité.

### `terraform.yml` — Infrastructure as Code

Déclenché sur push/PR vers `main` si `infra/` est modifié.

- **Sur PR** : `terraform plan` avec le résultat posté en commentaire sur la PR (pour review humaine avant merge)
- **Sur push main** : `terraform apply` automatique

### `festival_update.yml` — Mise à jour hebdomadaire des festivals

Planifié tous les **lundis à 6h UTC** + déclenchement manuel via `workflow_dispatch`.

```
lundi 6h UTC → scraping offi.fr → validation JSON (exit si vide)
             → commit dans le repo
             → ré-indexation ChromaDB (si runner self-hosted disponible)
```

### Secrets GitHub requis

| Secret | Description |
|---|---|
| `GCP_PROJECT_ID` | ID du projet GCP (ex: `projetetude-497218`) |
| `GCP_PROJECT_NUMBER` | Numéro du projet GCP (ex: `126997656473`) |

Le service account GitHub Actions `terraform-ci@<PROJECT_ID>.iam.gserviceaccount.com` doit avoir les rôles : `Editor` + `roles/run.admin` + `roles/iam.serviceAccountUser`.

---

## Docker et déploiement local

### `Dockerfile` — Image music-api

```bash
# Build
docker build -t music-api .

# Run avec fallback sur les fichiers locaux
docker run -p 8000:8000 \
  -v $(pwd)/models:/app/models \
  -v $(pwd)/data:/app/data \
  music-api
```

### `Dockerfile.dashboard` — Image music-dashboard

```bash
docker build -f Dockerfile.dashboard -t music-dashboard .
docker run -p 8080:8080 music-dashboard
```

### `docker-compose.yml` — Stack complète de développement

```bash
# Démarrer tous les services en local
docker compose up

# Uniquement l'API
docker compose up music-api
```

---

## Structure du projet

```
Recommandation_musique/
│
├── infra/                              # Terraform — infrastructure GCP complète
│   ├── main.tf                         # Provider Google + backend GCS
│   ├── variables.tf                    # Déclaration des variables Terraform
│   ├── terraform.tfvars.example        # Template de configuration (à copier)
│   ├── cloudrun.tf                     # Services Cloud Run + Artifact Registry
│   ├── vm.tf                           # GCE VM avec startup script systemd
│   ├── network.tf                      # IP statique + règles firewall
│   ├── storage.tf                      # Buckets GCS (raw, processed, festival, tfstate)
│   ├── secrets.tf                      # Accès Secret Manager pour Cloud Run
│   └── outputs.tf                      # Outputs : IP VM, commandes SSH, URLs
│
├── src/
│   ├── app/                            # Agent RAG Festivals
│   │   ├── agent/
│   │   │   ├── agent.py                # AgentExecutor LangChain (Ollama llama3.2:3b)
│   │   │   ├── tools.py                # Outil search_festival_store (requête ChromaDB)
│   │   │   └── prompt.py               # Prompt système de l'agent (instructions + exemples)
│   │   ├── load_festival/
│   │   │   ├── get_festival.py         # Scraping offi.fr → JSON local + upload GCS
│   │   │   └── festival_to_vectorstore.py  # JSON → embeddings MiniLM → ChromaDB
│   │   └── api.py                      # FastAPI standalone : POST /chat (usage direct)
│   │
│   ├── api/                            # API de recommandation (service Cloud Run)
│   │   ├── main.py                     # FastAPI : tous les endpoints
│   │   ├── recommender.py              # Chargement modèle ALS depuis GCS + inférence async
│   │   ├── catalog.py                  # Service catalogue (recherche RapidFuzz, pagination)
│   │   ├── library.py                  # Bibliothèque persistante (likes + playlists JSON)
│   │   └── cover_service.py            # Pochettes d'album via iTunes Search API (avec cache)
│   │
│   ├── models/
│   │   └── als_model.py                # Classe ALSRecommender (wrapper implicit library)
│   │                                   # fit(), recommend(), similar_items(), save(), load()
│   ├── train.py                        # Script d'entraînement ALS (CLI avec argparse)
│   ├── evaluate.py                     # Évaluation : Precision@K, Recall@K, NDCG@K, MAP,
│   │                                   #              Coverage, Novelty
│   ├── serve.py                        # Serving CLI (mode interactif)
│   └── static/
│       └── index.html                  # Frontend JavaScript (player + agent festival)
│
├── scripts/
│   ├── download_to_s3_via_ec2.py       # Lance EC2 AWS pour télécharger dumps → GCS
│   ├── download_to_gcs_via_gce.py      # Alternative : téléchargement via GCE
│   ├── download_listenbrainz.py        # Téléchargement direct ListenBrainz
│   ├── download_musicbrainz.py         # Téléchargement direct MusicBrainz
│   ├── parse_listens.py                # Parsing des dumps bruts (zstandard → JSON structuré)
│   ├── build_matrix.py                 # Construction matrice user-item sparse scipy CSR
│   ├── deduplicate_tracks.py           # Déduplication des enregistrements MusicBrainz
│   ├── aggregate_data.py               # Agrégations (popularité, statistiques utilisateurs)
│   ├── filter_listenbrainz_2025.py     # Filtrage des écoutes 2025 uniquement
│   ├── download_model.py               # Téléchargement du modèle depuis GCS
│   ├── upload_to_gcs.py                # Upload fichiers locaux vers GCS
│   └── update_festival_db.py           # Scraping + ré-indexation ChromaDB (utilisé en CI)
│
├── dags/
│   └── listenbrainz_pipeline.py        # DAG Airflow : ingestion → Spark (Dataproc) → modèle
│                                       # Planification : 1er et 15 du mois à 2h UTC
│
├── dashboard.py                        # Dashboard FastAPI + WebSocket (monitoring GCP)
├── templates/
│   └── dashboard.html                  # Template HTML Jinja2 du dashboard
│
├── config/
│   ├── download_instance.json          # Config EC2/GCE pour le téléchargement
│   ├── gcp_config.json                 # Config GCP (projet, région, buckets)
│   └── load_env.py                     # Chargement des variables d'environnement
│
├── docs/
│   ├── GUIDE_AWS.md                    # Guide configuration AWS
│   ├── GUIDE_EC2.md                    # Guide utilisation EC2
│   ├── ORGANIZATION.md                 # Organisation du projet
│   ├── algorithmes_recommandation.md   # Explication détaillée des algorithmes
│   └── plan_implementation.md          # Plan d'implémentation initial
│
├── data/
│   ├── festivals_2026.json             # Données scraped des festivals (versionnées)
│   ├── library.json                    # Bibliothèque utilisateurs (likes + playlists)
│   └── chroma/                         # Base vectorielle ChromaDB (SQLite + HNSW index)
│
├── Dockerfile                          # Image Docker music-api (python:3.11-slim + uv)
├── Dockerfile.dashboard                # Image Docker music-dashboard
├── docker-compose.yml                  # Stack de développement local
├── pyproject.toml                      # Dépendances Python (uv), groupes optionnels airflow/spark
├── uv.lock                             # Lockfile uv (reproductibilité exacte)
├── .env.example                        # Template variables d'environnement
└── .github/workflows/
    ├── ci.yml                          # Lint + import checks (chaque push/PR)
    ├── deploy.yml                      # Build Docker + déploiement Cloud Run (push main)
    ├── terraform.yml                   # Plan/Apply Terraform (si infra/ modifié)
    └── festival_update.yml             # Scraping hebdomadaire festivals (lundi 6h UTC)
```

---

## Coûts GCP estimés

| Ressource | Configuration | Coût estimé |
|---|---|---|
| GCE VM e2-medium | 0.033 $/h — à arrêter quand inutilisée | ~24 $/mois (si 24/7) |
| Cloud Run music-api | 2 CPU, 8 Gi, scale-to-zero | Pay-per-request |
| Cloud Run music-dashboard | 1 CPU, 512 Mi, scale-to-zero | Quasi gratuit |
| GCS brainz-raw (~140 Go) | ~0.02 $/Go/mois | ~2.80 $/mois |
| GCS brainz-processed (~5 Go) | ~0.02 $/Go/mois | ~0.10 $/mois |
| Artifact Registry | Stockage images Docker | ~0.10 $/mois |
| Dataproc (pipeline) | 3 × n1-standard-4, ~2h/run | ~1.50 $/run (2× par mois) |
| Ollama | LLM local — aucun coût API | 0 $ |
| sentence-transformers | Embeddings locaux — aucun coût API | 0 $ |
| ChromaDB | Vector store local — aucun coût | 0 $ |

**Total estimé (usage normal) : ~30-35 $/mois**

> **Conseil coût :** Arrêter la VM GCE entre les runs de pipeline réduit la facture à ~8-10 $/mois :
> ```bash
> gcloud compute instances stop vm-spotify --zone us-central1-b
> ```

---

## Licence

MIT — voir `LICENSE`.
