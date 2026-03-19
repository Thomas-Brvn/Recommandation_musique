# Plan d'Implémentation - Système de Recommandation Musicale

## Approche choisie : Collaborative Filtering (ALS)

Nous commençons par **Matrix Factorization avec ALS (Alternating Least Squares)**, la même base utilisée par Spotify pour Discover Weekly.

### Pourquoi ce choix ?
- ✅ Adapté à nos 85M d'écoutes
- ✅ Pas besoin de features audio
- ✅ Bibliothèque `implicit` optimisée (C++/CUDA)
- ✅ Résultats rapides pour valider le concept
- ✅ Base solide pour évoluer vers un système hybride

---

## Architecture du Système

```
┌─────────────────────────────────────────────────────────────────────┐
│                         S3 : brainz-data                            │
│  ┌─────────────────────────┐    ┌─────────────────────────────┐     │
│  │  raw/listenbrainz/      │    │  raw/musicbrainz/           │     │
│  │  incrementals/*.tar.zst │    │  artist.tar.xz              │     │
│  │  (30 dumps, 3.6 GB)     │    │  release.tar.xz             │     │
│  └───────────┬─────────────┘    └─────────────┬───────────────┘     │
└──────────────┼─────────────────────────────────┼────────────────────┘
               │                                 │
               ▼                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ÉTAPE 1 : Data Pipeline                         │
│  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐       │
│  │  Extraction    │──▶│  Nettoyage     │──▶│  Aggregation   │       │
│  │  (zstd + tar)  │   │  (dedup, NaN)  │   │  (user-item)   │       │
│  └────────────────┘   └────────────────┘   └────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ÉTAPE 2 : Preprocessing                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │  Matrice Sparse (CSR)                                      │     │
│  │  ┌─────────────────────────────────────────────────────┐   │     │
│  │  │         │ track_1 │ track_2 │ track_3 │ ... │       │   │     │
│  │  │ user_1  │    5    │    0    │    2    │     │       │   │     │
│  │  │ user_2  │    0    │    3    │    0    │     │       │   │     │
│  │  │ user_3  │    1    │    1    │    4    │     │       │   │     │
│  │  └─────────────────────────────────────────────────────┘   │     │
│  │  (nombre d'écoutes par user/track)                         │     │
│  └────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ÉTAPE 3 : Modèle ALS                            │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                Matrix Factorization                        │     │
│  │                                                            │     │
│  │    R ≈ U × V^T                                             │     │
│  │                                                            │     │
│  │    R = matrice user-item (sparse)                          │     │
│  │    U = matrice users (n_users × k factors)                 │     │
│  │    V = matrice items (n_items × k factors)                 │     │
│  │                                                            │     │
│  │    Hyperparamètres:                                        │     │
│  │    - factors: 128 (dimensions latentes)                    │     │
│  │    - regularization: 0.01                                  │     │
│  │    - iterations: 15                                        │     │
│  └────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ÉTAPE 4 : Évaluation                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │ Precision@K  │  │  Recall@K    │  │    NDCG      │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
│  ┌──────────────┐  ┌──────────────┐                                 │
│  │   Coverage   │  │   Novelty    │                                 │
│  └──────────────┘  └──────────────┘                                 │
└──────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ÉTAPE 5 : API / Serving                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │  GET /recommend/{user_id}?n=10                             │     │
│  │                                                            │     │
│  │  Response:                                                 │     │
│  │  [                                                         │     │
│  │    {"track": "Song A", "artist": "Artist X", "score": 0.9},│     │
│  │    {"track": "Song B", "artist": "Artist Y", "score": 0.8} │     │
│  │  ]                                                         │     │
│  └────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Étapes détaillées

### Étape 1 : Data Pipeline
**Objectif** : Extraire et consolider les 30 dumps en un dataset unifié

```
scripts/
├── extract_incrementals.py    # Décompresse les .tar.zst
├── parse_listens.py           # Parse les JSON lines
└── aggregate_data.py          # Crée le dataset final
```

**Output** : `data/processed/listens.parquet`
- Colonnes : `user_id`, `track_id`, `artist_id`, `timestamp`, `play_count`

---

### Étape 2 : Preprocessing
**Objectif** : Créer la matrice sparse user-item

```
scripts/
└── build_matrix.py
```

**Filtres appliqués** :
- Users avec < 5 écoutes → supprimés
- Tracks avec < 3 écoutes → supprimés
- Normalisation : log(1 + play_count)

**Output** :
- `data/processed/user_item_matrix.npz` (sparse CSR)
- `data/processed/user_mapping.json`
- `data/processed/item_mapping.json`

---

### Étape 3 : Entraînement ALS
**Objectif** : Entraîner le modèle de factorisation

```
src/
├── models/
│   └── als_model.py
└── train.py
```

**Librairie** : `implicit`
```python
from implicit.als import AlternatingLeastSquares

model = AlternatingLeastSquares(
    factors=128,
    regularization=0.01,
    iterations=15,
    use_gpu=False  # True si CUDA disponible
)
model.fit(user_item_matrix)
```

**Output** : `models/als_model.pkl`

---

### Étape 4 : Évaluation
**Objectif** : Mesurer la qualité des recommandations

```
src/
└── evaluate.py
```

**Métriques** :
| Métrique | Description | Cible |
|----------|-------------|-------|
| Precision@10 | % recommandations pertinentes | > 0.1 |
| Recall@10 | % items pertinents recommandés | > 0.05 |
| NDCG@10 | Qualité du ranking | > 0.15 |
| Coverage | % items recommandables | > 0.3 |

**Méthode** : Train/Test split temporel (dernière semaine = test)

---

### Étape 5 : API de Serving
**Objectif** : Exposer les recommandations via une API REST

```
src/
├── api/
│   ├── main.py          # FastAPI app
│   └── recommender.py   # Logique de recommandation
└── serve.py
```

**Endpoints** :
- `GET /recommend/{user_id}` : Top N recommandations
- `GET /similar/{track_id}` : Tracks similaires
- `GET /health` : Status du service

---

## Structure du projet

```
Recommandation_musique/
├── config/
│   └── aws_config.json
├── data/
│   ├── raw/                    # Données brutes (local cache)
│   └── processed/              # Données transformées
├── docs/
│   ├── algorithmes_recommandation.md
│   └── plan_implementation.md
├── models/                     # Modèles entraînés
├── notebooks/                  # Exploration et prototypage
├── scripts/                    # Scripts de data pipeline
│   ├── extract_incrementals.py
│   ├── parse_listens.py
│   ├── aggregate_data.py
│   └── build_matrix.py
├── src/                        # Code source principal
│   ├── models/
│   │   └── als_model.py
│   ├── api/
│   │   ├── main.py
│   │   └── recommender.py
│   ├── train.py
│   ├── evaluate.py
│   └── serve.py
├── tests/                      # Tests unitaires
├── requirements.txt
└── README.md
```

---

## Dépendances

```txt
# Data processing
pandas>=2.0
pyarrow>=14.0
scipy>=1.11
zstandard>=0.22

# ML
implicit>=0.7
numpy>=1.24
scikit-learn>=1.3

# API
fastapi>=0.104
uvicorn>=0.24

# AWS
boto3>=1.33

# Utils
tqdm>=4.66
python-dotenv>=1.0
```

---

## Timeline estimée

| Étape | Description | Priorité |
|-------|-------------|----------|
| 1 | Data Pipeline | 🔴 Haute |
| 2 | Preprocessing | 🔴 Haute |
| 3 | Modèle ALS | 🔴 Haute |
| 4 | Évaluation | 🟡 Moyenne |
| 5 | API Serving | 🟡 Moyenne |

---

## Évolutions futures (après MVP)

### Phase 2 : Modèle Hybride (LightFM)
- Ajouter les features artistes depuis MusicBrainz
- Combiner CF + Content-Based
- Améliorer le cold start

### Phase 3 : Séquentiel (SASRec)
- Prendre en compte l'ordre d'écoute
- Recommandations contextuelles
- Nécessite GPU

### Phase 4 : Enrichissement
- Intégrer les genres/tags via MusicBrainz API
- Ajouter les features audio via Spotify API (si accès)
- Clustering d'utilisateurs par goûts

---

## Annexe : Autres méthodes considérées

### Content-Based Filtering
- **Principe** : Recommander basé sur les attributs des morceaux
- **Avantages** : Pas de cold start utilisateur, expliquabilité
- **Inconvénients** : Nécessite métadonnées riches, filter bubble
- **Quand l'utiliser** : En complément du CF dans un système hybride

### Graph Neural Networks (LightGCN)
- **Principe** : Modéliser user-item-artist comme un graphe
- **Avantages** : Capture relations multi-hop, state-of-the-art
- **Inconvénients** : Complexe, ressources importantes
- **Quand l'utiliser** : Pour pousser les performances après validation du MVP

### Session-Based (SASRec/BERT4Rec)
- **Principe** : Prédire le prochain morceau via Transformers
- **Avantages** : Contextuel, temps réel, très performant
- **Inconvénients** : GPU requis, plus complexe à déployer
- **Quand l'utiliser** : Pour un système de recommandation en temps réel

### Bandits Contextuels
- **Principe** : Exploration vs exploitation en temps réel
- **Avantages** : S'adapte en continu, optimise l'engagement
- **Inconvénients** : Nécessite système en production avec feedback
- **Quand l'utiliser** : Une fois l'API déployée avec des vrais utilisateurs