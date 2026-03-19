# Algorithmes de Recommandation Musicale

## Données disponibles

- **30 dumps incrémentaux** (16 déc 2025 - 14 jan 2026)
- **~85 millions d'écoutes**
- **Champs disponibles** : `user_id`, `timestamp`, `track_name`, `artist_name`, `release_name`, `artist_mbids`, `recording_mbid`, `duration_ms`

---

## Option 1 : Filtrage Collaboratif (Collaborative Filtering)

### Principe
Recommander des morceaux basés sur les comportements d'utilisateurs similaires. "Les utilisateurs qui ont écouté X ont aussi écouté Y."

### Variantes

#### 1.1 User-Based CF
- Trouver des utilisateurs avec des goûts similaires
- Recommander ce qu'ils écoutent et que tu n'as pas encore écouté

#### 1.2 Item-Based CF
- Trouver des morceaux souvent écoutés ensemble
- Plus stable et scalable que User-Based

#### 1.3 Matrix Factorization (ALS, SVD)
- Décomposer la matrice utilisateur-morceau en facteurs latents
- Très performant, utilisé par Spotify/Netflix

### Avantages
- Pas besoin de métadonnées sur les morceaux
- Découvre des patterns non évidents
- Bien adapté à nos données (beaucoup d'interactions)

### Inconvénients
- Cold start problem (nouveaux utilisateurs/morceaux)
- Tendance à recommander du populaire
- Nécessite beaucoup de RAM pour les grandes matrices

### Implémentation
```python
# Libraries : Surprise, implicit, LightFM
from implicit.als import AlternatingLeastSquares
model = AlternatingLeastSquares(factors=128)
```

---

## Option 2 : Recommandation basée sur le contenu (Content-Based)

### Principe
Recommander des morceaux similaires à ceux que l'utilisateur aime, basé sur les attributs des morceaux.

### Features utilisables
- **Artiste** : genre, popularité
- **Morceau** : durée, année
- **Enrichissement possible** : via l'API Spotify (tempo, énergie, danceability) ou MusicBrainz

### Avantages
- Pas de cold start pour les nouveaux utilisateurs
- Expliquabilité ("car vous aimez le rock")
- Fonctionne avec peu de données utilisateur

### Inconvénients
- Nécessite des métadonnées riches
- Tendance à sur-spécialiser (filter bubble)
- Ne découvre pas de nouveaux genres

### Implémentation
```python
# TF-IDF sur artistes/genres + similarité cosinus
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
```

---

## Option 3 : Modèle Hybride

### Principe
Combiner filtrage collaboratif + content-based pour bénéficier des avantages des deux.

### Architecture
```
┌─────────────────┐     ┌─────────────────┐
│  Collaborative  │     │  Content-Based  │
│    Filtering    │     │    Features     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
              ┌──────▼──────┐
              │   Fusion    │
              │  (weighted) │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │ Recommandations │
              └─────────────┘
```

### Avantages
- Meilleure couverture
- Résout partiellement le cold start
- Plus robuste

### Inconvénients
- Plus complexe à implémenter et tuner
- Temps d'entraînement plus long

### Implémentation
```python
# LightFM combine les deux approches
from lightfm import LightFM
model = LightFM(loss='warp')  # ou 'bpr'
```

---

## Option 4 : Séquentiel / Session-Based

### Principe
Prédire le prochain morceau basé sur la séquence d'écoute actuelle. Tient compte de l'ordre temporel.

### Modèles
- **GRU4Rec** : RNN pour les sessions
- **SASRec** : Self-Attention (Transformer)
- **BERT4Rec** : Bidirectionnel

### Avantages
- Capture les tendances d'écoute en temps réel
- Contextuel (moment de la journée, humeur)
- State-of-the-art sur beaucoup de benchmarks

### Inconvénients
- Nécessite des sessions bien définies
- Plus complexe à entraîner (GPU recommandé)
- Données temporelles importantes

### Implémentation
```python
# RecBole framework
from recbole.model.sequential_recommender import SASRec
```

---

## Option 5 : Graph Neural Networks (GNN)

### Principe
Modéliser les relations utilisateur-morceau-artiste comme un graphe et apprendre des embeddings.

### Architecture
```
User ──écoute──► Track ──par──► Artist
  │                │              │
  └───similaire────┴──────────────┘
```

### Modèles
- **LightGCN** : simplifié, très performant
- **PinSage** : utilisé par Pinterest
- **GraphSAGE** : inductif

### Avantages
- Capture les relations multi-hop
- Utilise la structure du graphe
- Gère bien le cold start avec les features

### Inconvénients
- Complexe à implémenter
- Nécessite plus de ressources
- Tuning difficile

### Implémentation
```python
# PyTorch Geometric ou DGL
from torch_geometric.nn import LightGCN
```

---

## Recommandation

### Pour commencer (MVP)
**Option 1.3 : Matrix Factorization (ALS)**
- Simple à implémenter
- Performant avec nos 85M d'écoutes
- Bibliothèque `implicit` optimisée

### Pour améliorer ensuite
**Option 3 : Hybride avec LightFM**
- Ajouter les features artistes/genres
- Meilleure gestion du cold start

### Pour aller plus loin
**Option 4 : SASRec**
- Si on veut un système temps réel
- Nécessite un GPU

---

## Plan d'implémentation suggéré

### Phase 1 : Préparation des données
1. Parser les 30 dumps JSON
2. Créer la matrice user-item (sparse)
3. Filtrer utilisateurs/morceaux avec trop peu d'écoutes

### Phase 2 : Baseline ALS
1. Entraîner ALS avec `implicit`
2. Évaluer avec precision@k, recall@k, NDCG
3. Itérer sur les hyperparamètres

### Phase 3 : Amélioration
1. Ajouter features avec LightFM
2. Comparer les performances
3. A/B test si possible

### Métriques d'évaluation
- **Precision@K** : % de recommandations pertinentes
- **Recall@K** : % des items pertinents recommandés
- **NDCG** : qualité du ranking
- **Coverage** : diversité des recommandations
- **Novelty** : capacité à recommander des items moins populaires