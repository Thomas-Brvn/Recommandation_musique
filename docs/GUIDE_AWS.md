# Guide AWS - Stockage des données

Ce guide vous explique comment stocker les données MusicBrainz et ListenBrainz sur AWS S3.

## Prérequis

1. **Compte AWS** avec accès à S3
2. **AWS CLI configuré**:
   ```bash
   aws configure
   ```
   - Access Key ID: Votre clé d'accès
   - Secret Access Key: Votre clé secrète
   - Region: `eu-west-3` (Paris) ou `eu-west-1` (Irlande)
   - Output format: `json`

3. **wget installé** (pour télécharger les données):
   ```bash
   brew install wget
   ```

## Processus complet

### Étape 1: Configuration de S3

Créez le bucket S3 et la structure de dossiers:

```bash
python scripts/setup_aws_s3.py
```

Ce script va:
- Vérifier vos credentials AWS
- Créer un bucket S3 (nom par défaut: `music-recommendation-data`)
- Créer la structure de dossiers:
  - `raw/musicbrainz/` - Données brutes MusicBrainz
  - `raw/listenbrainz/` - Données brutes ListenBrainz
  - `extracted/` - Données extraites
  - `processed/` - Données traitées
  - `processed/features/` - Features pour ML
- Optionnel: Activer le versioning
- Optionnel: Configurer archivage automatique vers Glacier

### Étape 2: Télécharger les données MusicBrainz

```bash
python scripts/download_musicbrainz.py
```

Ce script télécharge:
- `artist.tar.xz` (~500 MB)
- `recording.tar.xz` (~2-3 GB)
- `release.tar.xz` (~1-2 GB)
- `release-group.tar.xz` (~500 MB)

Total: ~5-7 GB

**Fonctionnalités:**
- Vérification des checksums SHA256
- Reprise automatique si interruption
- Stockage local dans `data/raw/musicbrainz/`

### Étape 3: Télécharger les données ListenBrainz (Optionnel)

```bash
python scripts/download_listenbrainz.py
```

⚠️ **ATTENTION**: Ce dump fait 50-100 GB et peut prendre plusieurs heures!

**Fonctionnalités:**
- Détection automatique du dernier dump
- Reprise possible (Ctrl+C puis relancer le script)
- Stockage local dans `data/raw/listenbrainz/`

**Conseil**: Commencez sans ListenBrainz pour tester le système, ajoutez-le plus tard.

### Étape 4: Upload vers S3

```bash
python scripts/upload_to_s3.py
```

Ce script:
- Détecte automatiquement les données téléchargées
- Affiche la taille totale avant upload
- Utilise `aws s3 sync` pour un upload efficace
- Vérifie que les fichiers sont bien uploadés
- Permet de choisir quelles données uploader

## Structure finale sur S3

```
s3://music-recommendation-data/
├── raw/
│   ├── musicbrainz/
│   │   ├── artist.tar.xz
│   │   ├── recording.tar.xz
│   │   ├── release.tar.xz
│   │   └── release-group.tar.xz
│   └── listenbrainz/
│       └── listenbrainz-listens-dump-YYYYMMDD-HHMMSS-full.tar.zst
├── extracted/
│   ├── musicbrainz/
│   └── listenbrainz/
├── processed/
└── processed/features/
```

## Coûts estimés AWS

### Stockage S3 (région eu-west-3)

**Sans ListenBrainz:**
- ~7 GB = ~0.16 USD/mois

**Avec ListenBrainz:**
- ~100 GB = ~2.30 USD/mois

### Transfert de données

- Upload vers S3: Gratuit
- Download depuis S3: 0.09 USD/GB (premiers 10 TB)

### Avec archivage Glacier (après 90 jours)

- Stockage Glacier: ~0.004 USD/GB/mois (80% moins cher)

## Commandes utiles

### Lister les fichiers sur S3
```bash
aws s3 ls s3://music-recommendation-data/raw/ --recursive --human-readable
```

### Télécharger un fichier depuis S3
```bash
aws s3 cp s3://music-recommendation-data/raw/musicbrainz/artist.tar.xz ./
```

### Vérifier la taille du bucket
```bash
aws s3 ls s3://music-recommendation-data --recursive --summarize | grep "Total Size"
```

### Supprimer toutes les données (attention!)
```bash
aws s3 rm s3://music-recommendation-data/ --recursive
```

## Workflow recommandé pour débuter

1. **Phase 1: Test avec MusicBrainz uniquement**
   ```bash
   python scripts/setup_aws_s3.py          # Configuration S3
   python scripts/download_musicbrainz.py  # ~7 GB, 30-60 min
   python scripts/upload_to_s3.py          # Upload MusicBrainz
   ```

2. **Phase 2: Ajouter ListenBrainz plus tard**
   ```bash
   python scripts/download_listenbrainz.py  # ~100 GB, plusieurs heures
   python scripts/upload_to_s3.py           # Upload ListenBrainz
   ```

## Dépannage

### Erreur "credentials not configured"
```bash
aws configure
# Entrez vos credentials
```

### Erreur "bucket already exists"
Le bucket existe déjà, c'est normal. Le script continuera.

### Erreur "wget not found"
```bash
brew install wget
```

### Upload trop lent
- Vérifiez votre connexion internet
- Utilisez une région AWS plus proche
- Les fichiers compressés (.xz, .zst) sont optimisés pour le transfert

## Prochaines étapes

Une fois les données sur S3:

1. **Configurer EMR** pour le traitement Spark
2. **Ou utiliser Airflow** pour automatiser le pipeline complet
3. **Extraire et analyser** les données

Pour l'automatisation avec Airflow, voir [README.md](README.md).