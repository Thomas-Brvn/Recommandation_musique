# Recommandation Musique - Pipeline ListenBrainz/MusicBrainz

Système de recommandation musicale basé sur les données MusicBrainz (métadonnées) et ListenBrainz (comportements d'écoute), déployé sur AWS avec Airflow.

## Table des matières

- [Démarrage rapide](#-démarrage-rapide)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Structure du projet](#-structure-du-projet)
- [Utilisation](#-utilisation)
- [Données](#-données)
- [Coûts AWS](#-coûts-aws)

## Démarrage rapide

### 1. Installation de l'environnement

```bash
# Cloner le projet
cd Recommandation_musique

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

### 2. Configuration des credentials

```bash
# Copier le template de configuration
cp .env.example .env

# Éditer .env avec vos credentials AWS
nano .env
```

### 3. Menu interactif (optionnel)

```bash
./quick_start.sh
```

## Installation

### Prérequis

- Python 3.8+
- AWS CLI configuré
- Compte AWS avec $100 de crédits

### Installation automatique

```bash
./setup.sh
```

### Installation manuelle

```bash
# 1. Environnement virtuel
python3 -m venv venv
source venv/bin/activate

# 2. Dépendances
pip install -r requirements.txt

# 3. Configuration AWS
aws configure
```

## Configuration

### Variables d'environnement (.env)

Créez un fichier `.env` à la racine (voir `.env.example`):

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_DEFAULT_REGION=eu-north-1

# S3 Bucket
S3_BUCKET_NAME=listen-brainz-data
```

### Configuration AWS CLI

```bash
aws configure
```

Entrez:
- **AWS Access Key ID**: Votre clé d'accès
- **AWS Secret Access Key**: Votre clé secrète
- **Default region**: `eu-north-1` (Stockholm)
- **Default output format**: `json`

### Sécurité des credentials

 **IMPORTANT**: Ne JAMAIS commiter vos credentials!

- Les fichiers `.env`, `config/aws_config.json` sont dans `.gitignore`
- Utilisez `.env.example` comme template (sans vraies valeurs)
- Les clés AWS doivent rester secrètes

## Structure du projet

```
Recommandation_musique/
 .env                           # Credentials (NON VERSIONNÉ)
 .env.example                   # Template de configuration
 .gitignore                     # Fichiers à ignorer
 README.md                      # Ce fichier
 requirements.txt               # Dépendances Python

 venv/                          # Environnement virtuel (NON VERSIONNÉ)

 config/                        # Configuration
    .env                       # Credentials locaux (NON VERSIONNÉ)
    aws_config.json            # Config AWS (NON VERSIONNÉ)
    ec2_instance.json          # Instance EC2 active (NON VERSIONNÉ)
    load_env.py                # Utilitaire pour charger .env

 scripts/                       # Scripts de téléchargement AWS
    download_to_s3_via_ec2.py # Lance EC2 pour télécharger vers S3
    download_missing_files.py # Télécharge fichiers manquants
    monitor_ec2_download.py   # Surveille le téléchargement EC2
    setup_aws_s3.py           # Configuration S3
    upload_to_s3.py           # Upload manuel vers S3

 dags/                          # DAGs Airflow (pour plus tard)
    listenbrainz_pipeline.py  # Pipeline de traitement

 data/                          # Données locales (NON VERSIONNÉ)
    raw/                       # Données brutes
        musicbrainz/           # Dumps MusicBrainz
        listenbrainz/          # Dumps ListenBrainz

 docs/                          # Documentation
    GUIDE_AWS.md              # Guide AWS détaillé
    GUIDE_EC2.md              # Guide EC2

 logs/                          # Logs Airflow (NON VERSIONNÉ)
 plugins/                       # Plugins Airflow

 quick_start.sh                 # Menu interactif
```

## Utilisation

### Téléchargement des données

#### Option 1: Via EC2 (recommandé - pas de téléchargement local)

```bash
# Télécharger MusicBrainz uniquement (~7 GB)
python3 scripts/download_to_s3_via_ec2.py 1

# Télécharger ListenBrainz uniquement (~121 GB)
python3 scripts/download_to_s3_via_ec2.py 2

# Télécharger les deux
python3 scripts/download_to_s3_via_ec2.py 3
```

#### Monitoring du téléchargement

```bash
# Monitoring automatique
python3 scripts/monitor_ec2_download.py

# Vérifier les fichiers sur S3
aws s3 ls s3://listen-brainz-data/raw/ --recursive --region eu-north-1 --human-readable

# Voir les logs EC2
aws ec2 get-console-output --instance-id i-xxxxx --region eu-north-1
```

#### Important: Terminer l'instance après téléchargement

```bash
aws ec2 terminate-instances --instance-ids i-xxxxx --region eu-north-1
```

### Airflow (optionnel - pour plus tard)

```bash
# Installation Airflow
./setup.sh

# Démarrer Airflow
./start_airflow.sh

# Accès web: http://localhost:8080
# Login: admin / Password: admin
```

## Données

### MusicBrainz (~20.8 GB compressé)

Métadonnées musicales:

- **artist.tar.xz** (1.5 GB) - Informations artistes
- **recording.tar.xz** (30 MB) - Enregistrements/pistes
- **release.tar.xz** (18.3 GB) - Albums/singles
- **release-group.tar.xz** (1.0 GB) - Groupes d'albums

Source: https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/

### ListenBrainz (~121 GB compressé)

Comportements d'écoute utilisateurs:

- **listenbrainz-spark-dump-*.tar** - Historique d'écoutes complet

Source: https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/

### Hiérarchie des données

```
Artist (The Beatles)
   Release-Group (Abbey Road)
         Release (Abbey Road 1969 UK Vinyl)
               Recording (Come Together)
```

ListenBrainz → Recording → Artist/Release

## Coûts AWS

### Téléchargement via EC2

- **Instance t3.small**: ~0.025 USD/h
- **MusicBrainz** (~20 GB): ~0.03 USD (15-30 min)
- **ListenBrainz** (~121 GB): ~0.10-0.20 USD (2-4h)
- **Total téléchargement**: ~0.30-0.40 USD

### Stockage S3

- **MusicBrainz** (~20 GB): ~0.46 USD/mois
- **ListenBrainz** (~121 GB): ~2.78 USD/mois
- **Total stockage**: ~3.24 USD/mois

### Réduire les coûts

- Supprimer l'instance EC2 après téléchargement 
- Utiliser S3 Intelligent-Tiering
- Archiver vers S3 Glacier après traitement

## Commandes utiles

### AWS S3

```bash
# Lister les fichiers
aws s3 ls s3://listen-brainz-data/raw/ --recursive --human-readable

# Télécharger depuis S3
aws s3 cp s3://listen-brainz-data/raw/musicbrainz/artist.tar.xz . --region eu-north-1

# Supprimer des fichiers
aws s3 rm s3://listen-brainz-data/raw/file.tar.xz --region eu-north-1
```

### AWS EC2

```bash
# Voir les instances actives
aws ec2 describe-instances --region eu-north-1 --filters "Name=instance-state-name,Values=running"

# Terminer une instance
aws ec2 terminate-instances --instance-ids i-xxxxx --region eu-north-1
```

### Environnement

```bash
# Activer le venv
source venv/bin/activate

# Charger les variables d'environnement
python3 config/load_env.py

# Désactiver le venv
deactivate
```

## Documentation

- [GUIDE_AWS.md](docs/GUIDE_AWS.md) - Configuration AWS détaillée
- [GUIDE_EC2.md](docs/GUIDE_EC2.md) - Utilisation EC2
- [MusicBrainz Database Schema](https://musicbrainz.org/doc/MusicBrainz_Database)
- [ListenBrainz API](https://listenbrainz.readthedocs.io/)

## Contribution

Ce projet est un travail personnel. Pour toute question, ouvrez une issue.

## Licence

MIT License - Voir LICENSE pour plus de détails.

## Notes importantes

- **Sécurité**: Ne jamais commiter de credentials AWS
- **Coûts**: Toujours terminer les instances EC2 après usage
- **Données**: Les dumps font ~150 GB au total
- **Région**: Utiliser eu-north-1 (Stockholm) pour minimiser les coûts
