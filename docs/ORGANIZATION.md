# 📂 Organisation du Projet

Ce document explique l'organisation du projet et les bonnes pratiques de sécurité.

## 🎯 Philosophie

- **Sécurité d'abord**: Jamais de credentials dans Git
- **Séparation des concerns**: Code, config, data, docs séparés
- **Environment-based config**: Utilisation de variables d'environnement
- **Documentation claire**: README, guides, exemples

## 📁 Arborescence Détaillée

```
Recommandation_musique/
│
├── .env                           # ⚠️ SECRETS - NON VERSIONNÉ
├── .env.example                   # Template public (sans vraies valeurs)
├── .gitignore                     # Protection des fichiers sensibles
├── README.md                      # Documentation principale
├── requirements.txt               # Dépendances Python
├── Script.py                      # Script principal (legacy)
│
├── venv/                          # 🔒 Environnement virtuel - NON VERSIONNÉ
│   └── ...                        # Packages Python isolés
│
├── config/                        # ⚙️ Configuration
│   ├── .env                       # SECRETS locaux - NON VERSIONNÉ
│   ├── aws_config.json            # Config AWS - NON VERSIONNÉ
│   ├── ec2_instance.json          # Instance active - NON VERSIONNÉ
│   └── load_env.py                # Utilitaire pour charger .env
│
├── scripts/                       # 🚀 Scripts d'automatisation
│   ├── download_to_s3_via_ec2.py # Lance EC2 → S3
│   ├── download_missing_files.py # Fichiers manquants
│   ├── monitor_ec2_download.py   # Monitoring EC2
│   ├── setup_aws_s3.py           # Setup S3
│   └── upload_to_s3.py           # Upload manuel S3
│
├── dags/                          # 🔄 DAGs Airflow
│   └── listenbrainz_pipeline.py  # Pipeline principal
│
├── data/                          # 💾 Données - NON VERSIONNÉ
│   └── raw/
│       ├── musicbrainz/          # Dumps MusicBrainz
│       └── listenbrainz/         # Dumps ListenBrainz
│
├── docs/                          # 📚 Documentation
│   ├── GUIDE_AWS.md              # Guide AWS
│   ├── GUIDE_EC2.md              # Guide EC2
│   └── ORGANIZATION.md           # Ce fichier
│
├── logs/                          # 📝 Logs Airflow - NON VERSIONNÉ
├── plugins/                       # 🔌 Plugins Airflow
│
├── airflow.db*                    # 🗄️ DB Airflow - NON VERSIONNÉ
├── airflow.cfg                    # Config Airflow - NON VERSIONNÉ
│
└── *.sh                           # 🛠️ Scripts shell utilitaires
    ├── quick_start.sh            # Menu interactif
    ├── setup.sh                  # Installation
    └── start_airflow.sh          # Démarrage Airflow
```

## 🔒 Sécurité des Credentials

### Fichiers JAMAIS versionnés (.gitignore)

```
# Secrets
.env
.env.local
config/.env
config/aws_config.json
config/ec2_instance.json
AWS Access Key*

# Environnement
venv/

# Données
data/
*.tar.xz
*.tar

# Airflow
airflow.db*
airflow.cfg
logs/

# Système
.DS_Store
__pycache__/
```

### Configuration avec .env

**Étapes:**

1. **Copier le template:**
```bash
cp .env.example .env
```

2. **Éditer .env avec vos vraies valeurs:**
```bash
nano .env
```

3. **Utiliser dans le code:**
```python
from config.load_env import get_aws_config

config = get_aws_config()
# config['aws_access_key_id']
# config['aws_secret_access_key']
# config['region']
# config['bucket_name']
```

### ⚠️ CE QU'IL NE FAUT JAMAIS FAIRE

❌ Hardcoder des credentials dans le code:
```python
# ❌ JAMAIS FAIRE ÇA
AWS_ACCESS_KEY = "AKIAXXXXXXXXXXX"
```

❌ Commiter .env:
```bash
# ❌ JAMAIS FAIRE ÇA
git add .env
git commit -m "Add config"
```

❌ Laisser des credentials dans des noms de fichiers:
```bash
# ❌ MAUVAIS
AWS Access Key ID [None]: AKIAWPTKMJPJCB
```

### ✅ BONNES PRATIQUES

✅ Utiliser des variables d'environnement:
```python
import os
key = os.getenv('AWS_ACCESS_KEY_ID')
```

✅ Utiliser .env + .env.example:
```bash
# .env.example (versionné)
AWS_ACCESS_KEY_ID=your_key_here

# .env (NON versionné)
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXX
```

✅ Vérifier avant de commit:
```bash
git status
git diff
# Vérifier qu'aucun secret n'est présent
```

## 📦 Workflow de Développement

### 1. Setup initial

```bash
# Clone
git clone <repo>
cd Recommandation_musique

# Environnement
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configuration
cp .env.example .env
nano .env  # Remplir avec vos vraies valeurs
```

### 2. Développement

```bash
# Activer venv à chaque session
source venv/bin/activate

# Vos commandes...
python3 scripts/...

# Désactiver quand terminé
deactivate
```

### 3. Avant de commit

```bash
# Vérifier les fichiers modifiés
git status

# Vérifier le contenu
git diff

# S'assurer qu'aucun secret n'est présent
grep -r "AKIA" .  # Recherche de clés AWS
grep -r "secret" .

# Commit seulement si clean
git add <fichiers_safe>
git commit -m "Message"
```

## 🔄 Mise à jour de l'organisation

Si vous avez des fichiers mal placés:

```bash
# Supprimer du cache Git (sans supprimer le fichier)
git rm --cached fichier_sensible

# Ajouter à .gitignore
echo "fichier_sensible" >> .gitignore

# Commit
git add .gitignore
git commit -m "Update gitignore"
```

## 📊 État Actuel du Projet

### ✅ Complété

- [x] Structure de base créée
- [x] Environnement virtuel configuré
- [x] Scripts de téléchargement EC2 → S3
- [x] MusicBrainz téléchargé (20.8 GB)
- [x] Configuration .env mise en place
- [x] Documentation organisée

### 🔄 En cours

- [ ] ListenBrainz en téléchargement (~121 GB, 2-4h)

### 📋 À faire

- [ ] Décompression des archives
- [ ] Parsing JSON
- [ ] Chargement dans base de données / Spark
- [ ] Pipeline Airflow
- [ ] Algorithme de recommandation
- [ ] API de recommandation

## 🆘 Dépannage

### "Permission denied" sur scripts

```bash
chmod +x *.sh
chmod +x scripts/*.py
```

### Variables d'environnement non chargées

```bash
# Vérifier que .env existe
ls -la .env

# Tester le chargement
python3 config/load_env.py
```

### Git veut commit des secrets

```bash
# Retirer du staging
git reset HEAD fichier_secret

# Ajouter à .gitignore
echo "fichier_secret" >> .gitignore
```

## 📞 Support

Pour toute question sur l'organisation:
1. Consulter ce document
2. Vérifier [README.md](../README.md)
3. Ouvrir une issue sur GitHub