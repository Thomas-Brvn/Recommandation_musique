#!/usr/bin/env python3
"""
Utilitaire pour charger les variables d'environnement depuis .env
"""

import os
from pathlib import Path
from typing import Optional

def load_env_file(env_path: Optional[str] = None) -> dict:
    """
    Charge les variables d'environnement depuis un fichier .env

    Args:
        env_path: Chemin vers le fichier .env (optionnel)

    Returns:
        dict: Dictionnaire des variables d'environnement
    """
    if env_path is None:
        # Chercher .env à la racine du projet
        project_root = Path(__file__).parent.parent
        env_path = project_root / ".env"

    env_vars = {}

    if not os.path.exists(env_path):
        print(f"⚠️  Fichier .env non trouvé: {env_path}")
        print("💡 Copiez .env.example vers .env et remplissez vos credentials")
        return env_vars

    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Ignorer les commentaires et lignes vides
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    env_vars[key] = value
                    # Mettre aussi dans os.environ
                    os.environ[key] = value

    return env_vars

def get_gcp_config() -> dict:
    """
    Récupère la configuration GCP depuis les variables d'environnement

    Returns:
        dict: Configuration GCP
    """
    load_env_file()

    return {
        'project_id': os.getenv('GCP_PROJECT_ID', 'projetetude-497218'),
        'region': os.getenv('GCP_REGION', 'europe-north1'),
        'bucket_raw_lb': os.getenv('GCS_BUCKET_RAW_LB', 'brainz-raw-listenbrainz'),
        'bucket_raw_mb': os.getenv('GCS_BUCKET_RAW_MB', 'brainz-raw-musicbrainz'),
        'bucket_processed': os.getenv('GCS_BUCKET_PROCESSED', 'brainz-processed'),
    }

if __name__ == "__main__":
    # Test du chargement
    env_vars = load_env_file()
    print(f"✅ {len(env_vars)} variables chargées")

    # Afficher (masquer les valeurs sensibles)
    for key, value in env_vars.items():
        if 'KEY' in key or 'PASSWORD' in key or 'SECRET' in key:
            masked_value = value[:4] + '*' * (len(value) - 4) if len(value) > 4 else '****'
            print(f"  {key}={masked_value}")
        else:
            print(f"  {key}={value}")