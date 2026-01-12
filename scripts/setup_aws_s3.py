#!/usr/bin/env python3
"""
Script pour configurer AWS S3 pour le projet
Crée le bucket et la structure de dossiers
"""

import sys
import subprocess
import json
from pathlib import Path

# Configuration
DEFAULT_BUCKET_NAME = "music-recommendation-data"
DEFAULT_REGION = "eu-west-3"

def run_aws_command(cmd, check=True):
    """Exécute une commande AWS CLI"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=True,
            text=True
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout, e.stderr, e.returncode

def check_aws_credentials():
    """Vérifie que les credentials AWS sont configurés"""
    print("🔐 Vérification des credentials AWS...")
    stdout, stderr, code = run_aws_command("aws sts get-caller-identity", check=False)

    if code == 0:
        identity = json.loads(stdout)
        print(f"✅ Connecté en tant que: {identity.get('Arn', 'Unknown')}")
        return True
    else:
        print("❌ Credentials AWS non configurés")
        print("\nConfigurez vos credentials avec:")
        print("  aws configure")
        return False

def bucket_exists(bucket_name):
    """Vérifie si un bucket existe"""
    stdout, stderr, code = run_aws_command(f"aws s3 ls s3://{bucket_name}", check=False)
    return code == 0

def create_bucket(bucket_name, region):
    """Crée un bucket S3"""
    print(f"\n📦 Création du bucket: {bucket_name}")

    if bucket_exists(bucket_name):
        print(f"⏭️  Le bucket existe déjà: {bucket_name}")
        return True

    if region == "us-east-1":
        # us-east-1 ne nécessite pas LocationConstraint
        cmd = f"aws s3 mb s3://{bucket_name}"
    else:
        cmd = f"aws s3 mb s3://{bucket_name} --region {region}"

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code == 0:
        print(f"✅ Bucket créé: {bucket_name}")
        return True
    else:
        print(f"❌ Erreur lors de la création: {stderr}")
        return False

def create_folder_structure(bucket_name):
    """Crée la structure de dossiers dans S3"""
    print(f"\n📁 Création de la structure de dossiers...")

    folders = [
        "raw/musicbrainz/",
        "raw/listenbrainz/",
        "extracted/musicbrainz/",
        "extracted/listenbrainz/",
        "processed/",
        "processed/features/",
        "scripts/",
    ]

    for folder in folders:
        # Créer un fichier .keep pour maintenir le dossier
        cmd = f"aws s3api put-object --bucket {bucket_name} --key {folder}.keep --body /dev/null"
        stdout, stderr, code = run_aws_command(cmd, check=False)

        if code == 0:
            print(f"  ✅ {folder}")
        else:
            print(f"  ⚠️  {folder} (peut déjà exister)")

    print("✅ Structure de dossiers créée")
    return True

def enable_versioning(bucket_name):
    """Active le versioning sur le bucket (optionnel)"""
    print(f"\n🔄 Activation du versioning...")
    response = input("Voulez-vous activer le versioning? (o/N): ")

    if response.lower() == 'o':
        cmd = f"aws s3api put-bucket-versioning --bucket {bucket_name} --versioning-configuration Status=Enabled"
        stdout, stderr, code = run_aws_command(cmd, check=False)

        if code == 0:
            print("✅ Versioning activé")
            return True
        else:
            print(f"❌ Erreur: {stderr}")
            return False
    else:
        print("⏭️  Versioning non activé")
        return True

def setup_lifecycle_policy(bucket_name):
    """Configure une politique de cycle de vie (optionnel)"""
    print(f"\n♻️  Configuration de la politique de cycle de vie...")
    response = input("Voulez-vous archiver les anciennes données vers Glacier après 90 jours? (o/N): ")

    if response.lower() == 'o':
        policy = {
            "Rules": [
                {
                    "Id": "ArchiveOldData",
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "Days": 90,
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "Filter": {
                        "Prefix": "raw/"
                    }
                }
            ]
        }

        # Sauvegarder temporairement la politique
        policy_file = Path("/tmp/lifecycle-policy.json")
        with open(policy_file, 'w') as f:
            json.dump(policy, f)

        cmd = f"aws s3api put-bucket-lifecycle-configuration --bucket {bucket_name} --lifecycle-configuration file://{policy_file}"
        stdout, stderr, code = run_aws_command(cmd, check=False)

        policy_file.unlink()

        if code == 0:
            print("✅ Politique de cycle de vie configurée")
            return True
        else:
            print(f"❌ Erreur: {stderr}")
            return False
    else:
        print("⏭️  Politique de cycle de vie non configurée")
        return True

def display_bucket_info(bucket_name, region):
    """Affiche les informations du bucket"""
    print("\n" + "=" * 60)
    print("✅ Configuration S3 terminée!")
    print("=" * 60)
    print(f"📦 Bucket: {bucket_name}")
    print(f"🌍 Région: {region}")
    print(f"🔗 Console: https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}")
    print("\nStructure créée:")
    print("  • raw/musicbrainz/     - Données brutes MusicBrainz")
    print("  • raw/listenbrainz/    - Données brutes ListenBrainz")
    print("  • extracted/           - Données extraites")
    print("  • processed/           - Données traitées")
    print("  • processed/features/  - Features pour ML")
    print("\nProchaines étapes:")
    print("  1. Téléchargez les données: python scripts/download_musicbrainz.py")
    print("  2. Uploadez vers S3: python scripts/upload_to_s3.py")
    print("=" * 60)

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🚀 Configuration AWS S3 pour Recommandation Musique")
    print("=" * 60)

    # Vérifier les credentials
    if not check_aws_credentials():
        sys.exit(1)

    # Demander le nom du bucket
    print(f"\n📝 Configuration du bucket")
    bucket_name = input(f"Nom du bucket [{DEFAULT_BUCKET_NAME}]: ").strip()
    if not bucket_name:
        bucket_name = DEFAULT_BUCKET_NAME

    region = input(f"Région AWS [{DEFAULT_REGION}]: ").strip()
    if not region:
        region = DEFAULT_REGION

    print(f"\n📋 Récapitulatif:")
    print(f"  Bucket: {bucket_name}")
    print(f"  Région: {region}")
    response = input("\nContinuer? (O/n): ")
    if response.lower() == 'n':
        print("❌ Annulé")
        sys.exit(0)

    # Créer le bucket
    if not create_bucket(bucket_name, region):
        sys.exit(1)

    # Créer la structure
    create_folder_structure(bucket_name)

    # Options supplémentaires
    enable_versioning(bucket_name)
    setup_lifecycle_policy(bucket_name)

    # Sauvegarder la config
    config = {
        "bucket_name": bucket_name,
        "region": region
    }
    config_file = Path("config/aws_config.json")
    config_file.parent.mkdir(exist_ok=True)
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Configuration sauvegardée dans: {config_file}")

    # Afficher le résumé
    display_bucket_info(bucket_name, region)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Configuration interrompue")
        sys.exit(1)