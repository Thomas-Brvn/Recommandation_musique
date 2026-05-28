#!/usr/bin/env python3
"""
Configure GCP / GCS pour le projet.
Vérifie les credentials, crée config/gcp_config.json.
"""

import sys
import json
import os
import shutil
import subprocess
from pathlib import Path

DEFAULT_PROJECT = "projetetude-497218"
DEFAULT_REGION  = "europe-north1"
DEFAULT_BUCKET_RAW_LB    = "brainz-raw-listenbrainz"
DEFAULT_BUCKET_RAW_MB    = "brainz-raw-musicbrainz"
DEFAULT_BUCKET_PROCESSED = "brainz-processed"


def check_gcloud_installed() -> bool:
    return shutil.which("gcloud") is not None


def install_gcloud_instructions():
    print("\n❌ gcloud CLI non installé.")
    print("\nInstallation (macOS) :")
    print("  brew install --cask google-cloud-sdk")
    print("\nOu téléchargez depuis :")
    print("  https://cloud.google.com/sdk/docs/install")
    print("\nAprès installation, authentifiez-vous :")
    print("  gcloud auth application-default login")


def check_gcp_credentials() -> bool:
    """Vérifie les credentials GCP (gcloud ou GOOGLE_APPLICATION_CREDENTIALS)."""
    print("🔐 Vérification des credentials GCP...")

    # Option 1 : variable GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
    sa_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_file and Path(sa_file).exists():
        print(f"✅ Service account JSON trouvé : {sa_file}")
        return True

    # Option 2 : gcloud application-default credentials
    if check_gcloud_installed():
        result = subprocess.run(
            ["gcloud", "auth", "application-default", "print-access-token"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("✅ Credentials gcloud valides")
            return True
        else:
            print("⚠️  gcloud installé mais non authentifié.")
            print("Lancez : gcloud auth application-default login")
            return False
    else:
        # Option 3 : ADC par défaut (~/.config/gcloud/)
        adc_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
        if adc_path.exists():
            print(f"✅ Application Default Credentials trouvés : {adc_path}")
            return True

        install_gcloud_instructions()
        print("\n⚠️  Vous pouvez aussi utiliser un fichier service account JSON :")
        print("  export GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/service-account.json")
        return False


def check_bucket_exists(bucket_name: str, project: str) -> bool:
    """Vérifie si un bucket GCS existe."""
    try:
        from google.cloud import storage
        from google.api_core.exceptions import NotFound
        client = storage.Client(project=project)
        client.get_bucket(bucket_name)
        return True
    except Exception:
        return False


def display_bucket_info(config: dict):
    print("\n" + "=" * 60)
    print("✅ Configuration GCS terminée !")
    print("=" * 60)
    print(f"🔑 Projet    : {config['project_id']}")
    print(f"🌍 Région    : {config['region']}")
    print(f"📦 Bucket LB : gs://{config['bucket_raw_lb']}")
    print(f"📦 Bucket MB : gs://{config['bucket_raw_mb']}")
    print(f"📦 Processed : gs://{config['bucket_processed']}")
    print("\nProchaines étapes :")
    print("  1. Télécharger les données : python scripts/download_incrementals.py")
    print("  2. Ou lancer via GCE       : python scripts/download_to_gcs_via_gce.py")
    print("=" * 60)


def main():
    print("=" * 60)
    print("🚀 Configuration GCP/GCS pour Recommandation Musique")
    print("=" * 60)

    creds_ok = check_gcp_credentials()
    if not creds_ok:
        print("\n⚠️  Credentials manquants — la config sera sauvegardée mais")
        print("   les buckets ne pourront pas être vérifiés maintenant.")
        cont = input("Continuer quand même ? (o/N): ").strip().lower()
        if cont != 'o':
            sys.exit(1)

    print(f"\n📝 Configuration des buckets GCS")
    project = input(f"GCP Project ID [{DEFAULT_PROJECT}]: ").strip() or DEFAULT_PROJECT
    region  = input(f"Région GCP [{DEFAULT_REGION}]: ").strip() or DEFAULT_REGION

    bucket_raw_lb    = input(f"Bucket raw ListenBrainz [{DEFAULT_BUCKET_RAW_LB}]: ").strip() or DEFAULT_BUCKET_RAW_LB
    bucket_raw_mb    = input(f"Bucket raw MusicBrainz [{DEFAULT_BUCKET_RAW_MB}]: ").strip() or DEFAULT_BUCKET_RAW_MB
    bucket_processed = input(f"Bucket processed [{DEFAULT_BUCKET_PROCESSED}]: ").strip() or DEFAULT_BUCKET_PROCESSED

    config = {
        "project_id":       project,
        "region":           region,
        "bucket_raw_lb":    bucket_raw_lb,
        "bucket_raw_mb":    bucket_raw_mb,
        "bucket_processed": bucket_processed,
    }

    if creds_ok:
        print("\n📋 Vérification des buckets...")
        for label, name in [
            ("raw ListenBrainz", bucket_raw_lb),
            ("raw MusicBrainz",  bucket_raw_mb),
            ("processed",        bucket_processed),
        ]:
            if check_bucket_exists(name, project):
                print(f"  ✅ gs://{name}")
            else:
                print(f"  ⚠️  gs://{name} — introuvable (vérifiez le nom)")

    config_file = Path("config/gcp_config.json")
    config_file.parent.mkdir(exist_ok=True)
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Configuration sauvegardée : {config_file}")

    display_bucket_info(config)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Configuration interrompue")
        sys.exit(1)
