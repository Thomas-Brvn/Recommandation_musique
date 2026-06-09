#!/usr/bin/env python3
"""
Upload les données téléchargées vers GCS.
"""

import sys
import json
from pathlib import Path

from google.cloud import storage

DEFAULT_CONFIG_FILE = Path("config/gcp_config.json")
DATA_DIR = Path("data/raw")


def load_config() -> dict | None:
    if DEFAULT_CONFIG_FILE.exists():
        with open(DEFAULT_CONFIG_FILE, 'r') as f:
            config = json.load(f)
        print(f"✅ Configuration chargée : {DEFAULT_CONFIG_FILE}")
        return config
    print("⚠️  config/gcp_config.json non trouvé")
    print("💡 Lancez d'abord : python scripts/setup_gcs.py")
    return None


def get_file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def upload_file(client: storage.Client, local_path: Path, bucket_name: str, gcs_key: str):
    if not local_path.exists():
        print(f"❌ Fichier non trouvé : {local_path}")
        return False

    size_mb = get_file_size_mb(local_path)
    print(f"📤 Upload : {local_path.name} ({size_mb:.2f} MB) → gs://{bucket_name}/{gcs_key}")

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_key)
        blob.upload_from_filename(str(local_path))
        print("   ✅ Terminé")
        return True
    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return False


def upload_directory(client: storage.Client, local_dir: Path, bucket_name: str, gcs_prefix: str):
    if not local_dir.exists():
        print(f"⚠️  Répertoire non trouvé : {local_dir}")
        return False

    files = [f for f in local_dir.glob("*") if f.is_file()]
    if not files:
        print(f"⚠️  Aucun fichier dans : {local_dir}")
        return False

    total_size = sum(get_file_size_mb(f) for f in files)
    print(f"\n📂 Upload : {local_dir} → gs://{bucket_name}/{gcs_prefix}")
    print(f"   {len(files)} fichiers, {total_size:.2f} MB ({total_size/1024:.2f} GB)")

    confirm = input("\nContinuer ? (O/n): ")
    if confirm.lower() == 'n':
        print("❌ Annulé")
        return False

    success = 0
    for f in files:
        gcs_key = f"{gcs_prefix}{f.name}" if gcs_prefix else f.name
        if upload_file(client, f, bucket_name, gcs_key):
            success += 1

    print(f"\n✅ {success}/{len(files)} fichiers uploadés")
    return success == len(files)


def main():
    print("=" * 60)
    print("☁️  Upload des données vers GCS")
    print("=" * 60)

    config = load_config()
    if not config:
        sys.exit(1)

    project          = config["project_id"]
    bucket_raw_lb    = config["bucket_raw_lb"]
    bucket_raw_mb    = config["bucket_raw_mb"]

    client = storage.Client(project=project)

    print("\n📁 Données disponibles :")

    mb_dir = DATA_DIR / "musicbrainz"
    mb_files = list(mb_dir.glob("*.tar.xz")) if mb_dir.exists() else []
    if mb_files:
        total = sum(get_file_size_mb(f) for f in mb_files)
        print(f"  ✅ MusicBrainz  : {len(mb_files)} fichiers ({total:.2f} MB)")
        has_mb = True
    else:
        print("  ⚠️  MusicBrainz  : Aucune donnée")
        has_mb = False

    lb_dir = DATA_DIR / "listenbrainz"
    lb_files = list(lb_dir.glob("*.tar.zst")) if lb_dir.exists() else []
    if lb_files:
        total = sum(get_file_size_mb(f) for f in lb_files)
        print(f"  ✅ ListenBrainz : {len(lb_files)} fichiers ({total/1024:.2f} GB)")
        has_lb = True
    else:
        print("  ⚠️  ListenBrainz : Aucune donnée")
        has_lb = False

    if not has_mb and not has_lb:
        print("\n❌ Aucune donnée à uploader")
        sys.exit(1)

    print("\n1. MusicBrainz uniquement")
    print("2. ListenBrainz uniquement")
    print("3. Les deux")
    choice = input("Votre choix (1/2/3): ").strip()

    if choice in ['1', '3'] and has_mb:
        upload_directory(client, mb_dir, bucket_raw_mb, "")

    if choice in ['2', '3'] and has_lb:
        upload_directory(client, lb_dir, bucket_raw_lb, "")

    print("\n" + "=" * 60)
    print("✅ Upload terminé !")
    print(f"🔗 Console GCS : https://console.cloud.google.com/storage/browser/{bucket_raw_mb}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrompu")
        sys.exit(1)
