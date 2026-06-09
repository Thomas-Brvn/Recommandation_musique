#!/usr/bin/env python3
"""
Script pour uploader les données téléchargées vers S3
"""

import sys
import subprocess
import json
from pathlib import Path

# Configuration par défaut
DEFAULT_CONFIG_FILE = Path("config/aws_config.json")
DATA_DIR = Path("data/raw")

def load_config():
    """Charge la configuration AWS"""
    if DEFAULT_CONFIG_FILE.exists():
        with open(DEFAULT_CONFIG_FILE, 'r') as f:
            config = json.load(f)
        print(f"✅ Configuration chargée: {DEFAULT_CONFIG_FILE}")
        return config
    else:
        print("⚠️  Fichier de configuration non trouvé")
        print("💡 Lancez d'abord: python scripts/setup_aws_s3.py")
        return None

def get_file_size(path):
    """Retourne la taille d'un fichier en MB"""
    return path.stat().st_size / (1024 * 1024)

def upload_file(local_path, s3_path, show_progress=True):
    """Upload un fichier vers S3"""
    if not local_path.exists():
        print(f"❌ Fichier non trouvé: {local_path}")
        return False

    file_size = get_file_size(local_path)
    print(f"📤 Upload: {local_path.name} ({file_size:.2f} MB)")
    print(f"   → {s3_path}")

    try:
        # Utiliser aws s3 cp avec affichage de progression
        cmd = ["aws", "s3", "cp", str(local_path), s3_path]
        if show_progress:
            cmd.append("--no-progress")  # On gère nous-même l'affichage

        subprocess.run(cmd, check=True, capture_output=False)
        print("   ✅ Upload terminé")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Erreur lors de l'upload: {e}")
        return False

def upload_directory(local_dir, s3_prefix, bucket_name):
    """Upload un répertoire complet vers S3"""
    if not local_dir.exists():
        print(f"⚠️  Répertoire non trouvé: {local_dir}")
        return False

    files = list(local_dir.glob("*"))
    if not files:
        print(f"⚠️  Aucun fichier dans: {local_dir}")
        return False

    print(f"\n📂 Upload du répertoire: {local_dir}")
    print(f"   Destination: s3://{bucket_name}/{s3_prefix}")
    print(f"   Fichiers: {len(files)}")

    # Calculer la taille totale
    total_size = sum(get_file_size(f) for f in files if f.is_file())
    print(f"   Taille totale: {total_size:.2f} MB ({total_size/1024:.2f} GB)")

    response = input("\nContinuer? (O/n): ")
    if response.lower() == 'n':
        print("❌ Upload annulé")
        return False

    print()

    # Utiliser aws s3 sync pour un upload efficace
    s3_path = f"s3://{bucket_name}/{s3_prefix}"
    try:
        cmd = [
            "aws", "s3", "sync",
            str(local_dir),
            s3_path,
            "--exclude", ".*",  # Exclure les fichiers cachés
            "--exclude", "*.keep"  # Exclure les fichiers .keep
        ]

        print("🚀 Début de l'upload...")
        subprocess.run(cmd, check=True)
        print(f"\n✅ Upload terminé: {s3_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erreur lors de l'upload: {e}")
        return False

def verify_upload(bucket_name, s3_prefix):
    """Vérifie que les fichiers ont bien été uploadés"""
    print("\n🔍 Vérification des fichiers uploadés...")
    s3_path = f"s3://{bucket_name}/{s3_prefix}"

    try:
        result = subprocess.run(
            ["aws", "s3", "ls", s3_path, "--recursive", "--human-readable"],
            check=True,
            capture_output=True,
            text=True
        )

        files = [line for line in result.stdout.split('\n') if line.strip()]
        print(f"✅ {len(files)} fichiers trouvés sur S3:")
        for line in files[:5]:  # Afficher les 5 premiers
            print(f"   {line}")
        if len(files) > 5:
            print(f"   ... et {len(files) - 5} autres fichiers")

        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de la vérification: {e}")
        return False

def main():
    """Fonction principale"""
    print("=" * 60)
    print("☁️  Upload des données vers AWS S3")
    print("=" * 60)

    # Charger la config
    config = load_config()
    if not config:
        bucket_name = input("\nNom du bucket S3: ").strip()
        if not bucket_name:
            print("❌ Nom du bucket requis")
            sys.exit(1)
    else:
        bucket_name = config.get("bucket_name")
        print(f"📦 Bucket: {bucket_name}")

    print("\n📁 Données disponibles:")

    # Vérifier les données MusicBrainz
    mb_dir = DATA_DIR / "musicbrainz"
    if mb_dir.exists() and list(mb_dir.glob("*.tar.xz")):
        mb_files = list(mb_dir.glob("*.tar.xz"))
        total_size = sum(get_file_size(f) for f in mb_files)
        print(f"  ✅ MusicBrainz: {len(mb_files)} fichiers ({total_size:.2f} MB)")
        has_mb = True
    else:
        print("  ⚠️  MusicBrainz: Aucune donnée (lancez: python scripts/download_musicbrainz.py)")
        has_mb = False

    # Vérifier les données ListenBrainz
    lb_dir = DATA_DIR / "listenbrainz"
    if lb_dir.exists() and list(lb_dir.glob("*.tar.zst")):
        lb_files = list(lb_dir.glob("*.tar.zst"))
        total_size = sum(get_file_size(f) for f in lb_files)
        print(f"  ✅ ListenBrainz: {len(lb_files)} fichiers ({total_size/1024:.2f} GB)")
        has_lb = True
    else:
        print("  ⚠️  ListenBrainz: Aucune donnée (lancez: python scripts/download_listenbrainz.py)")
        has_lb = False

    if not has_mb and not has_lb:
        print("\n❌ Aucune donnée à uploader")
        print("💡 Téléchargez d'abord les données avec:")
        print("   python scripts/download_musicbrainz.py")
        print("   python scripts/download_listenbrainz.py")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Que souhaitez-vous uploader?")
    print("  1. MusicBrainz uniquement")
    print("  2. ListenBrainz uniquement")
    print("  3. Les deux")
    print("=" * 60)

    choice = input("Votre choix (1/2/3): ").strip()

    success = True

    # Upload MusicBrainz
    if choice in ['1', '3'] and has_mb:
        success = upload_directory(mb_dir, "raw/musicbrainz", bucket_name) and success
        if success:
            verify_upload(bucket_name, "raw/musicbrainz")

    # Upload ListenBrainz
    if choice in ['2', '3'] and has_lb:
        success = upload_directory(lb_dir, "raw/listenbrainz", bucket_name) and success
        if success:
            verify_upload(bucket_name, "raw/listenbrainz")

    if success:
        print("\n" + "=" * 60)
        print("✅ Upload terminé avec succès!")
        print("=" * 60)
        print(f"🔗 Console S3: https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}")
        print("\nProchaines étapes:")
        print("  • Configurer le traitement EMR")
        print("  • Ou configurer le DAG Airflow pour automatiser le pipeline")
        print("=" * 60)
    else:
        print("\n❌ Certains uploads ont échoué")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrompu")
        sys.exit(1)
