#!/usr/bin/env python3
"""
Script pour télécharger les données MusicBrainz
"""

import os
import sys
import subprocess
import hashlib
from pathlib import Path

# Configuration
MUSICBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
MB_TABLES = ["artist", "recording", "release", "release-group"]
OUTPUT_DIR = Path("data/raw/musicbrainz")

def download_file(url, output_path):
    """Télécharge un fichier avec wget"""
    print(f"📥 Téléchargement: {url}")
    try:
        subprocess.run(['wget', '-c', '-O', str(output_path), url], check=True)
        print(f"✅ Téléchargé: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du téléchargement: {e}")
        return False
    except FileNotFoundError:
        print("❌ wget n'est pas installé. Installez-le avec: brew install wget")
        return False

def verify_checksum(file_path, checksum_file):
    """Vérifie l'intégrité du fichier téléchargé"""
    print(f"🔍 Vérification du checksum pour {file_path.name}...")
    try:
        with open(checksum_file, 'r') as f:
            checksums = f.read()

        # Trouver le checksum pour ce fichier
        for line in checksums.split('\n'):
            if file_path.name in line:
                expected_hash = line.split()[0]

                # Calculer le hash du fichier
                sha256_hash = hashlib.sha256()
                with open(file_path, "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)

                actual_hash = sha256_hash.hexdigest()

                if actual_hash == expected_hash:
                    print(f"✅ Checksum valide pour {file_path.name}")
                    return True
                else:
                    print(f"❌ Checksum invalide pour {file_path.name}")
                    return False

        print(f"⚠️  Checksum non trouvé pour {file_path.name}")
        return False
    except Exception as e:
        print(f"⚠️  Impossible de vérifier le checksum: {e}")
        return False

def download_musicbrainz_dumps():
    """Télécharge tous les dumps MusicBrainz"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("🎵 Téléchargement des données MusicBrainz")
    print("=" * 60)
    print(f"Tables à télécharger: {', '.join(MB_TABLES)}")
    print(f"Destination: {OUTPUT_DIR}")
    print()

    # Télécharger le fichier de checksums
    checksum_url = f"{MUSICBRAINZ_BASE_URL}SHA256SUMS"
    checksum_file = OUTPUT_DIR / "SHA256SUMS"

    print("📥 Téléchargement du fichier de checksums...")
    if not download_file(checksum_url, checksum_file):
        print("⚠️  Impossible de télécharger les checksums, continuation sans vérification")
        checksum_file = None

    print()

    # Télécharger chaque table
    success_count = 0
    for i, table in enumerate(MB_TABLES, 1):
        print(f"\n[{i}/{len(MB_TABLES)}] Table: {table}")
        print("-" * 60)

        url = f"{MUSICBRAINZ_BASE_URL}{table}.tar.xz"
        output_path = OUTPUT_DIR / f"{table}.tar.xz"

        # Vérifier si déjà téléchargé
        if output_path.exists():
            print(f"⏭️  Fichier déjà présent: {output_path}")
            if checksum_file and verify_checksum(output_path, checksum_file):
                success_count += 1
                continue
            else:
                print("🔄 Re-téléchargement du fichier...")

        # Télécharger
        if download_file(url, output_path):
            # Vérifier le checksum si disponible
            if checksum_file:
                if verify_checksum(output_path, checksum_file):
                    success_count += 1
                else:
                    print("⚠️  Fichier téléchargé mais checksum invalide")
            else:
                success_count += 1

        file_size = output_path.stat().st_size / (1024 * 1024) if output_path.exists() else 0
        print(f"📦 Taille: {file_size:.2f} MB")

    print("\n" + "=" * 60)
    print(f"✅ Téléchargement terminé: {success_count}/{len(MB_TABLES)} fichiers")
    print(f"📂 Données stockées dans: {OUTPUT_DIR.absolute()}")
    print("=" * 60)

    return success_count == len(MB_TABLES)

if __name__ == "__main__":
    success = download_musicbrainz_dumps()
    sys.exit(0 if success else 1)