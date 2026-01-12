#!/usr/bin/env python3
"""
Script pour télécharger les données ListenBrainz
"""

import os
import sys
import subprocess
import re
from pathlib import Path
import requests

# Configuration
LISTENBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/"
OUTPUT_DIR = Path("data/raw/listenbrainz")

def find_latest_dump():
    """Trouve le dernier dump complet disponible"""
    print("🔍 Recherche du dernier dump ListenBrainz disponible...")
    try:
        response = requests.get(LISTENBRAINZ_BASE_URL, timeout=30)
        response.raise_for_status()

        # Chercher les dumps complets (format: listenbrainz-listens-dump-YYYYMMDD-HHMMSS-full.tar.zst)
        dumps = re.findall(r'listenbrainz-listens-dump-\d+-\d+-full\.tar\.zst', response.text)

        if not dumps:
            # Chercher aussi les dumps sans le suffixe -full
            dumps = re.findall(r'listenbrainz-listens-dump-\d+-\d+\.tar\.zst', response.text)

        if dumps:
            latest_dump = sorted(dumps)[-1]
            print(f"✅ Dernier dump trouvé: {latest_dump}")
            return latest_dump
        else:
            print("❌ Aucun dump trouvé")
            return None

    except requests.RequestException as e:
        print(f"❌ Erreur lors de la recherche: {e}")
        return None

def download_file(url, output_path):
    """Télécharge un fichier avec wget (supporte reprise)"""
    print(f"📥 Téléchargement: {url}")
    print(f"⚠️  ATTENTION: Ce fichier peut faire 50-100 GB et prendre plusieurs heures!")
    print(f"💡 Conseil: Le téléchargement peut être interrompu et repris avec Ctrl+C")
    print()

    try:
        # -c : continue (reprise)
        # -q : quiet
        # --show-progress : affiche la progression
        subprocess.run(
            ['wget', '-c', '--show-progress', '-O', str(output_path), url],
            check=True
        )
        print(f"\n✅ Téléchargé: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erreur lors du téléchargement: {e}")
        return False
    except FileNotFoundError:
        print("❌ wget n'est pas installé. Installez-le avec: brew install wget")
        return False
    except KeyboardInterrupt:
        print("\n⚠️  Téléchargement interrompu. Relancez le script pour reprendre.")
        return False

def download_listenbrainz_dump():
    """Télécharge le dump ListenBrainz"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("🎧 Téléchargement des données ListenBrainz")
    print("=" * 60)
    print(f"Destination: {OUTPUT_DIR}")
    print()

    # Trouver le dernier dump
    latest_dump = find_latest_dump()
    if not latest_dump:
        print("❌ Impossible de trouver un dump à télécharger")
        return False

    url = f"{LISTENBRAINZ_BASE_URL}{latest_dump}"
    output_path = OUTPUT_DIR / latest_dump

    # Vérifier si déjà téléchargé
    if output_path.exists():
        file_size = output_path.stat().st_size / (1024 * 1024 * 1024)
        print(f"⏭️  Fichier déjà présent: {output_path}")
        print(f"📦 Taille actuelle: {file_size:.2f} GB")
        response = input("Voulez-vous le re-télécharger? (o/N): ")
        if response.lower() != 'o':
            print("✅ Utilisation du fichier existant")
            return True

    # Télécharger
    success = download_file(url, output_path)

    if success and output_path.exists():
        file_size = output_path.stat().st_size / (1024 * 1024 * 1024)
        print("\n" + "=" * 60)
        print(f"✅ Téléchargement terminé!")
        print(f"📦 Taille: {file_size:.2f} GB")
        print(f"📂 Données stockées dans: {OUTPUT_DIR.absolute()}")
        print(f"📄 Fichier: {latest_dump}")
        print("=" * 60)

    return success

if __name__ == "__main__":
    try:
        success = download_listenbrainz_dump()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrompu par l'utilisateur")
        sys.exit(1)