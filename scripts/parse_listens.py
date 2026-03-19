#!/usr/bin/env python3
"""
Script pour parser les fichiers JSON lines des écoutes ListenBrainz.
Transforme les fichiers extraits en un format tabulaire.
"""
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Generator

import pandas as pd
from tqdm import tqdm

# Configuration
EXTRACTED_DIR = Path(__file__).parent.parent / "data" / "extracted" / "listenbrainz"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed"


def parse_listen_line(line: str) -> dict | None:
    """
    Parse une ligne JSON d'écoute ListenBrainz.

    Format attendu:
    {
        "user_name": "username",
        "track_metadata": {
            "track_name": "...",
            "artist_name": "...",
            "release_name": "...",
            "additional_info": {
                "recording_mbid": "...",
                "release_mbid": "...",
                "artist_mbids": ["..."]
            }
        },
        "listened_at": 1234567890
    }
    """
    try:
        data = json.loads(line)

        track_meta = data.get('track_metadata', {})
        additional = track_meta.get('additional_info', {})

        # Extraire les informations essentielles
        listen = {
            'user_name': data.get('user_name'),
            'listened_at': data.get('listened_at'),
            'track_name': track_meta.get('track_name'),
            'artist_name': track_meta.get('artist_name'),
            'release_name': track_meta.get('release_name'),
            'recording_mbid': additional.get('recording_mbid'),
            'release_mbid': additional.get('release_mbid'),
            'artist_mbid': additional.get('artist_mbids', [None])[0] if additional.get('artist_mbids') else None,
        }

        # Valider les champs essentiels
        if listen['user_name'] and listen['track_name']:
            return listen

    except json.JSONDecodeError:
        pass

    return None


def stream_listens_from_file(filepath: Path) -> Generator[dict, None, None]:
    """Génère les écoutes depuis un fichier JSON lines."""
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                listen = parse_listen_line(line)
                if listen:
                    yield listen


def find_listen_files(base_dir: Path) -> list[Path]:
    """Trouve tous les fichiers d'écoutes dans le dossier extrait."""
    listen_files = []

    for path in base_dir.rglob('*'):
        if path.is_file() and path.suffix in ('', '.json', '.jsonl'):
            # Vérifier si c'est un fichier d'écoutes (pas de metadata)
            if 'listen' in path.name.lower() or path.suffix == '':
                listen_files.append(path)

    return sorted(listen_files)


def parse_all_listens(
    extracted_dir: Path = EXTRACTED_DIR,
    output_dir: Path = OUTPUT_DIR,
    batch_size: int = 100_000,
    max_files: int = None
) -> Path:
    """
    Parse tous les fichiers d'écoutes et les sauvegarde en Parquet.

    Args:
        extracted_dir: Dossier contenant les fichiers extraits
        output_dir: Dossier de sortie
        batch_size: Nombre d'écoutes par batch avant écriture
        max_files: Limite le nombre de fichiers (pour tests)

    Returns:
        Chemin vers le fichier Parquet de sortie
    """
    print("=" * 60)
    print("Parsing des écoutes ListenBrainz")
    print("=" * 60)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "listens_raw.parquet"

    # Trouver les fichiers
    print(f"\nRecherche des fichiers dans {extracted_dir}...")
    listen_files = find_listen_files(extracted_dir)
    print(f"Trouvé {len(listen_files)} fichiers d'écoutes")

    if max_files:
        listen_files = listen_files[:max_files]
        print(f"Traitement limité à {max_files} fichiers")

    if not listen_files:
        print("Aucun fichier trouvé!")
        return None

    # Parser les fichiers
    all_listens = []
    total_parsed = 0
    total_errors = 0
    batch_num = 0

    for filepath in tqdm(listen_files, desc="Fichiers"):
        try:
            for listen in stream_listens_from_file(filepath):
                all_listens.append(listen)
                total_parsed += 1

                # Sauvegarder par batch pour gérer la mémoire
                if len(all_listens) >= batch_size:
                    df_batch = pd.DataFrame(all_listens)

                    # Premier batch: créer le fichier
                    if batch_num == 0:
                        df_batch.to_parquet(output_file, index=False)
                    else:
                        # Batches suivants: ajouter au fichier existant
                        # Note: on accumule tout en mémoire puis on écrit à la fin
                        pass

                    batch_num += 1
                    # On garde les données pour écriture finale

        except Exception as e:
            total_errors += 1
            print(f"\nErreur sur {filepath}: {e}")

    # Créer le DataFrame final
    print(f"\nCréation du DataFrame ({len(all_listens)} écoutes)...")
    df = pd.DataFrame(all_listens)

    # Convertir les types
    if 'listened_at' in df.columns:
        df['listened_at'] = pd.to_datetime(df['listened_at'], unit='s', errors='coerce')

    # Statistiques
    print(f"\n{'=' * 60}")
    print("STATISTIQUES")
    print(f"{'=' * 60}")
    print(f"Écoutes parsées: {total_parsed:,}")
    print(f"Fichiers avec erreurs: {total_errors}")
    print(f"Utilisateurs uniques: {df['user_name'].nunique():,}")
    print(f"Tracks uniques: {df['track_name'].nunique():,}")
    print(f"Artistes uniques: {df['artist_name'].nunique():,}")

    if 'listened_at' in df.columns:
        print(f"Période: {df['listened_at'].min()} à {df['listened_at'].max()}")

    # Sauvegarder
    print(f"\nSauvegarde vers {output_file}...")
    df.to_parquet(output_file, index=False)

    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"Taille du fichier: {file_size_mb:.1f} MB")

    return output_file


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Parser les écoutes ListenBrainz")
    parser.add_argument("--input", type=Path, default=EXTRACTED_DIR,
                       help="Dossier des fichiers extraits")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR,
                       help="Dossier de sortie")
    parser.add_argument("--batch-size", type=int, default=100_000,
                       help="Taille des batches")
    parser.add_argument("--max-files", type=int,
                       help="Limite de fichiers à traiter")

    args = parser.parse_args()

    parse_all_listens(
        extracted_dir=args.input,
        output_dir=args.output,
        batch_size=args.batch_size,
        max_files=args.max_files
    )


if __name__ == "__main__":
    main()
