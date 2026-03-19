#!/usr/bin/env python3
"""
Script pour extraire les dumps incrémentaux ListenBrainz (.tar.zst)
"""
import os
import sys
import tarfile
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import zstandard as zstd
import boto3
from tqdm import tqdm

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "listen-brainz-data")
S3_PREFIX = "raw/listenbrainz/incrementals/"
LOCAL_RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "listenbrainz"
LOCAL_EXTRACTED_DIR = Path(__file__).parent.parent / "data" / "extracted" / "listenbrainz"


def download_from_s3(s3_client, bucket: str, key: str, local_path: Path) -> Path:
    """Télécharge un fichier depuis S3."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if not local_path.exists():
        print(f"Téléchargement de {key}...")
        s3_client.download_file(bucket, key, str(local_path))
    return local_path


def extract_tar_zst(archive_path: Path, output_dir: Path) -> list[Path]:
    """
    Extrait une archive .tar.zst et retourne la liste des fichiers extraits.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_files = []

    # Décompression zstd
    dctx = zstd.ZstdDecompressor()

    with open(archive_path, 'rb') as compressed:
        with dctx.stream_reader(compressed) as reader:
            with tarfile.open(fileobj=reader, mode='r|') as tar:
                for member in tar:
                    if member.isfile():
                        # Extraire le fichier
                        tar.extract(member, path=output_dir)
                        extracted_path = output_dir / member.name
                        extracted_files.append(extracted_path)

    return extracted_files


def list_s3_archives(s3_client, bucket: str, prefix: str) -> list[str]:
    """Liste tous les fichiers .tar.zst dans S3."""
    archives = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.tar.zst'):
                archives.append(obj['Key'])

    return sorted(archives)


def process_archive(s3_client, bucket: str, s3_key: str, raw_dir: Path, extracted_dir: Path) -> dict:
    """Télécharge et extrait une archive."""
    filename = Path(s3_key).name
    local_archive = raw_dir / filename
    archive_output_dir = extracted_dir / filename.replace('.tar.zst', '')

    # Vérifier si déjà extrait
    if archive_output_dir.exists() and any(archive_output_dir.iterdir()):
        return {
            'archive': filename,
            'status': 'skipped',
            'files': list(archive_output_dir.glob('**/*'))
        }

    # Télécharger si nécessaire
    download_from_s3(s3_client, bucket, s3_key, local_archive)

    # Extraire
    extracted_files = extract_tar_zst(local_archive, archive_output_dir)

    # Optionnel: supprimer l'archive locale après extraction
    # local_archive.unlink()

    return {
        'archive': filename,
        'status': 'extracted',
        'files': extracted_files
    }


def main(max_archives: int = None, parallel: int = 4):
    """
    Extrait tous les dumps incrémentaux.

    Args:
        max_archives: Nombre max d'archives à traiter (None = toutes)
        parallel: Nombre de téléchargements parallèles
    """
    print("=" * 60)
    print("Extraction des dumps incrémentaux ListenBrainz")
    print("=" * 60)

    # Créer les dossiers
    LOCAL_RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

    # Client S3
    s3_client = boto3.client('s3')

    # Lister les archives
    print(f"\nRecherche des archives dans s3://{S3_BUCKET}/{S3_PREFIX}...")
    archives = list_s3_archives(s3_client, S3_BUCKET, S3_PREFIX)
    print(f"Trouvé {len(archives)} archives")

    if max_archives:
        archives = archives[:max_archives]
        print(f"Traitement limité à {max_archives} archives")

    # Traiter les archives
    results = []
    with tqdm(total=len(archives), desc="Extraction") as pbar:
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(
                    process_archive,
                    s3_client,
                    S3_BUCKET,
                    s3_key,
                    LOCAL_RAW_DIR,
                    LOCAL_EXTRACTED_DIR
                ): s3_key
                for s3_key in archives
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)
                pbar.set_postfix({'last': result['archive'][:30]})

    # Résumé
    extracted = sum(1 for r in results if r['status'] == 'extracted')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    total_files = sum(len(r['files']) for r in results)

    print(f"\n{'=' * 60}")
    print("RÉSUMÉ")
    print(f"{'=' * 60}")
    print(f"Archives extraites: {extracted}")
    print(f"Archives ignorées (déjà extraites): {skipped}")
    print(f"Total fichiers extraits: {total_files}")
    print(f"Dossier de sortie: {LOCAL_EXTRACTED_DIR}")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extraire les dumps ListenBrainz")
    parser.add_argument("--max", type=int, help="Nombre max d'archives à traiter")
    parser.add_argument("--parallel", type=int, default=4, help="Téléchargements parallèles")

    args = parser.parse_args()
    main(max_archives=args.max, parallel=args.parallel)
