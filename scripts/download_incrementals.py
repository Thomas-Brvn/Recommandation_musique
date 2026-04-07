#!/usr/bin/env python3
"""
Télécharge les dumps incrémentaux ListenBrainz manquants vers S3.

- Scrape automatiquement la page ListenBrainz pour trouver tous les dumps disponibles
- Compare avec les fichiers déjà présents dans S3
- Télécharge uniquement les nouveaux (streaming direct vers S3, sans stockage local)

Usage:
    python scripts/download_incrementals.py
    python scripts/download_incrementals.py --dry-run   # voir ce qui serait téléchargé
    python scripts/download_incrementals.py --limit 5   # télécharger max 5 nouveaux dumps
"""
import os
import re
import sys
import argparse
from pathlib import Path

import boto3
import requests
from tqdm import tqdm

# ── Configuration ──────────────────────────────────────────
BASE_URL  = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental"
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "brainz-data")
S3_PREFIX = "raw/listenbrainz/incrementals/"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB par chunk pour le streaming S3


# ── Découverte des dumps disponibles ───────────────────────

def list_available_dumps() -> list[dict]:
    """
    Scrape la page ListenBrainz pour trouver tous les dumps incrémentaux.
    Retourne une liste de dicts: {folder, filename, url}
    """
    print(f"Recherche des dumps disponibles sur ListenBrainz...")
    resp = requests.get(BASE_URL + "/", timeout=30)
    resp.raise_for_status()

    # Les dossiers ressemblent à: listenbrainz-dump-2365-20251216-000003-incremental/
    folders = re.findall(
        r'href="(listenbrainz-dump-\d+-\d+-\d+-incremental/)"',
        resp.text
    )

    dumps = []
    for folder in folders:
        folder_name = folder.rstrip('/')
        # Le fichier dans le dossier suit le pattern:
        # listenbrainz-listens-dump-XXXX-YYYYMMDD-HHMMSS-incremental.tar.zst
        # On reconstruit le nom en remplaçant "listenbrainz-dump-" par "listenbrainz-listens-dump-"
        filename = folder_name.replace(
            "listenbrainz-dump-",
            "listenbrainz-listens-dump-"
        ) + ".tar.zst"
        url = f"{BASE_URL}/{folder_name}/{filename}"
        dumps.append({
            "folder":   folder_name,
            "filename": filename,
            "url":      url,
        })

    return sorted(dumps, key=lambda d: d["filename"])


# ── Fichiers déjà dans S3 ──────────────────────────────────

def list_s3_files(s3_client, bucket: str, prefix: str) -> set[str]:
    """Retourne l'ensemble des noms de fichiers déjà présents dans S3."""
    print(f"Vérification des fichiers existants dans s3://{bucket}/{prefix}...")
    existing = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].replace(prefix, "")
            if filename:
                existing.add(filename)
    return existing


# ── Téléchargement streaming vers S3 ──────────────────────

def stream_to_s3(s3_client, url: str, bucket: str, s3_key: str, filename: str):
    """
    Télécharge un fichier depuis une URL et l'upload directement vers S3
    en streaming (multipart upload) — aucun stockage local.
    """
    # HEAD request pour avoir la taille
    head = requests.head(url, timeout=30)
    total_size = int(head.headers.get("content-length", 0))
    size_mb = total_size / 1024 / 1024

    print(f"  Taille : {size_mb:.0f} MB")

    # Multipart upload S3
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=s3_key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1

    try:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()

            with tqdm(
                total=total_size,
                unit="B", unit_scale=True,
                desc=f"  {filename[:50]}",
                leave=False
            ) as pbar:
                buffer = b""
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    buffer += chunk
                    pbar.update(len(chunk))

                    if len(buffer) >= CHUNK_SIZE:
                        resp = s3_client.upload_part(
                            Bucket=bucket, Key=s3_key,
                            UploadId=upload_id, PartNumber=part_number,
                            Body=buffer
                        )
                        parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                        part_number += 1
                        buffer = b""

                # Dernier chunk
                if buffer:
                    resp = s3_client.upload_part(
                        Bucket=bucket, Key=s3_key,
                        UploadId=upload_id, PartNumber=part_number,
                        Body=buffer
                    )
                    parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})

        s3_client.complete_multipart_upload(
            Bucket=bucket, Key=s3_key,
            MultipartUpload={"Parts": parts},
            UploadId=upload_id,
        )

    except Exception as e:
        s3_client.abort_multipart_upload(
            Bucket=bucket, Key=s3_key, UploadId=upload_id
        )
        raise e


# ── Pipeline principal ─────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Télécharge les nouveaux dumps ListenBrainz vers S3")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche les dumps à télécharger sans les télécharger")
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre maximum de nouveaux dumps à télécharger")
    parser.add_argument("--bucket", default=S3_BUCKET,
                        help=f"Bucket S3 (défaut: {S3_BUCKET})")
    args = parser.parse_args()

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # 1. Dumps disponibles sur ListenBrainz
    available = list_available_dumps()
    print(f"Dumps disponibles sur ListenBrainz : {len(available)}")

    # 2. Dernier dump dans S3
    existing = list_s3_files(s3_client, args.bucket, S3_PREFIX)
    print(f"Dumps déjà dans S3                : {len(existing)}")

    # 3. Dumps après le dernier fichier connu
    if existing:
        last_in_s3 = sorted(existing)[-1]
        print(f"Dernier dump en S3                : {last_in_s3}")
        missing = [d for d in available if d["filename"] > last_in_s3]
    else:
        missing = available
    print(f"Nouveaux dumps à télécharger      : {len(missing)}")

    if not missing:
        print("\nTout est à jour.")
        return

    if args.limit:
        missing = missing[:args.limit]
        print(f"(limité à {args.limit} dumps)")

    print("\nDumps à télécharger :")
    for d in missing:
        print(f"  {d['filename']}")

    if args.dry_run:
        print("\n[dry-run] Aucun téléchargement effectué.")
        return

    # 4. Téléchargement
    print(f"\nDémarrage du téléchargement vers s3://{args.bucket}/{S3_PREFIX}")
    success = 0
    errors  = 0

    for i, dump in enumerate(missing, 1):
        print(f"\n[{i}/{len(missing)}] {dump['filename']}")
        s3_key = S3_PREFIX + dump["filename"]
        try:
            stream_to_s3(s3_client, dump["url"], args.bucket, s3_key, dump["filename"])
            print(f"  ✓ Uploadé : s3://{args.bucket}/{s3_key}")
            success += 1
        except Exception as e:
            print(f"  ✗ Erreur  : {e}")
            errors += 1

    print(f"\n{'=' * 50}")
    print(f"Terminé : {success} uploadés, {errors} erreurs")
    print(f"Total dans S3 : {len(existing) + success} dumps")


if __name__ == "__main__":
    main()
