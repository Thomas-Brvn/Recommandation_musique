#!/usr/bin/env python3
"""
Télécharge les dumps incrémentaux ListenBrainz manquants vers GCS.

- Scrape automatiquement la page ListenBrainz pour trouver tous les dumps disponibles
- Compare avec les fichiers déjà présents dans GCS
- Télécharge uniquement les nouveaux (streaming direct vers GCS, sans stockage local)

Usage:
    python scripts/download_incrementals.py
    python scripts/download_incrementals.py --dry-run   # voir ce qui serait téléchargé
    python scripts/download_incrementals.py --limit 5   # télécharger max 5 nouveaux dumps
"""
import os
import re
import argparse

import requests
from tqdm import tqdm
from google.cloud import storage

# ── Configuration ──────────────────────────────────────────
BASE_URL    = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental"
GCS_BUCKET  = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
GCS_PREFIX  = "incrementals/"
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
CHUNK_SIZE  = 8 * 1024 * 1024  # 8 MB


# ── Découverte des dumps disponibles ───────────────────────

def list_available_dumps() -> list[dict]:
    """
    Scrape la page ListenBrainz pour trouver tous les dumps incrémentaux.
    Retourne une liste de dicts: {folder, filename, url}
    """
    print("Recherche des dumps disponibles sur ListenBrainz...")
    resp = requests.get(BASE_URL + "/", timeout=30)
    resp.raise_for_status()

    folders = re.findall(
        r'href="(listenbrainz-dump-\d+-\d+-\d+-incremental/)"',
        resp.text
    )

    dumps = []
    for folder in folders:
        folder_name = folder.rstrip('/')
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


# ── Fichiers déjà dans GCS ─────────────────────────────────

def list_gcs_files(client: storage.Client, bucket_name: str, prefix: str) -> set[str]:
    """Retourne l'ensemble des noms de fichiers déjà présents dans GCS."""
    print(f"Vérification des fichiers existants dans gs://{bucket_name}/{prefix}...")
    bucket = client.bucket(bucket_name)
    existing = set()
    for blob in bucket.list_blobs(prefix=prefix):
        filename = blob.name.replace(prefix, "")
        if filename:
            existing.add(filename)
    return existing


# ── Téléchargement streaming vers GCS ─────────────────────

def stream_to_gcs(client: storage.Client, url: str, bucket_name: str,
                  gcs_key: str, filename: str):
    """
    Télécharge un fichier depuis une URL et l'upload directement vers GCS
    en streaming (resumable upload) — aucun stockage local.
    """
    head = requests.head(url, timeout=30)
    total_size = int(head.headers.get("content-length", 0))
    size_mb = total_size / 1024 / 1024

    print(f"  Taille : {size_mb:.0f} MB")

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_key)

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()

        with tqdm(
            total=total_size,
            unit="B", unit_scale=True,
            desc=f"  {filename[:50]}",
            leave=False
        ) as pbar:
            # Accumuler les chunks puis uploader via resumable upload
            buf = bytearray()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                buf += chunk
                pbar.update(len(chunk))

            blob.upload_from_string(
                bytes(buf),
                content_type="application/octet-stream",
            )


# ── Pipeline principal ─────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Télécharge les nouveaux dumps ListenBrainz vers GCS")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche les dumps à télécharger sans les télécharger")
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre maximum de nouveaux dumps à télécharger")
    parser.add_argument("--bucket", default=GCS_BUCKET,
                        help=f"Bucket GCS (défaut: {GCS_BUCKET})")
    args = parser.parse_args()

    client = storage.Client(project=GCP_PROJECT)

    # 1. Dumps disponibles sur ListenBrainz
    available = list_available_dumps()
    print(f"Dumps disponibles sur ListenBrainz : {len(available)}")

    # 2. Dumps déjà dans GCS
    existing = list_gcs_files(client, args.bucket, GCS_PREFIX)
    print(f"Dumps déjà dans GCS               : {len(existing)}")

    # 3. Dumps après le dernier fichier connu
    if existing:
        last_in_gcs = sorted(existing)[-1]
        print(f"Dernier dump en GCS               : {last_in_gcs}")
        missing = [d for d in available if d["filename"] > last_in_gcs]
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
    print(f"\nDémarrage du téléchargement vers gs://{args.bucket}/{GCS_PREFIX}")
    success = 0
    errors  = 0

    for i, dump in enumerate(missing, 1):
        print(f"\n[{i}/{len(missing)}] {dump['filename']}")
        gcs_key = GCS_PREFIX + dump["filename"]
        try:
            stream_to_gcs(client, dump["url"], args.bucket, gcs_key, dump["filename"])
            print(f"  ✓ Uploadé : gs://{args.bucket}/{gcs_key}")
            success += 1
        except Exception as e:
            print(f"  ✗ Erreur  : {e}")
            errors += 1

    print(f"\n{'=' * 50}")
    print(f"Terminé : {success} uploadés, {errors} erreurs")
    print(f"Total dans GCS : {len(existing) + success} dumps")


if __name__ == "__main__":
    main()
