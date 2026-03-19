#!/usr/bin/env python3
"""
Script pour télécharger le modèle entraîné depuis S3.
Après l'entraînement sur EC2, ce script récupère le modèle et les fichiers
nécessaires pour lancer l'API localement ou sur une petite instance.
"""
import os
import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "listen-brainz-data")

# Chemins locaux
BASE_DIR = Path(__file__).parent.parent
MODELS_DIR = BASE_DIR / "models"
DATA_DIR = BASE_DIR / "data" / "processed"


def download_from_s3(s3_client, bucket: str, s3_key: str, local_path: Path) -> bool:
    """Télécharge un fichier depuis S3."""
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"  Téléchargement: {s3_key}...")
        s3_client.download_file(bucket, s3_key, str(local_path))
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"    → {local_path.name} ({size_mb:.1f} MB)")
        return True
    except ClientError as e:
        print(f"    ❌ Erreur: {e}")
        return False


def check_pipeline_completed(s3_client, bucket: str) -> bool:
    """Vérifie si le pipeline est terminé."""
    try:
        s3_client.head_object(Bucket=bucket, Key='status/pipeline_completed')
        return True
    except ClientError:
        return False


def download_model(s3_client, bucket: str):
    """Télécharge le modèle et les fichiers nécessaires."""
    print("=" * 60)
    print("TÉLÉCHARGEMENT DU MODÈLE")
    print("=" * 60)

    # Vérifier que le pipeline est terminé
    if not check_pipeline_completed(s3_client, bucket):
        print("⚠️  Le pipeline n'est pas encore terminé!")
        print(f"   Vérifie le statut sur S3: aws s3 ls s3://{bucket}/status/")
        return False

    print(f"\n✅ Pipeline terminé. Téléchargement des fichiers...\n")

    # Fichiers à télécharger
    files_to_download = [
        # Modèle
        ("models/als_model.pkl", MODELS_DIR / "als_model.pkl"),
        ("models/evaluation_results.json", MODELS_DIR / "evaluation_results.json"),

        # Données pour l'API
        ("processed/user_item_matrix.npz", DATA_DIR / "user_item_matrix.npz"),
        ("processed/mappings.json", DATA_DIR / "mappings.json"),
        ("processed/user_mapping.json", DATA_DIR / "user_mapping.json"),
        ("processed/item_mapping.json", DATA_DIR / "item_mapping.json"),
    ]

    success = True
    for s3_key, local_path in files_to_download:
        if not download_from_s3(s3_client, bucket, s3_key, local_path):
            success = False

    if success:
        print("\n" + "=" * 60)
        print("✅ TÉLÉCHARGEMENT TERMINÉ")
        print("=" * 60)
        print(f"\nModèle: {MODELS_DIR / 'als_model.pkl'}")
        print(f"Données: {DATA_DIR}")
        print("\nPour lancer l'API:")
        print(f"  cd {BASE_DIR / 'src'}")
        print("  python serve.py --port 8000")
    else:
        print("\n❌ Certains fichiers n'ont pas pu être téléchargés.")

    return success


def show_evaluation_results(s3_client, bucket: str):
    """Affiche les résultats d'évaluation."""
    import json

    results_file = MODELS_DIR / "evaluation_results.json"

    if not results_file.exists():
        # Essayer de télécharger
        download_from_s3(s3_client, bucket, "models/evaluation_results.json", results_file)

    if results_file.exists():
        with open(results_file) as f:
            results = json.load(f)

        print("\n" + "=" * 60)
        print("RÉSULTATS D'ÉVALUATION")
        print("=" * 60)

        for metric, values in results.items():
            if isinstance(values, dict) and 'mean' in values:
                print(f"  {metric}: {values['mean']:.4f} (±{values.get('std', 0):.4f})")
            elif isinstance(values, dict) and 'value' in values:
                print(f"  {metric}: {values['value']:.4f}")


def main():
    parser = argparse.ArgumentParser(description="Télécharger le modèle depuis S3")
    parser.add_argument("--bucket", default=S3_BUCKET, help="Bucket S3")
    parser.add_argument("--region", default=AWS_REGION, help="Région AWS")
    parser.add_argument("--results", action="store_true",
                       help="Afficher les résultats d'évaluation")

    args = parser.parse_args()

    s3_client = boto3.client('s3', region_name=args.region)

    if args.results:
        show_evaluation_results(s3_client, args.bucket)
    else:
        download_model(s3_client, args.bucket)
        show_evaluation_results(s3_client, args.bucket)


if __name__ == "__main__":
    main()
