#!/usr/bin/env python3
"""
Script pour monitorer le téléchargement sur GCE
Affiche les logs et le statut en temps réel
"""

import sys
import json
import time
import subprocess
from pathlib import Path

from google.cloud import compute_v1, storage

GCP_PROJECT = None  # chargé depuis config


def get_instance_status(instances_client: compute_v1.InstancesClient,
                        project: str, zone: str, instance_name: str) -> dict | None:
    try:
        inst = instances_client.get(project=project, zone=zone, instance=instance_name)
        return {
            'state':        inst.status,
            'machine_type': inst.machine_type.split('/')[-1],
            'zone':         zone,
        }
    except Exception:
        return None


def check_gcs_files(gcs_client: storage.Client, project: str,
                    bucket_raw_lb: str, bucket_raw_mb: str, bucket_processed: str):
    print("\n📦 Fichiers dans GCS :")

    for label, bucket_name, prefix in [
        ("MusicBrainz",  bucket_raw_mb,   ""),
        ("ListenBrainz", bucket_raw_lb,    "incrementals/"),
        ("Processed",    bucket_processed, ""),
    ]:
        try:
            bucket = gcs_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
            print(f"\n  {label} (gs://{bucket_name}/{prefix}) :")
            for blob in blobs:
                size_mb = blob.size / 1e6 if blob.size else 0
                print(f"    ✓ {blob.name} ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"\n  {label}: Erreur — {e}")


def monitor_instance(instances_client, gcs_client, instance_name: str,
                     project: str, zone: str,
                     bucket_raw_lb: str, bucket_raw_mb: str, bucket_processed: str):
    print("=" * 60)
    print(f"📊 Monitoring de l'instance {instance_name}")
    print("=" * 60)
    print("Appuyez sur Ctrl+C pour arrêter le monitoring\n")

    try:
        while True:
            status = get_instance_status(instances_client, project, zone, instance_name)

            if not status:
                print("❌ Instance non trouvée ou erreur")
                break

            print(f"\r⏱️  État: {status['state']} | Type: {status['machine_type']}", end='', flush=True)

            if status['state'] in ['TERMINATED', 'STOPPED']:
                print(f"\n\n✅ Instance {status['state']}")
                check_gcs_files(gcs_client, project, bucket_raw_lb, bucket_raw_mb, bucket_processed)
                break

            # Vérifier le marqueur de fin dans GCS
            try:
                blob = gcs_client.bucket(bucket_raw_lb).blob("status/download_completed")
                if blob.exists():
                    print("\n\n✅ Téléchargement terminé !")
                    check_gcs_files(gcs_client, project, bucket_raw_lb, bucket_raw_mb, bucket_processed)
                    break
            except Exception:
                pass

            time.sleep(10)

    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring arrêté")
        status = get_instance_status(instances_client, project, zone, instance_name)
        if status:
            print(f"  État: {status['state']}")
        check_gcs_files(gcs_client, project, bucket_raw_lb, bucket_raw_mb, bucket_processed)


def load_instance_config() -> dict | None:
    config_file = Path("config/download_instance.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None


def load_gcp_config() -> dict:
    config_file = Path("config/gcp_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    import os
    return {
        "project_id":       os.getenv("GCP_PROJECT_ID", "projetetude-497218"),
        "region":           os.getenv("GCP_REGION", "europe-north1"),
        "bucket_raw_lb":    os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz"),
        "bucket_raw_mb":    os.getenv("GCS_BUCKET_RAW_MB", "brainz-raw-musicbrainz"),
        "bucket_processed": os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed"),
    }


def main():
    instance_config = None
    if len(sys.argv) > 1:
        instance_name = sys.argv[1]
        zone          = sys.argv[2] if len(sys.argv) > 2 else "europe-north1-a"
    else:
        instance_config = load_instance_config()
        if instance_config:
            instance_name = instance_config['instance_name']
            zone          = instance_config['zone']
            print(f"✅ Configuration chargée: {instance_name}")
        else:
            print("❌ Aucune instance trouvée")
            print("\nUtilisation:")
            print("  python scripts/monitor_gce_download.py <instance_name> [zone]")
            sys.exit(1)

    gcp_config = load_gcp_config()
    project    = gcp_config["project_id"]

    instances_client = compute_v1.InstancesClient()
    gcs_client       = storage.Client(project=project)

    monitor_instance(
        instances_client, gcs_client, instance_name, project, zone,
        gcp_config["bucket_raw_lb"],
        gcp_config["bucket_raw_mb"],
        gcp_config["bucket_processed"],
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Arrêt du monitoring")
        sys.exit(0)
