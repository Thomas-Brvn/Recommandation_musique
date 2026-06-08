#!/usr/bin/env python3
"""
Lance une instance GCE qui télécharge les dumps ListenBrainz manquants vers GCS.

- Instance e2-small (~0.02€/h) — réseau uniquement, pas de calcul
- Clone le repo GitHub, lance download_incrementals.py
- Se termine automatiquement à la fin
- Coût typique : < 0.05€ par run

Usage:
    python scripts/launch_download_gce.py
    python scripts/launch_download_gce.py --monitor
"""
import os
import json
import time
import argparse
from pathlib import Path

from google.cloud import compute_v1, storage

GCP_PROJECT   = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION    = os.getenv("GCP_REGION", "europe-north1")
GCP_ZONE      = f"{GCP_REGION}-a"
GCS_BUCKET    = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
MACHINE_TYPE  = "e2-small"
GITHUB_REPO   = "https://github.com/Thomas-Brvn/Recommandation_musique.git"

STARTUP_SCRIPT = f"""#!/bin/bash
set -e
exec > >(tee /var/log/startup-script.log) 2>&1

echo "================================"
echo "TÉLÉCHARGEMENT DUMPS LISTENBRAINZ"
echo "================================"
date

GCS_BUCKET="{GCS_BUCKET}"
WORK_DIR="/home/debian/recommendation"

mkdir -p $WORK_DIR
cd $WORK_DIR

echo "Installation des dépendances..."
apt-get update -qq
apt-get install -y git python3-pip python3-venv -qq

echo "Clonage du repo..."
git clone {GITHUB_REPO} .

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install google-cloud-storage requests tqdm -q

echo "Lancement du téléchargement..."
GCS_BUCKET_RAW_LB=$GCS_BUCKET GCP_PROJECT_ID={GCP_PROJECT} python scripts/download_incrementals.py

echo "================================"
echo "TERMINÉ"
echo "================================"
date

# Marquer comme terminé dans GCS
echo "COMPLETED $(date)" > /tmp/download_completed
gsutil cp /tmp/download_completed gs://$GCS_BUCKET/status/download_completed

# Arrêt automatique
shutdown -h now
"""


def get_debian_image(images_client: compute_v1.ImagesClient) -> str:
    image = images_client.get_from_family(project="debian-cloud", family="debian-11")
    return image.self_link


def get_or_create_service_account(iam_client, project: str) -> str:
    """Retourne l'email du service account par défaut du projet."""
    # Utilise le service account par défaut Compute Engine
    return f"{project}@appspot.gserviceaccount.com"


def launch(instances_client: compute_v1.InstancesClient,
           images_client: compute_v1.ImagesClient) -> str:
    print("=" * 50)
    print("LANCEMENT GCE - TÉLÉCHARGEMENT INCRÉMENTAUX")
    print("=" * 50)
    print(f"Instance    : {MACHINE_TYPE}")
    print(f"Bucket GCS  : {GCS_BUCKET}")
    print(f"Zone        : {GCP_ZONE}")
    print("Coût estimé : < 0.05€")
    print("=" * 50)

    disk_image = get_debian_image(images_client)

    instance = compute_v1.Instance()
    instance.name = "lb-download-incrementals"
    instance.machine_type = f"zones/{GCP_ZONE}/machineTypes/{MACHINE_TYPE}"

    # Disk
    disk = compute_v1.AttachedDisk()
    disk.boot = True
    disk.auto_delete = True
    initialize_params = compute_v1.AttachedDiskInitializeParams()
    initialize_params.source_image = disk_image
    initialize_params.disk_size_gb = 30
    initialize_params.disk_type = f"zones/{GCP_ZONE}/diskTypes/pd-balanced"
    disk.initialize_params = initialize_params
    instance.disks = [disk]

    # Network
    network_interface = compute_v1.NetworkInterface()
    access_config = compute_v1.AccessConfig()
    access_config.name = "External NAT"
    access_config.type_ = "ONE_TO_ONE_NAT"
    network_interface.access_configs = [access_config]
    instance.network_interfaces = [network_interface]

    # Service account avec accès GCS
    sa = compute_v1.ServiceAccount()
    sa.email = "default"
    sa.scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    instance.service_accounts = [sa]

    # Startup script
    metadata = compute_v1.Metadata()
    item = compute_v1.Items()
    item.key = "startup-script"
    item.value = STARTUP_SCRIPT
    metadata.items = [item]
    instance.metadata = metadata

    # Labels
    instance.labels = {
        "project": "music-recommendation",
        "purpose": "lb-download",
    }

    # Shutdown behaviour
    instance.scheduling = compute_v1.Scheduling()
    instance.scheduling.on_host_maintenance = "TERMINATE"

    operation = instances_client.insert(
        project=GCP_PROJECT,
        zone=GCP_ZONE,
        instance_resource=instance,
    )
    operation.result()  # Attendre la fin de l'opération

    print(f"\nInstance lancée : {instance.name}")

    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "download_instance.json", "w") as f:
        json.dump({
            "instance_name": instance.name,
            "zone": GCP_ZONE,
            "project": GCP_PROJECT,
            "bucket": GCS_BUCKET,
        }, f, indent=2)

    return instance.name


def monitor(instances_client: compute_v1.InstancesClient,
            gcs_client: storage.Client, instance_name: str):
    print(f"\nMonitoring de {instance_name} (Ctrl+C pour arrêter)...")
    print(f"Logs : gcloud compute instances get-serial-port-output {instance_name} --zone={GCP_ZONE}\n")

    try:
        while True:
            inst = instances_client.get(project=GCP_PROJECT, zone=GCP_ZONE, instance=instance_name)
            state = inst.status

            # Vérifier si terminé dans GCS
            try:
                bucket = gcs_client.bucket(GCS_BUCKET)
                blob = bucket.blob("status/download_completed")
                if blob.exists():
                    print("\nTéléchargement terminé !")
                    print(f"Vérifier GCS : gsutil ls gs://{GCS_BUCKET}/incrementals/")
                    return
            except Exception:
                pass

            print(f"[{time.strftime('%H:%M:%S')}] Instance : {state}   ", end="\r")

            if state == "TERMINATED":
                print("\nInstance terminée.")
                return

            time.sleep(30)

    except KeyboardInterrupt:
        print(f"\n\nMonitoring arrêté. Instance {instance_name} continue en arrière-plan.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", action="store_true", help="Surveiller l'avancement")
    args = parser.parse_args()

    instances_client = compute_v1.InstancesClient()
    images_client    = compute_v1.ImagesClient()
    gcs_client       = storage.Client(project=GCP_PROJECT)

    instance_name = launch(instances_client, images_client)

    print("\nCommandes utiles :")
    print(f"  Logs   : gcloud compute instances get-serial-port-output {instance_name} --zone={GCP_ZONE}")
    print(f"  Statut : gcloud compute instances describe {instance_name} --zone={GCP_ZONE} --format='get(status)'")
    print(f"  Résultat: gsutil ls gs://{GCS_BUCKET}/incrementals/ | tail -5")

    if args.monitor:
        monitor(instances_client, gcs_client, instance_name)


if __name__ == "__main__":
    main()
