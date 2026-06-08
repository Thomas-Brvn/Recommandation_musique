#!/usr/bin/env python3
"""
Lance le pipeline COMPLET de recommandation sur GCE.

- Instance n2-highmem-2 (2 vCPU, 16 GB RAM)
- Paramètres de production (128 facteurs, 15 itérations)
- Coût estimé : ~0.10€/h × 2-4h = ~0.40€ total

Usage:
    python scripts/run_full_pipeline_gce.py
    python scripts/run_full_pipeline_gce.py --monitor
"""
import os
import json
import time
import argparse
from pathlib import Path

from google.cloud import compute_v1

GCP_PROJECT    = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION     = os.getenv("GCP_REGION", "europe-north1")
GCP_ZONE       = f"{GCP_REGION}-a"
GCS_RAW_LB     = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
GCS_PROCESSED  = os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed")
MACHINE_TYPE   = "n2-highmem-2"   # 2 vCPU, 16 GB RAM (~0.10€/h)
GITHUB_REPO    = "https://github.com/Thomas-Brvn/Recommandation_musique.git"


def get_startup_script(gcs_raw_lb: str, gcs_processed: str) -> str:
    return f"""#!/bin/bash
set -e
exec > >(tee /var/log/startup-script.log) 2>&1

echo "=========================================="
echo "PIPELINE COMPLET - RECOMMANDATION MUSICALE"
echo "=========================================="
date

GCS_RAW_LB="{gcs_raw_lb}"
GCS_PROCESSED="{gcs_processed}"
WORK_DIR="/home/debian/recommendation"

mkdir -p $WORK_DIR
cd $WORK_DIR

# Swap 16 GB pour compenser la RAM
fallocate -l 16G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
free -h

apt-get update -qq
apt-get install -y python3-pip python3-venv git zstd google-cloud-cli -qq

git clone {GITHUB_REPO} .
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip -q
pip install pandas pyarrow scipy zstandard numpy scikit-learn tqdm google-cloud-storage implicit -q

mkdir -p data/work data/processed models

# Télécharger les dumps depuis GCS
echo "Téléchargement des données depuis GCS..."
gsutil -m cp "gs://$GCS_RAW_LB/incrementals/*.tar.zst" data/work/ 2>/dev/null || true
echo "Données téléchargées: $(ls data/work/*.tar.zst 2>/dev/null | wc -l) fichiers"

# Lancer le pipeline
echo "Lancement du pipeline..."
python Script.py --input data/work/ --output data/processed/ --model models/

# Uploader les résultats
echo "Upload des résultats..."
gsutil -m cp -r data/processed/ gs://$GCS_PROCESSED/processed/
gsutil -m cp -r models/ gs://$GCS_PROCESSED/models/
echo "COMPLETED $(date)" | gsutil cp - gs://$GCS_PROCESSED/status/full_pipeline_completed

echo "=========================================="
echo "PIPELINE TERMINÉ — $(date)"
echo "=========================================="
shutdown -h now
"""


def launch(instances_client, images_client, project: str, zone: str,
           gcs_raw_lb: str, gcs_processed: str) -> str:
    print("=" * 50)
    print("LANCEMENT GCE - PIPELINE COMPLET")
    print("=" * 50)
    print(f"Machine     : {MACHINE_TYPE}")
    print(f"Zone        : {zone}")
    print("Coût estimé : ~0.40€ total")
    print("=" * 50)

    image = images_client.get_from_family(project="debian-cloud", family="debian-11")

    instance = compute_v1.Instance()
    instance.name = "brainz-full-pipeline"
    instance.machine_type = f"zones/{zone}/machineTypes/{MACHINE_TYPE}"

    disk = compute_v1.AttachedDisk()
    disk.boot = True
    disk.auto_delete = True
    init = compute_v1.AttachedDiskInitializeParams()
    init.source_image = image.self_link
    init.disk_size_gb = 100
    init.disk_type = f"zones/{zone}/diskTypes/pd-ssd"
    disk.initialize_params = init
    instance.disks = [disk]

    nic = compute_v1.NetworkInterface()
    ac = compute_v1.AccessConfig()
    ac.name = "External NAT"
    ac.type_ = "ONE_TO_ONE_NAT"
    nic.access_configs = [ac]
    instance.network_interfaces = [nic]

    sa = compute_v1.ServiceAccount()
    sa.email = "default"
    sa.scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    instance.service_accounts = [sa]

    metadata = compute_v1.Metadata()
    item = compute_v1.Items()
    item.key = "startup-script"
    item.value = get_startup_script(gcs_raw_lb, gcs_processed)
    metadata.items = [item]
    instance.metadata = metadata

    instance.labels = {"project": "music-recommendation", "purpose": "full-pipeline"}
    instance.scheduling = compute_v1.Scheduling()
    instance.scheduling.on_host_maintenance = "TERMINATE"

    operation = instances_client.insert(project=project, zone=zone, instance_resource=instance)
    operation.result()

    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "pipeline_instance.json", "w") as f:
        json.dump({"instance_name": instance.name, "zone": zone, "project": project}, f, indent=2)

    return instance.name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", action="store_true")
    args = parser.parse_args()

    instances_client = compute_v1.InstancesClient()
    images_client    = compute_v1.ImagesClient()
    instance_name = launch(instances_client, images_client, GCP_PROJECT, GCP_ZONE,
                           GCS_RAW_LB, GCS_PROCESSED)

    print(f"\n✅ Instance lancée : {instance_name}")
    print("\nCommandes utiles :")
    print(f"  Logs   : gcloud compute instances get-serial-port-output {instance_name} --zone={GCP_ZONE}")
    print(f"  Statut : gcloud compute instances describe {instance_name} --zone={GCP_ZONE} --format='get(status)'")
    print(f"  Résultats: gsutil ls gs://{GCS_PROCESSED}/models/")

    if args.monitor:
        print(f"\nMonitoring de {instance_name}...")
        try:
            while True:
                inst = instances_client.get(project=GCP_PROJECT, zone=GCP_ZONE, instance=instance_name)
                print(f"\r[{time.strftime('%H:%M:%S')}] État: {inst.status}   ", end='', flush=True)
                if inst.status == "TERMINATED":
                    print("\n\n✅ Pipeline terminé !")
                    break
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n\nMonitoring arrêté.")


if __name__ == "__main__":
    main()
