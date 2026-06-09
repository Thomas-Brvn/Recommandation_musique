#!/usr/bin/env python3
"""
Lance une instance GCE qui télécharge les données directement vers GCS.
Avantages:
- Pas de téléchargement local
- Bande passante Google (très rapide)
- Transfert gratuit GCE → GCS dans la même région
"""

import sys
import json
import os
from pathlib import Path

from google.cloud import compute_v1

DEFAULT_MACHINE_TYPE = "e2-medium"  # 2 vCPU, 4 GB RAM — ~0.05€/h
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION  = os.getenv("GCP_REGION", "europe-north1")
GCP_ZONE    = f"{GCP_REGION}-a"

GITHUB_REPO = "https://github.com/Thomas-Brvn/Recommandation_musique.git"


def load_gcp_config() -> dict:
    config_file = Path("config/gcp_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return {
        "project_id":       GCP_PROJECT,
        "region":           GCP_REGION,
        "bucket_raw_lb":    os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz"),
        "bucket_raw_mb":    os.getenv("GCS_BUCKET_RAW_MB", "brainz-raw-musicbrainz"),
        "bucket_processed": os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed"),
    }


def create_startup_script(bucket_raw_lb: str, bucket_raw_mb: str,
                           download_mb: bool, download_lb: bool) -> str:
    script = f"""#!/bin/bash
exec > >(tee /var/log/startup-script.log) 2>&1
echo "=========================================="
echo "Début du téléchargement des données"
echo "Date: $(date)"
echo "=========================================="

apt-get update -qq
apt-get install -y wget python3-pip python3-venv google-cloud-cli -qq

mkdir -p /data/musicbrainz /data/listenbrainz
cd /data

BUCKET_RAW_LB="{bucket_raw_lb}"
BUCKET_RAW_MB="{bucket_raw_mb}"
MB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
LB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/"
MB_TABLES="artist recording release release-group"
"""

    if download_mb:
        script += r"""
echo "=========================================="
echo "Téléchargement MusicBrainz"
echo "=========================================="

MB_LATEST=$(curl -s "$MB_BASE_URL" | grep -o 'href="[0-9]*-[0-9]*/"' | tail -1 | cut -d'"' -f2)
MUSICBRAINZ_URL="${MB_BASE_URL}${MB_LATEST}"

for table in $MB_TABLES; do
    echo "Téléchargement de $table..."
    wget -q --show-progress -O "/data/musicbrainz/$table.tar.xz" "${MUSICBRAINZ_URL}${table}.tar.xz"
    if [ $? -eq 0 ]; then
        gsutil cp "/data/musicbrainz/$table.tar.xz" "gs://$BUCKET_RAW_MB/$table.tar.xz"
        rm "/data/musicbrainz/$table.tar.xz"
        echo "✓ $table uploadé"
    fi
done
"""

    if download_lb:
        script += r"""
echo "=========================================="
echo "Téléchargement ListenBrainz"
echo "=========================================="

LB_LATEST_DIR=$(curl -s "$LB_BASE_URL" | grep -o 'href="listenbrainz-dump-[0-9]*-[0-9]*-[0-9]*-full/"' | tail -1 | cut -d'"' -f2)
LISTENBRAINZ_URL="${LB_BASE_URL}${LB_LATEST_DIR}"
LATEST_DUMP=$(curl -s "$LISTENBRAINZ_URL" | grep -o 'href="listenbrainz-listens-dump-[^"]*\.tar\.zst"' | head -1 | cut -d'"' -f2)

if [ -n "$LATEST_DUMP" ]; then
    wget -q --show-progress -O "/data/listenbrainz/$LATEST_DUMP" "${LISTENBRAINZ_URL}${LATEST_DUMP}"
    if [ $? -eq 0 ]; then
        gsutil -o GSUtil:parallel_composite_upload_threshold=150M \
            cp "/data/listenbrainz/$LATEST_DUMP" "gs://$BUCKET_RAW_LB/$LATEST_DUMP"
        rm "/data/listenbrainz/$LATEST_DUMP"
        echo "✓ ListenBrainz uploadé"
    fi
fi
"""

    script += """
echo "COMPLETED $(date)" > /tmp/download-status
gsutil cp /tmp/download-status "gs://$BUCKET_RAW_LB/status/.download-completed"
echo "=========================================="
echo "Téléchargement terminé — $(date)"
echo "=========================================="
shutdown -h now
"""
    return script


def launch_instance(instances_client, images_client, machine_type: str,
                    startup_script: str, project: str, zone: str) -> str:
    image = images_client.get_from_family(project="debian-cloud", family="debian-11")

    instance = compute_v1.Instance()
    instance.name = "brainz-data-download"
    instance.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

    disk = compute_v1.AttachedDisk()
    disk.boot = True
    disk.auto_delete = True
    init = compute_v1.AttachedDiskInitializeParams()
    init.source_image = image.self_link
    init.disk_size_gb = 200
    init.disk_type = f"zones/{zone}/diskTypes/pd-balanced"
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
    item.value = startup_script
    metadata.items = [item]
    instance.metadata = metadata

    instance.labels = {"project": "music-recommendation", "purpose": "data-download"}

    operation = instances_client.insert(project=project, zone=zone, instance_resource=instance)
    operation.result()

    return instance.name


def main():
    config = load_gcp_config()
    project       = config["project_id"]
    zone          = f"{config['region']}-a"
    bucket_raw_lb = config["bucket_raw_lb"]
    bucket_raw_mb = config["bucket_raw_mb"]

    print("=" * 60)
    print("🚀 Téléchargement des données via GCE")
    print("=" * 60)
    print(f"Projet : {project}")
    print(f"Zone   : {zone}")
    print(f"Bucket LB : gs://{bucket_raw_lb}")
    print(f"Bucket MB : gs://{bucket_raw_mb}")

    print("\n📦 Que souhaitez-vous télécharger ?")
    print("  1. MusicBrainz uniquement (~7 GB, 15-30 min)")
    print("  2. ListenBrainz uniquement (~100 GB, 2-4h)")
    print("  3. Les deux (~107 GB, 2-4h)")
    choice = input("Votre choix (1/2/3): ").strip()

    download_mb = choice in ['1', '3']
    download_lb = choice in ['2', '3']
    if not download_mb and not download_lb:
        print("❌ Choix invalide")
        sys.exit(1)

    startup_script = create_startup_script(bucket_raw_lb, bucket_raw_mb, download_mb, download_lb)

    print(f"\n🚀 Lancement de l'instance {DEFAULT_MACHINE_TYPE}...")
    instances_client = compute_v1.InstancesClient()
    images_client    = compute_v1.ImagesClient()

    instance_name = launch_instance(instances_client, images_client, DEFAULT_MACHINE_TYPE,
                                    startup_script, project, zone)

    print(f"✅ Instance lancée : {instance_name}")
    print("\nCommandes utiles :")
    print(f"  Logs   : gcloud compute instances get-serial-port-output {instance_name} --zone={zone}")
    print(f"  Statut : gcloud compute instances describe {instance_name} --zone={zone} --format='get(status)'")
    print(f"  Monitoring : python scripts/monitor_gce_download.py {instance_name} {zone}")

    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "download_instance.json", "w") as f:
        json.dump({"instance_name": instance_name, "zone": zone, "project": project}, f, indent=2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)
