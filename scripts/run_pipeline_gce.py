#!/usr/bin/env python3
"""
Lance le pipeline de recommandation avec GPU sur GCE.

- Instance n1-standard-4 + NVIDIA T4 GPU
- Équivalent de g4dn.xlarge sur AWS
- Coût estimé : ~0.53€/h × 2-4h = ~1-2€ total

Usage:
    python scripts/run_pipeline_gce.py
    python scripts/run_pipeline_gce.py --cpu-only  # sans GPU
"""
import os
import json
import time
import argparse
from pathlib import Path

from google.cloud import compute_v1

GCP_PROJECT   = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION    = os.getenv("GCP_REGION", "europe-north1")
GCP_ZONE      = f"{GCP_REGION}-a"
GCS_RAW_LB    = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
GCS_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed")
GPU_MACHINE   = "n1-standard-4"   # 4 vCPU, 15 GB RAM + T4
CPU_MACHINE   = "n2-highmem-4"    # 4 vCPU, 32 GB RAM (sans GPU)
GITHUB_REPO   = "https://github.com/Thomas-Brvn/Recommandation_musique.git"


def get_startup_script(gcs_raw_lb: str, gcs_processed: str) -> str:
    return f"""#!/bin/bash
set -e
exec > >(tee /var/log/startup-script.log) 2>&1

echo "=========================================="
echo "DÉMARRAGE DU PIPELINE DE RECOMMANDATION"
echo "=========================================="
date

GCS_RAW_LB="{gcs_raw_lb}"
GCS_PROCESSED="{gcs_processed}"
WORK_DIR="/home/debian/recommendation"

mkdir -p $WORK_DIR
cd $WORK_DIR

apt-get update -qq
apt-get install -y python3-pip python3-venv git zstd google-cloud-cli -qq

git clone {GITHUB_REPO} .
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip -q
pip install pandas pyarrow scipy zstandard numpy scikit-learn tqdm google-cloud-storage implicit -q

mkdir -p data/work data/processed models

# Télécharger les données
gsutil -m cp "gs://$GCS_RAW_LB/incrementals/*.tar.zst" data/work/ 2>/dev/null || true

# Pipeline
python Script.py --input data/work/ --output data/processed/ --model models/

# Upload résultats
gsutil -m cp -r data/processed/ gs://$GCS_PROCESSED/processed/
gsutil -m cp -r models/ gs://$GCS_PROCESSED/models/
echo "COMPLETED $(date)" | gsutil cp - gs://$GCS_PROCESSED/status/full_pipeline_completed

echo "TERMINÉ — $(date)"
shutdown -h now
"""


def launch(instances_client, images_client, use_gpu: bool,
           project: str, zone: str, gcs_raw_lb: str, gcs_processed: str) -> str:
    machine_type = GPU_MACHINE if use_gpu else CPU_MACHINE
    print(f"Machine : {machine_type} {'+ NVIDIA T4' if use_gpu else ''}")

    image = images_client.get_from_family(
        project="deeplearning-platform-release" if use_gpu else "debian-cloud",
        family="common-cu121-debian-11-py310" if use_gpu else "debian-11"
    )

    instance = compute_v1.Instance()
    instance.name = "brainz-ml-pipeline"
    instance.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

    disk = compute_v1.AttachedDisk()
    disk.boot = True
    disk.auto_delete = True
    init = compute_v1.AttachedDiskInitializeParams()
    init.source_image = image.self_link
    init.disk_size_gb = 100
    init.disk_type = f"zones/{zone}/diskTypes/pd-ssd"
    disk.initialize_params = init
    instance.disks = [disk]

    if use_gpu:
        gpu = compute_v1.AcceleratorConfig()
        gpu.accelerator_count = 1
        gpu.accelerator_type = f"zones/{zone}/acceleratorTypes/nvidia-tesla-t4"
        instance.guest_accelerators = [gpu]

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

    instance.labels = {"project": "music-recommendation", "purpose": "ml-pipeline"}
    sched = compute_v1.Scheduling()
    sched.on_host_maintenance = "TERMINATE"
    instance.scheduling = sched

    operation = instances_client.insert(project=project, zone=zone, instance_resource=instance)
    operation.result()

    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "pipeline_instance.json", "w") as f:
        json.dump({"instance_name": instance.name, "zone": zone, "project": project}, f, indent=2)

    return instance.name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpu-only", action="store_true", help="Utiliser CPU uniquement (sans GPU)")
    parser.add_argument("--monitor",  action="store_true")
    args = parser.parse_args()

    instances_client = compute_v1.InstancesClient()
    images_client    = compute_v1.ImagesClient()

    instance_name = launch(instances_client, images_client, not args.cpu_only,
                           GCP_PROJECT, GCP_ZONE, GCS_RAW_LB, GCS_PROCESSED)

    print(f"\n✅ Instance lancée : {instance_name}")
    print(f"  Logs : gcloud compute instances get-serial-port-output {instance_name} --zone={GCP_ZONE}")

    if args.monitor:
        try:
            while True:
                inst = instances_client.get(project=GCP_PROJECT, zone=GCP_ZONE, instance=instance_name)
                print(f"\r[{time.strftime('%H:%M:%S')}] {inst.status}   ", end='', flush=True)
                if inst.status == "TERMINATED":
                    print("\n✅ Terminé !")
                    break
                time.sleep(30)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
