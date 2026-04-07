#!/usr/bin/env python3
"""
Lance une instance EC2 qui télécharge les dumps ListenBrainz manquants vers S3.

- Instance t3.small (~0.02$/h) — réseau uniquement, pas de calcul
- Clone le repo GitHub, lance download_incrementals.py
- Se termine automatiquement à la fin
- Coût typique : < 0.05$ par run

Usage:
    python scripts/launch_download_ec2.py
    python scripts/launch_download_ec2.py --monitor
"""
import os
import json
import time
import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

AWS_REGION    = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET     = os.getenv("S3_BUCKET_NAME", "brainz-data")
INSTANCE_TYPE = "t3.small"
GITHUB_REPO   = "https://github.com/Thomas-Brvn/Recommandation_musique.git"

USER_DATA = f"""#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1

echo "================================"
echo "TÉLÉCHARGEMENT DUMPS LISTENBRAINZ"
echo "================================"
date

# Variables
S3_BUCKET="{S3_BUCKET}"
WORK_DIR="/home/ec2-user/recommendation"

mkdir -p $WORK_DIR
cd $WORK_DIR

# Dépendances
echo "Installation des dépendances..."
sudo yum update -y -q
sudo yum install -y git python3-pip -q

# Cloner le repo
echo "Clonage du repo..."
git clone {GITHUB_REPO} .

# Environnement virtuel
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install boto3 requests tqdm -q

# Lancer le téléchargement
echo "Lancement du téléchargement..."
S3_BUCKET_NAME=$S3_BUCKET python scripts/download_incrementals.py

echo "================================"
echo "TERMINÉ"
echo "================================"
date

# Marquer comme terminé dans S3
echo "COMPLETED $(date)" > /tmp/download_completed
aws s3 cp /tmp/download_completed s3://$S3_BUCKET/status/download_completed --region {AWS_REGION}

# Arrêt automatique
sudo shutdown -h now
"""


def get_or_create_iam_role(iam_client) -> str:
    role_name            = "EC2-ML-Pipeline-Role"
    instance_profile_name = "EC2-ML-Pipeline-Profile"

    try:
        iam_client.get_role(RoleName=role_name)
    except ClientError:
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                           "Principal": {"Service": "ec2.amazonaws.com"},
                           "Action": "sts:AssumeRole"}]
        }
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for EC2 S3 access"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

    try:
        iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError:
        iam_client.create_instance_profile(InstanceProfileName=instance_profile_name)
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        time.sleep(10)

    return instance_profile_name


def get_amazon_linux_ami(ec2_client) -> str:
    response = ec2_client.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name",            "Values": ["al2023-ami-*-x86_64"]},
            {"Name": "state",           "Values": ["available"]},
            {"Name": "architecture",    "Values": ["x86_64"]},
        ]
    )
    images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
    return images[0]["ImageId"]


def launch(ec2_client, iam_client) -> str:
    print("=" * 50)
    print("LANCEMENT EC2 - TÉLÉCHARGEMENT INCRÉMENTAUX")
    print("=" * 50)
    print(f"Instance  : {INSTANCE_TYPE}")
    print(f"Bucket S3 : {S3_BUCKET}")
    print(f"Région    : {AWS_REGION}")
    print(f"Coût estimé : < 0.05$")
    print("=" * 50)

    instance_profile = get_or_create_iam_role(iam_client)
    ami_id = get_amazon_linux_ami(ec2_client)
    print(f"AMI : {ami_id}")

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=INSTANCE_TYPE,
        MinCount=1, MaxCount=1,
        IamInstanceProfile={"Name": instance_profile},
        UserData=USER_DATA,
        BlockDeviceMappings=[{
            "DeviceName": "/dev/xvda",
            "Ebs": {"VolumeSize": 30, "VolumeType": "gp3", "DeleteOnTermination": True}
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name",    "Value": "LB-Download-Incrementals"},
                {"Key": "Project", "Value": "MusicRecommendation"},
            ]
        }],
        InstanceInitiatedShutdownBehavior="terminate",
    )

    instance_id = response["Instances"][0]["InstanceId"]
    print(f"\nInstance lancée : {instance_id}")

    # Sauvegarder l'ID
    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "download_instance.json", "w") as f:
        json.dump({"instance_id": instance_id, "region": AWS_REGION, "bucket": S3_BUCKET}, f, indent=2)

    return instance_id


def monitor(ec2_client, s3_client, instance_id: str):
    print(f"\nMonitoring de {instance_id} (Ctrl+C pour arrêter)...")
    print(f"Logs : aws ec2 get-console-output --instance-id {instance_id} --region {AWS_REGION}\n")

    try:
        while True:
            resp  = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]

            # Vérifier si terminé
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key="status/download_completed")
                print("\nTéléchargement terminé !")
                print(f"Vérifier S3 : aws s3 ls s3://{S3_BUCKET}/raw/listenbrainz/incrementals/ --region {AWS_REGION}")
                return
            except ClientError:
                pass

            print(f"[{time.strftime('%H:%M:%S')}] Instance : {state}   ", end="\r")

            if state == "terminated":
                print("\nInstance terminée.")
                return

            time.sleep(30)

    except KeyboardInterrupt:
        print(f"\n\nMonitoring arrêté. Instance {instance_id} continue en arrière-plan.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", action="store_true", help="Surveiller l'avancement")
    args = parser.parse_args()

    ec2_client = boto3.client("ec2", region_name=AWS_REGION)
    iam_client = boto3.client("iam", region_name=AWS_REGION)
    s3_client  = boto3.client("s3",  region_name=AWS_REGION)

    instance_id = launch(ec2_client, iam_client)

    print(f"\nCommandes utiles :")
    print(f"  Logs    : aws ec2 get-console-output --instance-id {instance_id} --region {AWS_REGION}")
    print(f"  Statut  : aws ec2 describe-instances --instance-ids {instance_id} --region {AWS_REGION} --query 'Reservations[0].Instances[0].State.Name'")
    print(f"  Résultat: aws s3 ls s3://{S3_BUCKET}/raw/listenbrainz/incrementals/ --region {AWS_REGION} | tail -5")

    if args.monitor:
        monitor(ec2_client, s3_client, instance_id)


if __name__ == "__main__":
    main()
