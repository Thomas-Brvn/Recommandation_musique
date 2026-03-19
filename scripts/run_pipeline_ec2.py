#!/usr/bin/env python3
"""
Script pour lancer le pipeline de recommandation sur EC2 avec GPU.

Ce script:
1. Lance une instance EC2 g4dn.xlarge (GPU NVIDIA T4)
2. Installe les dépendances
3. Télécharge les données depuis S3
4. Exécute le pipeline complet (extraction → training → évaluation)
5. Upload le modèle entraîné vers S3
6. Termine l'instance automatiquement

Coût estimé: ~0.53€/h × 2-4h = ~1-2€ total
"""
import os
import sys
import json
import time
import base64
import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "listen-brainz-data")

# Instance EC2
INSTANCE_TYPE = "g4dn.xlarge"  # 4 vCPU, 16 GB RAM, 1x NVIDIA T4 GPU
# Alternative CPU only: "r5.xlarge" (4 vCPU, 32 GB RAM, ~0.25€/h)

# AMI Deep Learning avec CUDA pré-installé (Amazon Linux 2)
# Ces AMIs ont déjà CUDA, cuDNN, et Python installés
DEEP_LEARNING_AMIS = {
    "eu-north-1": "ami-0c1a7f89451184c8b",  # Deep Learning AMI GPU PyTorch
    "eu-west-1": "ami-0c1a7f89451184c8b",
    "eu-west-3": "ami-0c1a7f89451184c8b",
    "us-east-1": "ami-0c1a7f89451184c8b",
}

# Script de démarrage (user-data)
USER_DATA_SCRIPT = """#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1

echo "=========================================="
echo "DÉMARRAGE DU PIPELINE DE RECOMMANDATION"
echo "=========================================="
date

# Variables
S3_BUCKET="{s3_bucket}"
WORK_DIR="/home/ec2-user/recommendation"

# Créer le répertoire de travail
mkdir -p $WORK_DIR
cd $WORK_DIR

# Installer les dépendances système
echo "Installation des dépendances système..."
sudo yum install -y python3-pip git zstd

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les packages Python
echo "Installation des packages Python..."
pip install --upgrade pip
pip install pandas pyarrow scipy zstandard numpy scikit-learn tqdm boto3
pip install implicit  # Supporte GPU automatiquement si CUDA disponible
pip install fastapi uvicorn pydantic

# Télécharger le code depuis S3
echo "Téléchargement du code..."
aws s3 cp s3://$S3_BUCKET/code/scripts/ scripts/ --recursive
aws s3 cp s3://$S3_BUCKET/code/src/ src/ --recursive

# Créer les répertoires
mkdir -p data/raw/listenbrainz data/extracted data/processed models

# ==========================================
# ÉTAPE 1: Télécharger et extraire les données
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 1: Extraction des données"
echo "=========================================="

# Télécharger les dumps ListenBrainz
echo "Téléchargement des dumps depuis S3..."
aws s3 cp s3://$S3_BUCKET/raw/listenbrainz/incrementals/ data/raw/listenbrainz/ --recursive

# Extraire les archives
echo "Extraction des archives .tar.zst..."
cd data/raw/listenbrainz
for f in *.tar.zst; do
    if [ -f "$f" ]; then
        echo "Extraction de $f..."
        zstd -d "$f" -o "${{f%.zst}}"
        tar -xf "${{f%.zst}}" -C ../../extracted/
        rm "${{f%.zst}}"  # Supprimer le .tar intermédiaire
    fi
done
cd $WORK_DIR

# ==========================================
# ÉTAPE 2: Parser les écoutes
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 2: Parsing des écoutes"
echo "=========================================="

python scripts/parse_listens.py --input data/extracted --output data/processed

# ==========================================
# ÉTAPE 3: Agréger les données
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 3: Agrégation des données"
echo "=========================================="

python scripts/aggregate_data.py \
    --min-user-listens 5 \
    --min-track-listens 3

# ==========================================
# ÉTAPE 4: Construire la matrice
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 4: Construction de la matrice sparse"
echo "=========================================="

python scripts/build_matrix.py --split --test-ratio 0.2

# ==========================================
# ÉTAPE 5: Entraîner le modèle
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 5: Entraînement du modèle ALS"
echo "=========================================="

cd src
python train.py \
    --factors 128 \
    --regularization 0.01 \
    --iterations 20 \
    --gpu

# ==========================================
# ÉTAPE 6: Évaluer le modèle
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 6: Évaluation du modèle"
echo "=========================================="

python evaluate.py \
    --sample 5000 \
    --output ../models/evaluation_results.json

cd $WORK_DIR

# ==========================================
# ÉTAPE 7: Upload des résultats vers S3
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 7: Upload vers S3"
echo "=========================================="

# Upload le modèle
aws s3 cp models/ s3://$S3_BUCKET/models/ --recursive

# Upload les données transformées (pour l'API)
aws s3 cp data/processed/user_item_matrix.npz s3://$S3_BUCKET/processed/
aws s3 cp data/processed/mappings.json s3://$S3_BUCKET/processed/
aws s3 cp data/processed/user_mapping.json s3://$S3_BUCKET/processed/
aws s3 cp data/processed/item_mapping.json s3://$S3_BUCKET/processed/

# Marquer comme terminé
echo "COMPLETED" > /tmp/pipeline_completed
aws s3 cp /tmp/pipeline_completed s3://$S3_BUCKET/status/pipeline_completed

echo ""
echo "=========================================="
echo "PIPELINE TERMINÉ AVEC SUCCÈS!"
echo "=========================================="
echo "Modèle uploadé: s3://$S3_BUCKET/models/als_model.pkl"
echo "Résultats: s3://$S3_BUCKET/models/evaluation_results.json"
date

# Optionnel: arrêter l'instance automatiquement
# sudo shutdown -h now
"""


def get_or_create_iam_role(iam_client) -> str:
    """Crée ou récupère le rôle IAM pour EC2."""
    role_name = "EC2-ML-Pipeline-Role"
    instance_profile_name = "EC2-ML-Pipeline-Profile"

    try:
        iam_client.get_role(RoleName=role_name)
        print(f"Rôle IAM existant: {role_name}")
    except ClientError:
        print(f"Création du rôle IAM: {role_name}")

        # Politique de confiance pour EC2
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }

        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for ML pipeline on EC2"
        )

        # Attacher les politiques nécessaires
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

    # Créer le profil d'instance si nécessaire
    try:
        iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError:
        print(f"Création du profil d'instance: {instance_profile_name}")
        iam_client.create_instance_profile(InstanceProfileName=instance_profile_name)
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        time.sleep(10)  # Attendre la propagation

    return instance_profile_name


def get_latest_deep_learning_ami(ec2_client, region: str) -> str:
    """Récupère la dernière AMI Deep Learning."""
    # Chercher l'AMI Deep Learning Amazon Linux 2 avec GPU
    response = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            {'Name': 'name', 'Values': ['Deep Learning AMI GPU PyTorch*Amazon Linux 2*']},
            {'Name': 'state', 'Values': ['available']},
            {'Name': 'architecture', 'Values': ['x86_64']}
        ]
    )

    if response['Images']:
        # Trier par date de création et prendre la plus récente
        images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
        ami_id = images[0]['ImageId']
        print(f"AMI Deep Learning trouvée: {ami_id}")
        return ami_id

    # Fallback sur Amazon Linux 2 standard
    print("AMI Deep Learning non trouvée, utilisation d'Amazon Linux 2 standard")
    response = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
    return images[0]['ImageId']


def upload_code_to_s3(s3_client, bucket: str):
    """Upload le code source vers S3."""
    print("Upload du code vers S3...")

    base_dir = Path(__file__).parent.parent

    # Scripts
    scripts_dir = base_dir / "scripts"
    for script in ['extract_incrementals.py', 'parse_listens.py', 'aggregate_data.py', 'build_matrix.py']:
        script_path = scripts_dir / script
        if script_path.exists():
            s3_client.upload_file(str(script_path), bucket, f"code/scripts/{script}")
            print(f"  Uploadé: {script}")

    # Source
    src_dir = base_dir / "src"
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if file.endswith('.py'):
                local_path = Path(root) / file
                relative_path = local_path.relative_to(base_dir)
                s3_key = f"code/{relative_path}"
                s3_client.upload_file(str(local_path), bucket, s3_key)
                print(f"  Uploadé: {relative_path}")


def launch_ec2_instance(
    ec2_client,
    iam_client,
    s3_client,
    instance_type: str = INSTANCE_TYPE,
    use_gpu: bool = True
) -> str:
    """Lance une instance EC2 pour le pipeline."""

    print("=" * 60)
    print("LANCEMENT DU PIPELINE EC2")
    print("=" * 60)

    # Upload du code
    upload_code_to_s3(s3_client, S3_BUCKET)

    # Rôle IAM
    instance_profile = get_or_create_iam_role(iam_client)

    # AMI
    ami_id = get_latest_deep_learning_ami(ec2_client, AWS_REGION)

    # User data
    user_data = USER_DATA_SCRIPT.format(s3_bucket=S3_BUCKET)
    user_data_encoded = base64.b64encode(user_data.encode()).decode()

    # Lancer l'instance
    print(f"\nLancement de l'instance {instance_type}...")

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={'Name': instance_profile},
        UserData=user_data,
        BlockDeviceMappings=[{
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'VolumeSize': 200,  # 200 GB pour les données
                'VolumeType': 'gp3',
                'DeleteOnTermination': True
            }
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': 'ML-Recommendation-Pipeline'},
                {'Key': 'Project', 'Value': 'MusicRecommendation'}
            ]
        }]
    )

    instance_id = response['Instances'][0]['InstanceId']
    print(f"Instance lancée: {instance_id}")

    # Sauvegarder l'ID
    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    with open(config_dir / "pipeline_instance.json", 'w') as f:
        json.dump({
            'instance_id': instance_id,
            'instance_type': instance_type,
            'region': AWS_REGION,
            'bucket': S3_BUCKET
        }, f, indent=2)

    return instance_id


def monitor_pipeline(ec2_client, s3_client, instance_id: str):
    """Surveille l'avancement du pipeline."""
    print("\n" + "=" * 60)
    print("MONITORING DU PIPELINE")
    print("=" * 60)
    print("Appuyez sur Ctrl+C pour arrêter le monitoring")
    print("")

    try:
        while True:
            # Vérifier le statut de l'instance
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']

            # Vérifier si le pipeline est terminé
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key='status/pipeline_completed')
                print("\n✅ PIPELINE TERMINÉ!")
                print(f"Modèle disponible: s3://{S3_BUCKET}/models/als_model.pkl")
                print(f"Résultats: s3://{S3_BUCKET}/models/evaluation_results.json")
                return True
            except ClientError:
                pass

            print(f"[{time.strftime('%H:%M:%S')}] Instance: {state}", end='\r')
            time.sleep(30)

    except KeyboardInterrupt:
        print("\n\nMonitoring arrêté.")
        print(f"L'instance {instance_id} continue de tourner.")
        print(f"Pour voir les logs: aws ec2 get-console-output --instance-id {instance_id}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Lancer le pipeline ML sur EC2")
    parser.add_argument("--instance-type", default=INSTANCE_TYPE,
                       help=f"Type d'instance EC2 (défaut: {INSTANCE_TYPE})")
    parser.add_argument("--no-gpu", action="store_true",
                       help="Utiliser une instance CPU only (r5.xlarge)")
    parser.add_argument("--monitor", action="store_true",
                       help="Surveiller l'avancement du pipeline")
    parser.add_argument("--terminate", metavar="INSTANCE_ID",
                       help="Terminer une instance")

    args = parser.parse_args()

    # Clients AWS
    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    iam_client = boto3.client('iam', region_name=AWS_REGION)
    s3_client = boto3.client('s3', region_name=AWS_REGION)

    if args.terminate:
        print(f"Termination de l'instance {args.terminate}...")
        ec2_client.terminate_instances(InstanceIds=[args.terminate])
        print("Instance terminée.")
        return

    # Type d'instance
    instance_type = "r5.xlarge" if args.no_gpu else args.instance_type

    # Lancer l'instance
    instance_id = launch_ec2_instance(
        ec2_client, iam_client, s3_client,
        instance_type=instance_type
    )

    print("\n" + "=" * 60)
    print("INSTANCE LANCÉE")
    print("=" * 60)
    print(f"Instance ID: {instance_id}")
    print(f"Type: {instance_type}")
    print(f"Région: {AWS_REGION}")
    print(f"Bucket S3: {S3_BUCKET}")
    print("")
    print("Commandes utiles:")
    print(f"  Logs: aws ec2 get-console-output --instance-id {instance_id} --region {AWS_REGION}")
    print(f"  Stop: aws ec2 stop-instances --instance-ids {instance_id} --region {AWS_REGION}")
    print(f"  Terminer: aws ec2 terminate-instances --instance-ids {instance_id} --region {AWS_REGION}")
    print("")
    print(f"⚠️  N'oublie pas de terminer l'instance après usage!")
    print(f"    Coût estimé: ~0.53€/h (GPU) ou ~0.25€/h (CPU)")

    if args.monitor:
        monitor_pipeline(ec2_client, s3_client, instance_id)


if __name__ == "__main__":
    main()
