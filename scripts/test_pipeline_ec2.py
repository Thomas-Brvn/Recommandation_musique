#!/usr/bin/env python3
"""
Script de TEST du pipeline sur un petit échantillon.

Utilise:
- 2 dumps incrémentaux seulement (~200 MB)
- Instance t3.medium (pas de GPU, ~0.04€/h)
- Validation complète du process

Coût estimé: ~0.10-0.20€ total
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "brainz-data")

# Instance de test (Free Tier eligible avec assez de RAM)
TEST_INSTANCE_TYPE = "m7i-flex.large"  # 2 vCPU, 8 GB RAM, Free Tier eligible


def get_user_data_script(s3_bucket: str, max_dumps: int) -> str:
    """Génère le script user-data pour EC2."""
    return f'''#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1

echo "=========================================="
echo "TEST PIPELINE - ÉCHANTILLON RÉDUIT"
echo "=========================================="
date

# Variables
S3_BUCKET="{s3_bucket}"
WORK_DIR="/home/ec2-user/recommendation"
MAX_DUMPS={max_dumps}

# Créer le répertoire de travail
mkdir -p $WORK_DIR
cd $WORK_DIR

# Installer les dépendances système
echo "Installation des dépendances..."
sudo yum update -y
sudo yum install -y python3-pip zstd

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les packages Python
pip install --upgrade pip
pip install pandas pyarrow scipy zstandard numpy scikit-learn tqdm boto3
pip install implicit
pip install fastapi uvicorn pydantic

# Créer les répertoires
mkdir -p data/raw data/extracted data/processed models

# ==========================================
# ÉTAPE 1: Télécharger les dumps
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 1: Téléchargement de $MAX_DUMPS dumps (TEST)"
echo "=========================================="

cd data/raw
aws s3 ls s3://$S3_BUCKET/raw/listenbrainz/incrementals/ | head -$MAX_DUMPS | awk '{{print $4}}' > dumps_to_download.txt

while read filename; do
    if [ -n "$filename" ]; then
        echo "Téléchargement: $filename"
        aws s3 cp "s3://$S3_BUCKET/raw/listenbrainz/incrementals/$filename" .
    fi
done < dumps_to_download.txt

echo "Fichiers téléchargés:"
ls -lh *.tar.zst 2>/dev/null || echo "Aucun fichier"

# ==========================================
# ÉTAPE 2: Extraire les archives
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 2: Extraction des archives"
echo "=========================================="

for f in *.tar.zst; do
    if [ -f "$f" ]; then
        echo "Extraction de $f..."
        zstd -d "$f" -o "${{f%.zst}}" --force
        tar -xf "${{f%.zst}}" -C ../extracted/
        rm "${{f%.zst}}"
        rm "$f"
    fi
done

cd $WORK_DIR
echo "Fichiers extraits:"
find data/extracted -type f | head -20

# ==========================================
# ÉTAPE 3-6: Scripts Python inline
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 3: Parsing et traitement"
echo "=========================================="

python3 << 'PYTHON_SCRIPT'
import json
import pickle
import numpy as np
import pandas as pd
from scipy import sparse
from pathlib import Path
from datetime import datetime

print("=" * 50)
print("PARSING DES ÉCOUTES")
print("=" * 50)

extracted_dir = Path("data/extracted")
all_listens = []

# Trouver tous les fichiers
listen_files = [f for f in extracted_dir.rglob("*") if f.is_file() and f.stat().st_size > 0]
print(f"Fichiers trouvés: {{len(listen_files)}}")

for filepath in listen_files:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    track_meta = data.get('track_metadata', {{}})
                    listen = {{
                        'user_name': data.get('user_name'),
                        'listened_at': data.get('listened_at'),
                        'track_name': track_meta.get('track_name'),
                        'artist_name': track_meta.get('artist_name'),
                    }}
                    if listen['user_name'] and listen['track_name']:
                        all_listens.append(listen)
                except json.JSONDecodeError:
                    pass
    except Exception as e:
        print(f"Erreur {{filepath}}: {{e}}")

print(f"Total écoutes parsées: {{len(all_listens):,}}")

if not all_listens:
    print("ERREUR: Aucune écoute!")
    exit(1)

df = pd.DataFrame(all_listens)
df['listened_at'] = pd.to_datetime(df['listened_at'], unit='s', errors='coerce')
print(f"Users uniques: {{df['user_name'].nunique():,}}")
print(f"Tracks uniques: {{df['track_name'].nunique():,}}")

# ==========================================
print("\\n" + "=" * 50)
print("AGRÉGATION")
print("=" * 50)

df['track_key'] = df['artist_name'].fillna('Unknown') + ' - ' + df['track_name']

# Filtrer (seuils bas pour le test)
min_user, min_track = 2, 2

user_counts = df['user_name'].value_counts()
valid_users = user_counts[user_counts >= min_user].index
df = df[df['user_name'].isin(valid_users)]

track_counts = df['track_key'].value_counts()
valid_tracks = track_counts[track_counts >= min_track].index
df = df[df['track_key'].isin(valid_tracks)]

print(f"Après filtrage: {{len(df):,}} écoutes")

# Mappings
users = df['user_name'].unique()
tracks = df['track_key'].unique()

user_to_id = {{v: i for i, v in enumerate(users)}}
track_to_id = {{v: i for i, v in enumerate(tracks)}}
id_to_user = {{i: v for v, i in user_to_id.items()}}
id_to_track = {{i: v for v, i in track_to_id.items()}}

df['user_id'] = df['user_name'].map(user_to_id)
df['track_id'] = df['track_key'].map(track_to_id)

# Agréger
agg_df = df.groupby(['user_id', 'track_id']).size().reset_index(name='play_count')

print(f"Interactions: {{len(agg_df):,}}")
print(f"Users: {{len(users):,}}")
print(f"Tracks: {{len(tracks):,}}")

# Sauver mappings
Path("data/processed").mkdir(parents=True, exist_ok=True)
mappings = {{
    'user_to_id': user_to_id,
    'id_to_user': {{str(k): v for k, v in id_to_user.items()}},
    'track_to_id': track_to_id,
    'id_to_track': {{str(k): v for k, v in id_to_track.items()}},
}}
with open('data/processed/mappings.json', 'w') as f:
    json.dump(mappings, f)

# ==========================================
print("\\n" + "=" * 50)
print("CONSTRUCTION MATRICE SPARSE")
print("=" * 50)

n_users = len(users)
n_items = len(tracks)

alpha = 40.0
values = 1 + alpha * np.log1p(agg_df['play_count'].values)

matrix = sparse.csr_matrix(
    (values, (agg_df['user_id'].values, agg_df['track_id'].values)),
    shape=(n_users, n_items),
    dtype=np.float32
)

print(f"Dimensions: {{n_users}} users x {{n_items}} items")
print(f"Non-zeros: {{matrix.nnz:,}}")
print(f"Sparsité: {{(1 - matrix.nnz / (n_users * n_items)) * 100:.2f}}%")

sparse.save_npz('data/processed/user_item_matrix.npz', matrix)

# Split train/test
np.random.seed(42)
train = matrix.tolil()
test = sparse.lil_matrix(matrix.shape, dtype=matrix.dtype)

for user_id in range(matrix.shape[0]):
    items = matrix[user_id].indices
    if len(items) > 1:
        n_test = max(1, int(len(items) * 0.2))
        test_items = np.random.choice(items, size=n_test, replace=False)
        for item in test_items:
            test[user_id, item] = train[user_id, item]
            train[user_id, item] = 0

train = train.tocsr()
test = test.tocsr()
train.eliminate_zeros()
test.eliminate_zeros()

sparse.save_npz('data/processed/train_matrix.npz', train)
sparse.save_npz('data/processed/test_matrix.npz', test)
print(f"Train: {{train.nnz:,}} | Test: {{test.nnz:,}}")

# ==========================================
print("\\n" + "=" * 50)
print("ENTRAÎNEMENT ALS")
print("=" * 50)

from implicit.als import AlternatingLeastSquares

model = AlternatingLeastSquares(
    factors=64,
    regularization=0.01,
    iterations=10,
    use_gpu=False
)

print("Entraînement...")
model.fit(matrix.T.tocsr(), show_progress=True)

# Sauvegarder
Path("models").mkdir(parents=True, exist_ok=True)
state = {{
    'model': model,
    'factors': 64,
    'regularization': 0.01,
    'iterations': 10,
    'user_mapping': id_to_user,
    'item_mapping': id_to_track,
    'is_fitted': True
}}
with open('models/als_model.pkl', 'wb') as f:
    pickle.dump(state, f)

print("Modèle sauvegardé: models/als_model.pkl")

# Test rapide du modèle
print("\\nTest rapide du modèle...")
print(f"  User factors shape: {{model.user_factors.shape}}")
print(f"  Item factors shape: {{model.item_factors.shape}}")
print("  Modèle OK!")

print("\\n" + "=" * 50)
print("TRAITEMENT TERMINÉ!")
print("=" * 50)
PYTHON_SCRIPT

# ==========================================
# ÉTAPE 7: Upload vers S3
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 7: Upload vers S3"
echo "=========================================="

aws s3 cp models/ s3://$S3_BUCKET/test_models/ --recursive
aws s3 cp data/processed/ s3://$S3_BUCKET/test_processed/ --recursive

# Marquer comme terminé
echo "TEST COMPLETED $(date)" > /tmp/test_completed
aws s3 cp /tmp/test_completed s3://$S3_BUCKET/status/test_pipeline_completed

echo ""
echo "=========================================="
echo "TEST PIPELINE TERMINÉ AVEC SUCCÈS!"
echo "=========================================="
date

echo ""
echo "Résumé:"
ls -lh data/processed/
ls -lh models/
'''


def get_or_create_iam_role(iam_client) -> str:
    """Crée ou récupère le rôle IAM pour EC2."""
    role_name = "EC2-S3-Access-Role"
    instance_profile_name = "EC2-S3-Access-Profile"

    try:
        iam_client.get_role(RoleName=role_name)
        print(f"Rôle IAM existant: {role_name}")
    except ClientError:
        print(f"Création du rôle IAM: {role_name}")
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
            Description="Role for EC2 S3 access"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

    try:
        iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError:
        print(f"Création du profil: {instance_profile_name}")
        iam_client.create_instance_profile(InstanceProfileName=instance_profile_name)
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        time.sleep(10)

    return instance_profile_name


def get_amazon_linux_ami(ec2_client) -> str:
    """Récupère la dernière AMI Amazon Linux 2."""
    response = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
    return images[0]['ImageId']


def launch_test_instance(ec2_client, iam_client, max_dumps: int = 2) -> str:
    """Lance une instance EC2 de test."""

    print("=" * 60)
    print("LANCEMENT DU TEST PIPELINE")
    print("=" * 60)
    print(f"Dumps à traiter: {max_dumps}")
    print(f"Instance: {TEST_INSTANCE_TYPE}")
    print(f"Coût estimé: ~0.10-0.20€")
    print("=" * 60)

    # Rôle IAM
    instance_profile = get_or_create_iam_role(iam_client)

    # AMI
    ami_id = get_amazon_linux_ami(ec2_client)
    print(f"AMI: {ami_id}")

    # User data
    user_data = get_user_data_script(S3_BUCKET, max_dumps)

    # Lancer l'instance
    print(f"\nLancement de l'instance...")

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=TEST_INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={'Name': instance_profile},
        UserData=user_data,
        BlockDeviceMappings=[{
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'VolumeSize': 30,
                'VolumeType': 'gp3',
                'DeleteOnTermination': True
            }
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': 'TEST-Recommendation-Pipeline'},
                {'Key': 'Project', 'Value': 'MusicRecommendation-TEST'}
            ]
        }],
        InstanceInitiatedShutdownBehavior='terminate'
    )

    instance_id = response['Instances'][0]['InstanceId']
    print(f"Instance lancée: {instance_id}")

    return instance_id


def monitor_test(ec2_client, s3_client, instance_id: str):
    """Surveille le test."""
    print("\n" + "=" * 60)
    print("MONITORING DU TEST")
    print("=" * 60)
    print("Le test devrait prendre ~15-30 minutes")
    print("Ctrl+C pour arrêter le monitoring (l'instance continue)")
    print("")

    start_time = time.time()

    try:
        while True:
            elapsed = int(time.time() - start_time)
            mins, secs = divmod(elapsed, 60)

            # Status instance
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']

            # Vérifier si terminé
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key='status/test_pipeline_completed')
                print(f"\n\n{'=' * 60}")
                print("✅ TEST TERMINÉ AVEC SUCCÈS!")
                print(f"{'=' * 60}")
                print(f"Durée: {mins}m {secs}s")
                print(f"\nRésultats sur S3:")
                print(f"  Modèle: s3://{S3_BUCKET}/test_models/")
                print(f"  Données: s3://{S3_BUCKET}/test_processed/")
                print(f"\n⚠️  IMPORTANT - Terminer l'instance:")
                print(f"  python scripts/test_pipeline_ec2.py --terminate {instance_id}")
                return True
            except ClientError:
                pass

            if state == 'terminated':
                print(f"\n❌ Instance terminée prématurément!")
                return False

            print(f"[{mins:02d}:{secs:02d}] Instance: {state:<12}", end='\r')
            time.sleep(10)

    except KeyboardInterrupt:
        print(f"\n\nMonitoring arrêté après {mins}m {secs}s")
        print(f"L'instance {instance_id} continue de tourner.")
        print(f"\nPour voir les logs:")
        print(f"  python scripts/test_pipeline_ec2.py --logs {instance_id}")
        print(f"\nPour terminer:")
        print(f"  python scripts/test_pipeline_ec2.py --terminate {instance_id}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Tester le pipeline sur un échantillon")
    parser.add_argument("--dumps", type=int, default=2,
                       help="Nombre de dumps à utiliser (défaut: 2)")
    parser.add_argument("--no-monitor", action="store_true",
                       help="Ne pas surveiller (lancer et quitter)")
    parser.add_argument("--terminate", metavar="INSTANCE_ID",
                       help="Terminer une instance")
    parser.add_argument("--logs", metavar="INSTANCE_ID",
                       help="Voir les logs d'une instance")

    args = parser.parse_args()

    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    iam_client = boto3.client('iam')
    s3_client = boto3.client('s3', region_name=AWS_REGION)

    if args.terminate:
        print(f"Termination de {args.terminate}...")
        ec2_client.terminate_instances(InstanceIds=[args.terminate])
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key='status/test_pipeline_completed')
        except:
            pass
        print("Instance terminée.")
        return

    if args.logs:
        print(f"Logs de {args.logs}:\n")
        try:
            response = ec2_client.get_console_output(InstanceId=args.logs)
            if 'Output' in response and response['Output']:
                print(response['Output'])
            else:
                print("Pas encore de logs disponibles (attendre 2-3 min après le lancement)")
        except Exception as e:
            print(f"Erreur: {e}")
        return

    # Lancer le test
    instance_id = launch_test_instance(ec2_client, iam_client, max_dumps=args.dumps)

    print(f"\n{'=' * 60}")
    print("INSTANCE DE TEST LANCÉE")
    print(f"{'=' * 60}")
    print(f"Instance ID: {instance_id}")
    print(f"Région: {AWS_REGION}")
    print(f"Dumps: {args.dumps}")
    print("")
    print("Commandes utiles:")
    print(f"  Logs:     python scripts/test_pipeline_ec2.py --logs {instance_id}")
    print(f"  Terminer: python scripts/test_pipeline_ec2.py --terminate {instance_id}")

    if not args.no_monitor:
        print("")
        monitor_test(ec2_client, s3_client, instance_id)


if __name__ == "__main__":
    main()
