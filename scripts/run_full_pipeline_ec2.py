#!/usr/bin/env python3
"""
Script pour lancer le pipeline COMPLET de recommandation sur EC2.

Utilise:
- TOUS les dumps incrémentaux (~30 fichiers, ~3.6 GB)
- Instance r5.large (2 vCPU, 16 GB RAM) - assez pour 85M d'écoutes
- Paramètres de production (128 facteurs, 15 itérations)

Coût estimé: ~0.10€/h × 2-4h = ~0.40€ total
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

# Instance pour le pipeline complet (Free Tier eligible)
INSTANCE_TYPE = "m7i-flex.large"  # 2 vCPU, 8 GB RAM (free-tier eligible)


def get_user_data_script(s3_bucket: str) -> str:
    """Génère le script user-data pour le pipeline complet."""
    return f'''#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1

echo "=========================================="
echo "PIPELINE COMPLET - RECOMMANDATION MUSICALE"
echo "=========================================="
date

# Variables
S3_BUCKET="{s3_bucket}"
WORK_DIR="/home/ec2-user/recommendation"

# Créer le répertoire de travail
mkdir -p $WORK_DIR
cd $WORK_DIR

# Ajouter du swap (16 GB) pour compenser la RAM limitée
echo "Configuration du swap (16 GB)..."
sudo fallocate -l 16G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
free -h

# Installer les dépendances système
echo "Installation des dépendances..."
sudo yum update -y
sudo yum install -y python3-pip git zstd

# Cloner le code depuis GitHub
echo "Clonage du repo GitHub..."
git clone https://github.com/Thomas-Brvn/Recommandation_musique.git .

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les packages Python
pip install --upgrade pip
pip install pandas pyarrow scipy zstandard numpy scikit-learn tqdm boto3
pip install implicit
pip install fastapi uvicorn pydantic

# Créer les répertoires
mkdir -p data/work data/processed models

# ==========================================
# ÉTAPE 1-3: Télécharger, extraire et parser chaque dump un par un
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 1-3: Traitement fichier par fichier"
echo "=========================================="

# Lister les dumps
aws s3 ls s3://$S3_BUCKET/raw/listenbrainz/incrementals/ | awk '{{print $4}}' | sort > /tmp/dumps_list.txt
TOTAL_DUMPS=$(wc -l < /tmp/dumps_list.txt)
echo "Dumps à traiter: $TOTAL_DUMPS"

# Parser chaque dump et accumuler les données dans un fichier CSV intermédiaire
COUNT=0
while read filename; do
    if [ -n "$filename" ]; then
        COUNT=$((COUNT + 1))
        echo ""
        echo "[$COUNT/$TOTAL_DUMPS] $filename"
        echo "  Téléchargement..."
        aws s3 cp "s3://$S3_BUCKET/raw/listenbrainz/incrementals/$filename" data/work/current.tar.zst --no-progress

        echo "  Extraction..."
        mkdir -p data/work/extracted
        zstd -d data/work/current.tar.zst -o data/work/current.tar --force -q
        tar -xf data/work/current.tar -C data/work/extracted/
        rm data/work/current.tar data/work/current.tar.zst

        echo "  Parsing et agrégation..."
        python3 << PARSE_SCRIPT
import json, csv
from pathlib import Path

extracted_dir = Path("data/work/extracted")
out_file = "data/work/listens_chunk.csv"

count = 0
with open(out_file, 'w', newline='', encoding='utf-8') as out:
    writer = csv.writer(out)
    for filepath in extracted_dir.rglob("*"):
        if not filepath.is_file() or filepath.stat().st_size == 0:
            continue
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        user = data.get('user_name')
                        meta = data.get('track_metadata', {{}})
                        track = meta.get('track_name')
                        artist = meta.get('artist_name', 'Unknown')
                        if user and track:
                            writer.writerow([user, f"{{artist}} - {{track}}"])
                            count += 1
                    except:
                        pass
        except Exception as e:
            print(f"Erreur {{filepath}}: {{e}}")

print(f"  {{count:,}} écoutes parsées depuis ce dump")
PARSE_SCRIPT

        # Ajouter au fichier global
        cat data/work/listens_chunk.csv >> data/work/all_listens.csv
        rm data/work/listens_chunk.csv

        # Nettoyer le dossier extrait
        rm -rf data/work/extracted/*

        echo "  Disque utilisé: $(du -sh data/work/ | cut -f1)"
    fi
done < /tmp/dumps_list.txt

echo ""
echo "Total lignes dans all_listens.csv: $(wc -l < data/work/all_listens.csv)"
echo "Taille: $(du -sh data/work/all_listens.csv)"

# ==========================================
# ÉTAPE 4-6: Pipeline Python complet depuis le CSV
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 4: Traitement Python (agrégation + entraînement)"
echo "=========================================="

python3 << 'PYTHON_SCRIPT'
import gc
import json
import pickle
import numpy as np
import pandas as pd
from scipy import sparse
from pathlib import Path
from collections import defaultdict

print("=" * 60)
print("AGRÉGATION DES ÉCOUTES")
print("=" * 60)

# Lire le CSV en chunks pour économiser la RAM
print("Lecture et agrégation du CSV par chunks...")
user_track_counts = defaultdict(lambda: defaultdict(int))
total_listens = 0
chunk_size = 1_000_000

for chunk in pd.read_csv('data/work/all_listens.csv', header=None, names=['user', 'track'], chunksize=chunk_size, dtype=str):
    chunk = chunk.dropna()
    for row in chunk.itertuples(index=False):
        user_track_counts[row.user][row.track] += 1
        total_listens += 1
    if total_listens % 10_000_000 == 0:
        print(f"  {{total_listens:,}} écoutes traitées...")

print(f"\\nTotal écoutes: {{total_listens:,}}")
print(f"Users uniques: {{len(user_track_counts):,}}")

# ==========================================
print("\\n" + "=" * 60)
print("FILTRAGE ET AGRÉGATION")
print("=" * 60)

# Filtrer users avec au moins 5 écoutes
min_user_listens = 5
min_track_listens = 3

user_listen_counts = {{u: sum(tracks.values()) for u, tracks in user_track_counts.items()}}
valid_users = {{u for u, c in user_listen_counts.items() if c >= min_user_listens}}
print(f"Users avec >= {{min_user_listens}} écoutes: {{len(valid_users):,}}")

# Recompter les tracks après filtrage users
track_counts = defaultdict(int)
for user in valid_users:
    for track, count in user_track_counts[user].items():
        track_counts[track] += count

valid_tracks = {{t for t, c in track_counts.items() if c >= min_track_listens}}
print(f"Tracks avec >= {{min_track_listens}} écoutes: {{len(valid_tracks):,}}")

# Créer les mappings
users_list = sorted(valid_users)
tracks_list = sorted(valid_tracks)

user_to_id = {{u: i for i, u in enumerate(users_list)}}
track_to_id = {{t: i for i, t in enumerate(tracks_list)}}
id_to_user = {{i: u for u, i in user_to_id.items()}}
id_to_track = {{i: t for t, i in track_to_id.items()}}

print(f"\\nMatrice finale: {{len(users_list)}} users x {{len(tracks_list)}} tracks")

# Libérer la mémoire
del user_listen_counts, track_counts
gc.collect()

# ==========================================
print("\\n" + "=" * 60)
print("CONSTRUCTION MATRICE SPARSE")
print("=" * 60)

# Construire les listes pour la matrice sparse
rows = []
cols = []
values = []

alpha = 40.0  # Confidence scaling

for user in valid_users:
    user_id = user_to_id[user]
    for track, count in user_track_counts[user].items():
        if track in valid_tracks:
            track_id = track_to_id[track]
            confidence = 1 + alpha * np.log1p(count)
            rows.append(user_id)
            cols.append(track_id)
            values.append(confidence)

# Libérer la mémoire
del user_track_counts
gc.collect()

n_users = len(users_list)
n_items = len(tracks_list)

matrix = sparse.csr_matrix(
    (values, (rows, cols)),
    shape=(n_users, n_items),
    dtype=np.float32
)

del rows, cols, values
gc.collect()

print(f"Dimensions: {{n_users:,}} users x {{n_items:,}} items")
print(f"Non-zeros: {{matrix.nnz:,}}")
print(f"Sparsité: {{(1 - matrix.nnz / (n_users * n_items)) * 100:.4f}}%")

# Sauvegarder
Path("data/processed").mkdir(parents=True, exist_ok=True)
sparse.save_npz('data/processed/user_item_matrix.npz', matrix)

# Sauvegarder les mappings
mappings = {{
    'user_to_id': user_to_id,
    'id_to_user': {{str(k): v for k, v in id_to_user.items()}},
    'track_to_id': track_to_id,
    'id_to_track': {{str(k): v for k, v in id_to_track.items()}},
}}
with open('data/processed/mappings.json', 'w') as f:
    json.dump(mappings, f)

# Split train/test (20%)
print("\\nCréation split train/test...")
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

del train, test
gc.collect()

# ==========================================
print("\\n" + "=" * 60)
print("ENTRAÎNEMENT ALS (Production)")
print("=" * 60)

from implicit.als import AlternatingLeastSquares

# Paramètres de production
model = AlternatingLeastSquares(
    factors=128,
    regularization=0.01,
    iterations=15,
    use_gpu=False,
    num_threads=0  # Utilise tous les cores
)

print("Entraînement (128 facteurs, 15 itérations)...")
model.fit(matrix.T.tocsr(), show_progress=True)

# Sauvegarder le modèle
Path("models").mkdir(parents=True, exist_ok=True)
state = {{
    'model': model,
    'factors': 128,
    'regularization': 0.01,
    'iterations': 15,
    'user_mapping': id_to_user,
    'item_mapping': id_to_track,
    'is_fitted': True
}}
with open('models/als_model.pkl', 'wb') as f:
    pickle.dump(state, f)

print("Modèle sauvegardé: models/als_model.pkl")
print(f"  User factors shape: {{model.user_factors.shape}}")
print(f"  Item factors shape: {{model.item_factors.shape}}")

# ==========================================
print("\\n" + "=" * 60)
print("ÉVALUATION RAPIDE")
print("=" * 60)

# Charger test set
test = sparse.load_npz('data/processed/test_matrix.npz')

# Évaluer sur un échantillon
from collections import defaultdict
precisions = []
recalls = []

sample_users = np.random.choice(n_users, size=min(1000, n_users), replace=False)

for user_id in sample_users:
    test_items = set(test[user_id].indices)
    if not test_items:
        continue

    try:
        user_items = matrix[user_id]
        ids, scores = model.recommend(user_id, user_items, N=10, filter_already_liked_items=True)
        recommended = set(ids)

        hits = len(recommended & test_items)
        precisions.append(hits / 10)
        recalls.append(hits / len(test_items))
    except:
        pass

if precisions:
    print(f"Precision@10: {{np.mean(precisions):.4f}}")
    print(f"Recall@10: {{np.mean(recalls):.4f}}")

    # Sauver les résultats
    results = {{
        'precision_at_10': float(np.mean(precisions)),
        'recall_at_10': float(np.mean(recalls)),
        'n_users': n_users,
        'n_items': n_items,
        'n_interactions': matrix.nnz
    }}
    with open('models/evaluation_results.json', 'w') as f:
        json.dump(results, f, indent=2)

print("\\n" + "=" * 60)
print("TRAITEMENT TERMINÉ!")
print("=" * 60)
PYTHON_SCRIPT

# ==========================================
# ÉTAPE 7: Upload vers S3
# ==========================================
echo ""
echo "=========================================="
echo "ÉTAPE 7: Upload vers S3"
echo "=========================================="

aws s3 cp models/ s3://$S3_BUCKET/models/ --recursive
aws s3 cp data/processed/user_item_matrix.npz s3://$S3_BUCKET/processed/
aws s3 cp data/processed/mappings.json s3://$S3_BUCKET/processed/
aws s3 cp data/processed/train_matrix.npz s3://$S3_BUCKET/processed/
aws s3 cp data/processed/test_matrix.npz s3://$S3_BUCKET/processed/

# Marquer comme terminé
echo "PIPELINE COMPLETED $(date)" > /tmp/pipeline_completed
aws s3 cp /tmp/pipeline_completed s3://$S3_BUCKET/status/full_pipeline_completed

echo ""
echo "=========================================="
echo "PIPELINE COMPLET TERMINÉ!"
echo "=========================================="
date

echo ""
echo "Résumé des fichiers:"
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
    """Récupère la dernière AMI Amazon Linux 2023."""
    response = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            {'Name': 'name', 'Values': ['al2023-ami-*-x86_64']},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
    return images[0]['ImageId']


def launch_pipeline_instance(ec2_client, iam_client) -> str:
    """Lance une instance EC2 pour le pipeline complet."""

    print("=" * 60)
    print("LANCEMENT DU PIPELINE COMPLET")
    print("=" * 60)
    print(f"Instance: {INSTANCE_TYPE} (16 GB RAM)")
    print(f"Bucket: {S3_BUCKET}")
    print(f"Coût estimé: ~0.10€/h × 2-4h = ~0.40€")
    print("=" * 60)

    # Rôle IAM
    instance_profile = get_or_create_iam_role(iam_client)

    # AMI
    ami_id = get_amazon_linux_ami(ec2_client)
    print(f"AMI: {ami_id}")

    # User data
    user_data = get_user_data_script(S3_BUCKET)

    # Lancer l'instance
    print(f"\nLancement de l'instance...")

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={'Name': instance_profile},
        UserData=user_data,
        BlockDeviceMappings=[{
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'VolumeSize': 100,  # 100 GB pour les données
                'VolumeType': 'gp3',
                'DeleteOnTermination': True
            }
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': 'FULL-Recommendation-Pipeline'},
                {'Key': 'Project', 'Value': 'MusicRecommendation'}
            ]
        }],
        InstanceInitiatedShutdownBehavior='terminate'
    )

    instance_id = response['Instances'][0]['InstanceId']
    print(f"Instance lancée: {instance_id}")

    return instance_id


def monitor_pipeline(ec2_client, s3_client, instance_id: str):
    """Surveille le pipeline."""
    print("\n" + "=" * 60)
    print("MONITORING DU PIPELINE")
    print("=" * 60)
    print("Le pipeline devrait prendre 2-4 heures")
    print("Ctrl+C pour arrêter le monitoring (l'instance continue)")
    print("")

    start_time = time.time()

    try:
        while True:
            elapsed = int(time.time() - start_time)
            hours, remainder = divmod(elapsed, 3600)
            mins, secs = divmod(remainder, 60)

            # Status instance
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']

            # Vérifier si terminé
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key='status/full_pipeline_completed')
                print(f"\n\n{'=' * 60}")
                print("✅ PIPELINE COMPLET TERMINÉ!")
                print(f"{'=' * 60}")
                print(f"Durée: {hours}h {mins}m {secs}s")
                print(f"\nRésultats sur S3:")
                print(f"  Modèle: s3://{S3_BUCKET}/models/als_model.pkl")
                print(f"  Données: s3://{S3_BUCKET}/processed/")
                print(f"\n⚠️  IMPORTANT - Terminer l'instance:")
                print(f"  python scripts/run_full_pipeline_ec2.py --terminate {instance_id}")
                return True
            except ClientError:
                pass

            if state == 'terminated':
                print(f"\n❌ Instance terminée!")
                return False

            print(f"[{hours:02d}:{mins:02d}:{secs:02d}] Instance: {state:<12}", end='\r')
            time.sleep(30)

    except KeyboardInterrupt:
        print(f"\n\nMonitoring arrêté après {hours}h {mins}m")
        print(f"L'instance {instance_id} continue de tourner.")
        print(f"\nPour voir les logs:")
        print(f"  python scripts/run_full_pipeline_ec2.py --logs {instance_id}")
        print(f"\nPour terminer:")
        print(f"  python scripts/run_full_pipeline_ec2.py --terminate {instance_id}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Lancer le pipeline complet sur EC2")
    parser.add_argument("--no-monitor", action="store_true",
                       help="Ne pas surveiller (lancer et quitter)")
    parser.add_argument("--terminate", metavar="INSTANCE_ID",
                       help="Terminer une instance")
    parser.add_argument("--logs", metavar="INSTANCE_ID",
                       help="Voir les logs d'une instance")
    parser.add_argument("--status", action="store_true",
                       help="Vérifier le statut du pipeline")

    args = parser.parse_args()

    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    iam_client = boto3.client('iam')
    s3_client = boto3.client('s3', region_name=AWS_REGION)

    if args.terminate:
        print(f"Termination de {args.terminate}...")
        ec2_client.terminate_instances(InstanceIds=[args.terminate])
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key='status/full_pipeline_completed')
        except:
            pass
        print("Instance terminée.")
        return

    if args.logs:
        print(f"Logs de {args.logs}:\n")
        try:
            response = ec2_client.get_console_output(InstanceId=args.logs)
            if 'Output' in response and response['Output']:
                # Afficher les dernières lignes pertinentes
                output = response['Output']
                lines = output.split('\n')
                # Chercher les lignes de notre script
                relevant_lines = [l for l in lines if 'cloud-init' in l or 'ÉTAPE' in l or 'PIPELINE' in l or 'Entraînement' in l]
                if relevant_lines:
                    print('\n'.join(relevant_lines[-50:]))
                else:
                    print('\n'.join(lines[-100:]))
            else:
                print("Pas encore de logs disponibles (attendre quelques minutes)")
        except Exception as e:
            print(f"Erreur: {e}")
        return

    if args.status:
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key='status/full_pipeline_completed')
            print("✅ Pipeline TERMINÉ!")
            print(f"\nModèle disponible: s3://{S3_BUCKET}/models/als_model.pkl")
        except ClientError:
            print("⏳ Pipeline en cours ou pas encore lancé")
        return

    # Lancer le pipeline
    instance_id = launch_pipeline_instance(ec2_client, iam_client)

    print(f"\n{'=' * 60}")
    print("INSTANCE LANCÉE")
    print(f"{'=' * 60}")
    print(f"Instance ID: {instance_id}")
    print(f"Région: {AWS_REGION}")
    print("")
    print("Commandes utiles:")
    print(f"  Status:   python scripts/run_full_pipeline_ec2.py --status")
    print(f"  Logs:     python scripts/run_full_pipeline_ec2.py --logs {instance_id}")
    print(f"  Terminer: python scripts/run_full_pipeline_ec2.py --terminate {instance_id}")

    if not args.no_monitor:
        print("")
        monitor_pipeline(ec2_client, s3_client, instance_id)


if __name__ == "__main__":
    main()
