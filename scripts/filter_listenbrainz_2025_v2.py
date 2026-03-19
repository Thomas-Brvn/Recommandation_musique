#!/usr/bin/env python3
"""
Script pour filtrer les données ListenBrainz et ne garder que l'année 2025
Utilise une instance EC2 pour traiter le fichier complet (127 GB)
"""

import sys
import json
import time
import subprocess
from pathlib import Path

# Configuration
INSTANCE_TYPE = "t3.small"  # 2 vCPU, 2 GB RAM
DEFAULT_REGION = "eu-north-1"

def run_aws_command(cmd, check=True):
    """Exécute une commande AWS CLI"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=True,
            text=True
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout, e.stderr, e.returncode

def get_ubuntu_ami(region):
    """Récupère l'AMI Ubuntu 22.04 LTS la plus récente pour la région"""
    print(f"🔍 Recherche de l'AMI Ubuntu 22.04 pour {region}...")

    cmd = f"""aws ec2 describe-images \
        --region {region} \
        --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
                  "Name=state,Values=available" \
        --query "sort_by(Images, &CreationDate)[-1].[ImageId,Name]" \
        --output json"""

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code == 0 and stdout:
        try:
            result = json.loads(stdout)
            if result and len(result) > 0:
                ami_id = result[0]
                ami_name = result[1] if len(result) > 1 else "Ubuntu 22.04 LTS"
                print(f"✅ AMI trouvée: {ami_id}")
                return ami_id
        except:
            pass

    print("❌ Impossible de trouver l'AMI automatiquement")
    return None

def load_config():
    """Charge la configuration AWS"""
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def create_filter_script(bucket_name):
    """Crée le script qui sera exécuté au démarrage de l'instance"""
    script = """#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Filtrage ListenBrainz 2025"
echo "Date: $(date)"
echo "=========================================="

# Mettre à jour le système et installer les dépendances
echo "Installation des dépendances..."
apt-get update
apt-get install -y awscli python3-pip

# Installer PyArrow pour lire les Parquet
pip3 install pyarrow pandas

# Créer un répertoire de travail avec beaucoup d'espace
mkdir -p /data/input /data/output
cd /data

# Configuration
BUCKET_NAME="{}"
INPUT_FILE="listenbrainz-spark-dump-2382-20260101-000003-full.tar"
OUTPUT_DIR="/data/output/listenbrainz-2025"

# Timestamps pour 2025 (UTC)
# 2025-01-01 00:00:00 = 1735689600
# 2025-12-31 23:59:59 = 1767225599
START_TS=1735689600
END_TS=1767225599

echo "=========================================="
echo "Téléchargement depuis S3"
echo "=========================================="
echo "Fichier: $INPUT_FILE"
echo "Taille: ~127 GB"

aws s3 cp "s3://$BUCKET_NAME/raw/listenbrainz/$INPUT_FILE" /data/input/$INPUT_FILE --region {}

if [ $? -ne 0 ]; then
    echo "✗ Erreur téléchargement depuis S3"
    exit 1
fi

echo "✓ Téléchargement terminé"

echo "=========================================="
echo "Extraction de l'archive"
echo "=========================================="

mkdir -p /data/extracted
tar -xf /data/input/$INPUT_FILE -C /data/extracted

if [ $? -ne 0 ]; then
    echo "✗ Erreur extraction"
    exit 1
fi

echo "✓ Extraction terminée"

# Trouver le répertoire extrait
EXTRACTED_DIR=$(find /data/extracted -name "listenbrainz-spark-dump-*-full" -type d | head -1)
echo "Répertoire extrait: $EXTRACTED_DIR"

echo "=========================================="
echo "Filtrage des données 2025"
echo "=========================================="

# Créer le répertoire de sortie avec la même structure
mkdir -p $OUTPUT_DIR

# Copier les fichiers métadata
cp $EXTRACTED_DIR/SCHEMA_SEQUENCE $OUTPUT_DIR/ 2>/dev/null || true
cp $EXTRACTED_DIR/TIMESTAMP $OUTPUT_DIR/ 2>/dev/null || true
cp $EXTRACTED_DIR/COPYING $OUTPUT_DIR/ 2>/dev/null || true

# Script Python pour filtrer les Parquet
cat > /data/filter_parquet.py << 'PYEOF'
import sys
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

def filter_parquet_file(input_file, output_file, start_ts, end_ts):
    try:
        # Lire le fichier Parquet
        table = pq.read_table(input_file)

        # Filtrer sur la colonne listened_at
        mask = (table['listened_at'] >= start_ts) & (table['listened_at'] <= end_ts)
        filtered_table = table.filter(mask)

        # Sauvegarder si des données restent
        if len(filtered_table) > 0:
            pq.write_table(filtered_table, output_file)
            return len(filtered_table)
        else:
            return 0
    except Exception as e:
        print("Erreur sur " + str(input_file) + ": " + str(e))
        return -1

if __name__ == "__main__":
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    start_ts = int(sys.argv[3])
    end_ts = int(sys.argv[4])

    input_path = Path(input_dir)
    output_path = Path(output_dir)

    # Trouver tous les fichiers .parquet
    parquet_files = sorted(input_path.glob("*.parquet"))

    print("Trouve " + str(len(parquet_files)) + " fichiers Parquet")

    total_rows = 0
    files_kept = 0

    for i, parquet_file in enumerate(parquet_files):
        print("[" + str(i+1) + "/" + str(len(parquet_files)) + "] Traitement de " + parquet_file.name + "...", end=" ")

        output_file = output_path / parquet_file.name
        rows = filter_parquet_file(parquet_file, output_file, start_ts, end_ts)

        if rows > 0:
            print("OK " + str(rows) + " lignes conservees")
            total_rows += rows
            files_kept += 1
        elif rows == 0:
            print("Aucune donnee 2025")
        else:
            print("Erreur")

    print("\\n========================================")
    print("Resume du filtrage:")
    print("  - Fichiers traites: " + str(len(parquet_files)))
    print("  - Fichiers conserves: " + str(files_kept))
    print("  - Total lignes 2025: " + str(total_rows))
    print("========================================")
PYEOF

# Exécuter le filtrage
python3 /data/filter_parquet.py "$EXTRACTED_DIR" "$OUTPUT_DIR" $START_TS $END_TS

if [ $? -ne 0 ]; then
    echo "✗ Erreur filtrage"
    exit 1
fi

echo "✓ Filtrage terminé"

echo "=========================================="
echo "Compression et upload vers S3"
echo "=========================================="

cd /data/output
tar -czf listenbrainz-2025-only.tar.gz listenbrainz-2025/

if [ $? -ne 0 ]; then
    echo "✗ Erreur compression"
    exit 1
fi

echo "✓ Compression terminée"

# Taille du fichier
SIZE=$(du -h listenbrainz-2025-only.tar.gz | cut -f1)
echo "Taille du fichier compressé: $SIZE"

# Upload vers S3
aws s3 cp listenbrainz-2025-only.tar.gz "s3://$BUCKET_NAME/processed/listenbrainz/listenbrainz-2025-only.tar.gz" --region {}

if [ $? -ne 0 ]; then
    echo "✗ Erreur upload S3"
    exit 1
fi

echo "✓ Upload terminé"

# Créer un fichier metadata
cat > /tmp/metadata-2025.json << METAEOF
{{
  "source_file": "$INPUT_FILE",
  "filter": "year 2025",
  "start_timestamp": $START_TS,
  "end_timestamp": $END_TS,
  "start_date": "2025-01-01 00:00:00 UTC",
  "end_date": "2025-12-31 23:59:59 UTC",
  "processed_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "output_size": "$SIZE"
}}
METAEOF

aws s3 cp /tmp/metadata-2025.json "s3://$BUCKET_NAME/processed/listenbrainz/metadata-2025-v2.json" --region {}

echo "=========================================="
echo "Traitement terminé avec succès"
echo "Date: $(date)"
echo "=========================================="

# Créer un fichier de statut
echo "COMPLETED" > /tmp/filter-status
aws s3 cp /tmp/filter-status "s3://$BUCKET_NAME/processed/listenbrainz/.filter-2025-completed" --region {}
"""

    return script.format(bucket_name, DEFAULT_REGION, DEFAULT_REGION, DEFAULT_REGION, DEFAULT_REGION)

def create_instance(region, bucket_name):
    """Crée et lance l'instance EC2"""
    print(f"\n🚀 Lancement de l'instance EC2...")
    print(f"   Région: {region}")
    print(f"   Type: {INSTANCE_TYPE}")

    # Générer le user data script
    user_data = create_filter_script(bucket_name)
    user_data_file = Path("/tmp/ec2-filter-2025.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI pour la région
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        return None

    # Vérifier le profil IAM
    print("\n📋 Vérification du rôle IAM...")
    instance_profile = "EC2-S3-Access-Profile"

    stdout, stderr, code = run_aws_command(
        f"aws iam get-instance-profile --instance-profile-name {instance_profile}",
        check=False
    )

    if code != 0:
        print("❌ Profil IAM non trouvé. Veuillez exécuter download_to_s3_via_ec2.py d'abord.")
        return None

    print("✅ Profil IAM existant trouvé")

    # Lancer l'instance avec un volume EBS de 300 GB
    # (127 GB tar + 127 GB extrait + 50 GB filtré + marge)
    print(f"\n🚀 Lancement de l'instance {INSTANCE_TYPE}...")

    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {INSTANCE_TYPE} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":300,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
        --region {region}"""

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code != 0:
        print(f"❌ Erreur lors du lancement: {stderr}")
        return None

    instance_info = json.loads(stdout)
    instance_id = instance_info['Instances'][0]['InstanceId']

    print(f"✅ Instance lancée: {instance_id}")

    return instance_id

def monitor_instance(instance_id, region):
    """Affiche les informations de monitoring"""
    print("\n" + "=" * 60)
    print("📊 Instance EC2 lancée avec succès!")
    print("=" * 60)
    print(f"Instance ID: {instance_id}")
    print(f"Région: {region}")
    print("\n📝 Commandes utiles:")
    print(f"\n  # Voir les logs en temps réel:")
    print(f"  aws ec2 get-console-output --instance-id {instance_id} --region {region} --output text | tail -50")
    print(f"\n  # Voir le statut:")
    print(f"  aws ec2 describe-instances --instance-ids {instance_id} --region {region}")
    print(f"\n  # Terminer l'instance:")
    print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")

    print("\n⏱️  Durée estimée:")
    print("  • Téléchargement S3 -> EC2: 30-60 min")
    print("  • Extraction tar: 20-30 min")
    print("  • Filtrage Parquet: 1-2 heures")
    print("  • Compression: 10-20 min")
    print("  • Upload S3: 20-40 min")
    print("  • TOTAL: 2-4 heures")

    print("\n💰 Coût estimé: ~0.20-0.40 USD (instance t3.small)")
    print("\n⚠️  N'oubliez pas de terminer l'instance après le traitement!")
    print("=" * 60)

    # Sauvegarder l'instance ID
    instance_file = Path("config/ec2_instance.json")
    instance_file.parent.mkdir(exist_ok=True)
    with open(instance_file, 'w') as f:
        json.dump({"instance_id": instance_id, "region": region}, f, indent=2)
    print(f"\n💾 Instance ID sauvegardé dans: {instance_file}")

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🔍 Filtrage ListenBrainz 2025")
    print("=" * 60)

    # Charger la config
    config = load_config()
    if config:
        bucket_name = config.get("bucket_name")
        region = config.get("region", DEFAULT_REGION)
        print(f"✅ Configuration chargée")
        print(f"   Bucket: {bucket_name}")
        print(f"   Région: {region}")
    else:
        print("❌ Configuration non trouvée")
        sys.exit(1)

    # Confirmation
    print("\n📋 Ce script va:")
    print("  1. Télécharger le fichier complet (127.7 GB) depuis S3")
    print("  2. L'extraire (~127 GB décompressé)")
    print("  3. Filtrer uniquement les données de 2025")
    print("  4. Recompresser et uploader sur S3")

    print("\n⚠️  Attention:")
    print("  • Durée estimée: 2-4 heures")
    print("  • Coût estimé: ~0.20-0.40 USD")
    print("  • Volume EBS: 300 GB (sera supprimé automatiquement)")

    if len(sys.argv) <= 1 or sys.argv[1].lower() != 'y':
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("❌ Annulé")
            sys.exit(0)

    # Lancer l'instance
    instance_id = create_instance(region, bucket_name)

    if instance_id:
        monitor_instance(instance_id, region)
    else:
        print("❌ Échec du lancement de l'instance")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)