#!/usr/bin/env python3
"""
Script pour télécharger les données ListenBrainz 2025
Télécharge le fichier listens-dump (organisé par année) et extrait uniquement 2025
"""

import sys
import json
import subprocess
from pathlib import Path

# Configuration
INSTANCE_TYPE = "t3.small"
DEFAULT_REGION = "eu-north-1"

# URL du fichier listens-dump (décembre 2025)
LISTENS_DUMP_URL = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/listenbrainz-dump-2351-20251203-000003-full/listenbrainz-listens-dump-2351-20251203-000003-full.tar.zst"
LISTENS_DUMP_FILE = "listenbrainz-listens-dump-2351-20251203-000003-full.tar.zst"

def run_aws_command(cmd, check=True):
    try:
        result = subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)
        return result.stdout, result.stderr, result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout, e.stderr, e.returncode

def get_ubuntu_ami(region):
    print(f"Recherche de l'AMI Ubuntu 22.04 pour {region}...")
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
                print(f"AMI trouvee: {result[0]}")
                return result[0]
        except:
            pass
    print("Impossible de trouver l'AMI")
    return None

def load_config():
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def create_download_script(bucket_name):
    script = """#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Telechargement ListenBrainz 2025"
echo "Date: $(date)"
echo "=========================================="

# Installer les dependances
echo "Installation des dependances..."
apt-get update
apt-get install -y wget awscli zstd

# Creer un repertoire de travail
mkdir -p /data
cd /data

# Configuration
BUCKET_NAME="{}"
REGION="{}"
DUMP_URL="https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/listenbrainz-dump-2351-20251203-000003-full/listenbrainz-listens-dump-2351-20251203-000003-full.tar.zst"
DUMP_FILE="listenbrainz-listens-dump.tar.zst"

echo "=========================================="
echo "Telechargement du dump listens"
echo "=========================================="
echo "URL: $DUMP_URL"
echo "Taille: ~127 GB"

wget -q --show-progress -O $DUMP_FILE "$DUMP_URL"

if [ $? -ne 0 ]; then
    echo "Erreur telechargement"
    exit 1
fi

echo "Telechargement termine"

echo "=========================================="
echo "Decompression (zstd)"
echo "=========================================="

# Decompresser le .zst
zstd -d $DUMP_FILE -o listenbrainz-listens-dump.tar

if [ $? -ne 0 ]; then
    echo "Erreur decompression zstd"
    exit 1
fi

# Supprimer le fichier compresse pour liberer de l'espace
rm $DUMP_FILE
echo "Decompression zstd terminee"

echo "=========================================="
echo "Extraction des donnees 2025 uniquement"
echo "=========================================="

# Lister le contenu pour trouver le bon chemin
echo "Structure de l'archive:"
tar -tf listenbrainz-listens-dump.tar | head -20

# Extraire uniquement le dossier 2025
mkdir -p /data/output/2025
tar -xf listenbrainz-listens-dump.tar --wildcards '*/listens/2025/*' -C /data/output --strip-components=2

if [ $? -ne 0 ]; then
    echo "Erreur extraction 2025"
    # Essayer une autre methode
    echo "Tentative extraction complete..."
    tar -xf listenbrainz-listens-dump.tar

    # Trouver le dossier 2025
    YEAR_DIR=$(find /data -path "*/listens/2025" -type d 2>/dev/null | head -1)
    if [ -n "$YEAR_DIR" ]; then
        echo "Dossier 2025 trouve: $YEAR_DIR"
        cp -r "$YEAR_DIR" /data/output/
    else
        echo "Dossier 2025 non trouve"
        exit 1
    fi
fi

# Supprimer l'archive tar pour liberer de l'espace
rm -f listenbrainz-listens-dump.tar

echo "Extraction terminee"

# Afficher ce qu'on a extrait
echo "Contenu extrait:"
ls -la /data/output/
ls -la /data/output/2025/ 2>/dev/null || ls -la /data/output/listens/2025/ 2>/dev/null

echo "=========================================="
echo "Compression et upload vers S3"
echo "=========================================="

cd /data/output

# Compresser le dossier 2025
tar -czf listenbrainz-2025-listens.tar.gz 2025/ 2>/dev/null || tar -czf listenbrainz-2025-listens.tar.gz listens/2025/

if [ $? -ne 0 ]; then
    echo "Erreur compression"
    exit 1
fi

# Taille du fichier
SIZE=$(du -h listenbrainz-2025-listens.tar.gz | cut -f1)
echo "Taille du fichier compresse: $SIZE"

# Compter les fichiers
NUM_FILES=$(find . -name "*.listens" | wc -l)
echo "Nombre de fichiers .listens: $NUM_FILES"

# Upload vers S3
aws s3 cp listenbrainz-2025-listens.tar.gz "s3://$BUCKET_NAME/processed/listenbrainz/listenbrainz-2025-listens.tar.gz" --region $REGION

if [ $? -ne 0 ]; then
    echo "Erreur upload S3"
    exit 1
fi

echo "Upload termine"

# Metadata
cat > /tmp/metadata-2025-listens.json << METAEOF
{{
  "source": "listenbrainz-listens-dump-2351-20251203-000003-full.tar.zst",
  "type": "listens",
  "year": 2025,
  "format": "JSON lines (.listens files)",
  "processed_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "output_size": "$SIZE",
  "num_files": "$NUM_FILES"
}}
METAEOF

aws s3 cp /tmp/metadata-2025-listens.json "s3://$BUCKET_NAME/processed/listenbrainz/metadata-2025-listens.json" --region $REGION

echo "=========================================="
echo "Traitement termine avec succes"
echo "Date: $(date)"
echo "=========================================="

echo "COMPLETED" > /tmp/download-status
aws s3 cp /tmp/download-status "s3://$BUCKET_NAME/processed/listenbrainz/.listens-2025-completed" --region $REGION
"""
    return script.format(bucket_name, DEFAULT_REGION)

def create_instance(region, bucket_name):
    print(f"\nLancement de l'instance EC2...")
    print(f"   Region: {region}")
    print(f"   Type: {INSTANCE_TYPE}")

    user_data = create_download_script(bucket_name)
    user_data_file = Path("/tmp/ec2-download-2025.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        return None

    print("\nVerification du role IAM...")
    instance_profile = "EC2-S3-Access-Profile"
    stdout, stderr, code = run_aws_command(
        f"aws iam get-instance-profile --instance-profile-name {instance_profile}",
        check=False
    )
    if code != 0:
        print("Profil IAM non trouve")
        return None
    print("Profil IAM OK")

    print(f"\nLancement de l'instance {INSTANCE_TYPE}...")

    # Volume de 300 GB pour avoir assez d'espace
    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {INSTANCE_TYPE} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":300,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
        --region {region}"""

    stdout, stderr, code = run_aws_command(cmd, check=False)
    if code != 0:
        print(f"Erreur: {stderr}")
        return None

    instance_info = json.loads(stdout)
    instance_id = instance_info['Instances'][0]['InstanceId']
    print(f"Instance lancee: {instance_id}")
    return instance_id

def main():
    print("=" * 60)
    print("Telechargement ListenBrainz 2025 (listens-dump)")
    print("=" * 60)

    config = load_config()
    if config:
        bucket_name = config.get("bucket_name")
        region = config.get("region", DEFAULT_REGION)
        print(f"Configuration chargee")
        print(f"   Bucket: {bucket_name}")
        print(f"   Region: {region}")
    else:
        print("Configuration non trouvee")
        sys.exit(1)

    print("\nCe script va:")
    print("  1. Telecharger listenbrainz-listens-dump (~127 GB)")
    print("  2. Decompresser le fichier .zst")
    print("  3. Extraire uniquement le dossier /listens/2025/")
    print("  4. Compresser et uploader sur S3")
    print("\nDuree estimee: 3-5 heures")
    print("Cout estime: ~0.30-0.50 USD")

    if len(sys.argv) <= 1 or sys.argv[1].lower() != 'y':
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("Annule")
            sys.exit(0)

    instance_id = create_instance(region, bucket_name)

    if instance_id:
        print("\n" + "=" * 60)
        print("Instance EC2 lancee!")
        print("=" * 60)
        print(f"Instance ID: {instance_id}")
        print(f"\nCommandes utiles:")
        print(f"  # Voir les logs:")
        print(f"  aws ec2 get-console-output --instance-id {instance_id} --region {region} --output text | tail -50")
        print(f"\n  # Terminer l'instance:")
        print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")
        print("\nDuree estimee: 3-5 heures")
        print("=" * 60)

        # Sauvegarder l'instance ID
        instance_file = Path("config/ec2_instance.json")
        with open(instance_file, 'w') as f:
            json.dump({"instance_id": instance_id, "region": region}, f, indent=2)
    else:
        print("Echec du lancement")
        sys.exit(1)

if __name__ == "__main__":
    main()