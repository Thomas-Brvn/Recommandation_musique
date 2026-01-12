#!/usr/bin/env python3
"""
Script pour télécharger les fichiers MusicBrainz manquants (artist et recording)
"""

import sys
import json
import subprocess
from pathlib import Path

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
                print(f"✅ AMI trouvée: {ami_id}")
                return ami_id
        except:
            pass

    return None

def create_user_data_script(bucket_name):
    """Crée le script pour télécharger uniquement artist et recording"""
    script = f"""#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Téléchargement des fichiers manquants"
echo "Date: $(date)"
echo "=========================================="

# Installation
apt-get update
apt-get install -y wget awscli

# Créer le répertoire
mkdir -p /data/musicbrainz
cd /data

# Configuration
BUCKET_NAME="{bucket_name}"
MB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"

# Trouver la dernière version
echo "Recherche de la dernière version MusicBrainz..."
MB_LATEST=$(curl -s "$MB_BASE_URL" | grep -o 'href="[0-9]*-[0-9]*/"' | tail -1 | cut -d'"' -f2)
MUSICBRAINZ_URL="${{MB_BASE_URL}}${{MB_LATEST}}"
echo "Version: $MB_LATEST"
echo "URL: $MUSICBRAINZ_URL"

# Liste des fichiers manquants
MISSING_FILES="artist recording"

for table in $MISSING_FILES; do
    echo "=========================================="
    echo "Téléchargement de $table.tar.xz"
    echo "=========================================="

    FILE_URL="${{MUSICBRAINZ_URL}}${{table}}.tar.xz"
    echo "URL complète: $FILE_URL"

    # Téléchargement avec plus de verbosité
    wget --timeout=300 --tries=3 -O "/data/musicbrainz/${{table}}.tar.xz" "$FILE_URL"
    WGET_EXIT=$?

    if [ $WGET_EXIT -eq 0 ]; then
        FILE_SIZE=$(du -h "/data/musicbrainz/${{table}}.tar.xz" | cut -f1)
        echo "✓ $table téléchargé ($FILE_SIZE)"

        echo "Upload vers S3..."
        aws s3 cp "/data/musicbrainz/${{table}}.tar.xz" "s3://$BUCKET_NAME/raw/musicbrainz/${{table}}.tar.xz"

        if [ $? -eq 0 ]; then
            echo "✓ $table uploadé vers S3"
            rm "/data/musicbrainz/${{table}}.tar.xz"
        else
            echo "✗ Erreur upload $table vers S3"
        fi
    else
        echo "✗ Erreur téléchargement $table (exit code: $WGET_EXIT)"
        echo "Vérification de l'URL..."
        curl -I "$FILE_URL" 2>&1 | head -5
    fi
done

echo "=========================================="
echo "Terminé"
echo "Date: $(date)"
echo "=========================================="

# Créer un marqueur de fin
echo "COMPLETED_MISSING" > /tmp/download-status
aws s3 cp /tmp/download-status "s3://$BUCKET_NAME/raw/.download-missing-completed"
"""

    return script

def load_config():
    """Charge la configuration AWS"""
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def create_instance(region, bucket_name):
    """Crée et lance l'instance EC2"""
    print(f"\n🚀 Lancement de l'instance EC2 pour les fichiers manquants...")
    print(f"   Région: {region}")
    print(f"   Type: t3.small")
    print(f"   Fichiers: artist.tar.xz, recording.tar.xz")

    # Générer le user data script
    user_data = create_user_data_script(bucket_name)
    user_data_file = Path("/tmp/ec2-missing-files.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        return None

    # Profil IAM
    instance_profile = "EC2-S3-Access-Profile"

    # Lancer l'instance avec 20GB (suffisant pour artist 1.5GB + recording 30MB)
    print(f"\n🚀 Lancement de l'instance t3.small avec 20GB...")
    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type t3.small \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
        --region {region}"""

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code != 0:
        print(f"❌ Erreur lors du lancement: {stderr}")
        return None

    instance_info = json.loads(stdout)
    instance_id = instance_info['Instances'][0]['InstanceId']

    print(f"✅ Instance lancée: {instance_id}")

    # Sauvegarder l'instance ID
    instance_file = Path("config/ec2_instance.json")
    with open(instance_file, 'w') as f:
        json.dump({"instance_id": instance_id, "region": region}, f, indent=2)

    return instance_id

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🚀 Téléchargement des fichiers MusicBrainz manquants")
    print("=" * 60)

    # Charger la config
    config = load_config()
    if not config:
        print("❌ Configuration non trouvée")
        sys.exit(1)

    bucket_name = config.get("bucket_name")
    region = config.get("region")

    print(f"✅ Configuration chargée")
    print(f"   Bucket: {bucket_name}")
    print(f"   Région: {region}")

    print("\n📦 Fichiers à télécharger:")
    print("  • artist.tar.xz (~1.5 GB)")
    print("  • recording.tar.xz (~30 MB)")

    print("\n💰 Coût estimé: ~0.01 USD (5-10 minutes)")

    if len(sys.argv) <= 1:
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("❌ Annulé")
            sys.exit(0)

    # Lancer l'instance
    instance_id = create_instance(region, bucket_name)

    if instance_id:
        print("\n" + "=" * 60)
        print("📊 Instance EC2 lancée avec succès!")
        print("=" * 60)
        print(f"Instance ID: {instance_id}")
        print(f"Région: {region}")
        print("\n💡 Monitoring:")
        print(f"  python scripts/monitor_ec2_download.py")
        print("\n⏱️  Durée estimée: 5-10 minutes")
        print("\n⚠️  N'oubliez pas de terminer l'instance après!")
        print("=" * 60)
    else:
        print("❌ Échec du lancement de l'instance")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)