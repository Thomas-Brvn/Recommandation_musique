#!/usr/bin/env python3
"""
Script de TEST pour vérifier que le téléchargement ListenBrainz fonctionne
Télécharge seulement les premiers 500 MB pour validation

Usage: python3 scripts/test_listenbrainz_download.py
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

def load_config():
    """Charge la configuration AWS"""
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def create_test_script(bucket_name):
    """Crée le script de TEST qui télécharge seulement 500 MB"""
    script = f"""#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "TEST de téléchargement ListenBrainz"
echo "Téléchargement de 500 MB seulement"
echo "Date: $(date)"
echo "=========================================="

# Installation
apt-get update
apt-get install -y wget awscli

# Créer le répertoire
mkdir -p /data/listenbrainz
cd /data

# Configuration
BUCKET_NAME="{bucket_name}"
LB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/"

echo "=========================================="
echo "Recherche du dernier dump ListenBrainz..."
echo "=========================================="

# Trouver le dernier dump (regex corrigée: 3 champs numériques)
LB_LATEST_DIR=$(curl -s "$LB_BASE_URL" | grep -o 'href="listenbrainz-dump-[0-9]*-[0-9]*-[0-9]*-full/"' | tail -1 | cut -d'"' -f2)

if [ -z "$LB_LATEST_DIR" ]; then
    echo "✗ Aucun dump trouvé"
    echo "DEBUG: Voici ce que curl retourne:"
    curl -s "$LB_BASE_URL" | grep listenbrainz-dump | head -5
    exit 1
fi

echo "✓ Dernier dump: $LB_LATEST_DIR"
LISTENBRAINZ_URL="${{LB_BASE_URL}}${{LB_LATEST_DIR}}"

# Trouver le fichier tar
echo "Recherche du fichier tar..."
LATEST_DUMP=$(curl -s "$LISTENBRAINZ_URL" | grep -o 'href="listenbrainz-spark-dump-[^"]*\\.tar"' | head -1 | cut -d'"' -f2)

if [ -z "$LATEST_DUMP" ]; then
    echo "✗ Aucun fichier tar trouvé"
    echo "DEBUG: Contenu du répertoire:"
    curl -s "$LISTENBRAINZ_URL" | grep -o 'href="[^"]*"' | head -10
    exit 1
fi

echo "✓ Fichier trouvé: $LATEST_DUMP"
FILE_URL="${{LISTENBRAINZ_URL}}${{LATEST_DUMP}}"
echo "✓ URL complète: $FILE_URL"

echo "=========================================="
echo "TEST: Téléchargement des 500 premiers MB"
echo "=========================================="

# Télécharger seulement 500 MB (524288000 bytes)
# Option --range pour télécharger partiellement
wget --timeout=300 \\
     --tries=3 \\
     --range=0-524288000 \\
     -O "/data/listenbrainz/test-${{LATEST_DUMP}}" \\
     "$FILE_URL"

WGET_EXIT=$?

if [ $WGET_EXIT -eq 0 ] || [ $WGET_EXIT -eq 8 ]; then
    # Exit 8 = erreur serveur partielle (normal avec --range)
    FILE_SIZE=$(du -h "/data/listenbrainz/test-${{LATEST_DUMP}}" | cut -f1)
    echo "✓ TEST réussi! Téléchargé: $FILE_SIZE"

    echo "Upload du fichier test vers S3..."
    aws s3 cp "/data/listenbrainz/test-${{LATEST_DUMP}}" "s3://$BUCKET_NAME/raw/listenbrainz/TEST-${{LATEST_DUMP}}"

    if [ $? -eq 0 ]; then
        echo "✓ Fichier test uploadé vers S3"
        echo ""
        echo "=========================================="
        echo "✅ TEST RÉUSSI!"
        echo "=========================================="
        echo "Le téléchargement fonctionne correctement."
        echo "Fichier test créé: TEST-$LATEST_DUMP (500 MB)"
        echo ""
        echo "Pour télécharger le fichier complet (~121 GB):"
        echo "  python3 scripts/download_to_s3_via_ec2.py 2"
        echo "=========================================="

        # Créer marqueur de succès
        echo "TEST_SUCCESS" > /tmp/test-status
        aws s3 cp /tmp/test-status "s3://$BUCKET_NAME/raw/.test-completed"
    else
        echo "✗ Erreur upload vers S3"
    fi
else
    echo "✗ Erreur téléchargement (exit code: $WGET_EXIT)"
fi

echo ""
echo "Date fin: $(date)"
"""

    return script

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🧪 TEST de téléchargement ListenBrainz")
    print("=" * 60)
    print("Ce script va télécharger seulement 500 MB pour tester")
    print("que le téléchargement fonctionne correctement.")
    print("")
    print("Durée estimée: 2-5 minutes")
    print("Coût estimé: ~0.01 USD")
    print("=" * 60)

    # Charger la config
    config = load_config()
    if not config:
        print("❌ Configuration non trouvée")
        sys.exit(1)

    bucket_name = config.get("bucket_name")
    region = config.get("region")

    print(f"✅ Configuration")
    print(f"   Bucket: {bucket_name}")
    print(f"   Région: {region}")

    if len(sys.argv) <= 1:
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("❌ Annulé")
            sys.exit(0)

    # Générer le script de test
    user_data = create_test_script(bucket_name)
    user_data_file = Path("/tmp/ec2-test-listenbrainz.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        sys.exit(1)

    # Lancer l'instance (petit volume de 10 GB suffit pour test)
    print(f"\n🚀 Lancement de l'instance de test...")
    instance_profile = "EC2-S3-Access-Profile"
    instance_type = "t3.small"

    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {instance_type} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":10,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
        --region {region}"""

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code != 0:
        print(f"❌ Erreur: {stderr}")
        sys.exit(1)

    instance_info = json.loads(stdout)
    instance_id = instance_info['Instances'][0]['InstanceId']

    print(f"✅ Instance lancée: {instance_id}")

    # Sauvegarder l'instance ID
    instance_file = Path("config/ec2_instance.json")
    with open(instance_file, 'w') as f:
        json.dump({"instance_id": instance_id, "region": region}, f, indent=2)

    print("\n" + "=" * 60)
    print("📊 Instance de test lancée!")
    print("=" * 60)
    print(f"Instance ID: {instance_id}")
    print(f"Région: {region}")
    print("")
    print("⏱️  Attendez 2-5 minutes, puis vérifiez:")
    print("")
    print("  # Voir les logs:")
    print(f"  aws ec2 get-console-output --instance-id {instance_id} --region {region} --output text | tail -50")
    print("")
    print("  # Vérifier le fichier test sur S3:")
    print(f"  aws s3 ls s3://{bucket_name}/raw/listenbrainz/ --region {region} --human-readable")
    print("")
    print("  # Terminer l'instance:")
    print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")
    print("")
    print("Si vous voyez un fichier TEST-* de ~500 MB, c'est bon! ✅")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)