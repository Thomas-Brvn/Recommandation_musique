#!/usr/bin/env python3
"""
Script pour lancer une instance EC2 qui télécharge les données directement vers S3
Avantages:
- Pas de téléchargement local (pas de 100 GB sur votre machine)
- Bande passante AWS (beaucoup plus rapide)
- Transfert gratuit EC2 -> S3 dans la même région
"""

import sys
import json
import time
import subprocess
from pathlib import Path

# Configuration
DEFAULT_INSTANCE_TYPE = "t3.medium"  # 2 vCPU, 4 GB RAM - ~0.05 USD/heure
FREE_TIER_INSTANCE_TYPE = "t2.micro"  # 1 vCPU, 1 GB RAM - Free Tier eligible
DEFAULT_REGION = "eu-west-3"

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

    # Chercher l'AMI Ubuntu 22.04 LTS officielle
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
                print(f"✅ AMI trouvée: {ami_id} ({ami_name})")
                return ami_id
        except:
            pass

    print("⚠️  Impossible de trouver l'AMI automatiquement")
    print("💡 Vous pouvez trouver l'AMI Ubuntu 22.04 pour votre région sur:")
    print("   https://cloud-images.ubuntu.com/locator/ec2/")

    ami_id = input(f"\nEntrez l'AMI ID pour {region} (ou laissez vide pour annuler): ").strip()
    if not ami_id:
        return None

    return ami_id

def load_config():
    """Charge la configuration AWS"""
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def create_user_data_script(bucket_name, download_mb=True, download_lb=True):
    """Crée le script qui sera exécuté au démarrage de l'instance"""
    script = f"""#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Début du téléchargement des données"
echo "Date: $(date)"
echo "=========================================="

# Mettre à jour le système
apt-get update
apt-get install -y wget awscli python3-pip

# Créer un répertoire de travail
mkdir -p /data/musicbrainz /data/listenbrainz
cd /data

# Configuration
BUCKET_NAME="{bucket_name}"
MB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
LB_BASE_URL="https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/"

MB_TABLES="artist recording release release-group"

# Trouver la dernière version MusicBrainz
echo "Recherche de la dernière version MusicBrainz..."
MB_LATEST=$(curl -s "$MB_BASE_URL" | grep -o 'href="[0-9]*-[0-9]*/"' | tail -1 | cut -d'"' -f2)
MUSICBRAINZ_URL="${{MB_BASE_URL}}${{MB_LATEST}}"
echo "Version MusicBrainz: $MB_LATEST"

echo "=========================================="
echo "Bucket S3: $BUCKET_NAME"
echo "=========================================="
"""

    if download_mb:
        script += """
echo "=========================================="
echo "Téléchargement MusicBrainz"
echo "=========================================="

for table in $MB_TABLES; do
    echo "Téléchargement de $table..."
    wget -q --show-progress -O "/data/musicbrainz/$table.tar.xz" "$MUSICBRAINZ_URL$table.tar.xz"

    if [ $? -eq 0 ]; then
        echo "✓ $table téléchargé"
        echo "Upload vers S3..."
        aws s3 cp "/data/musicbrainz/$table.tar.xz" "s3://$BUCKET_NAME/raw/musicbrainz/$table.tar.xz"

        if [ $? -eq 0 ]; then
            echo "✓ $table uploadé vers S3"
            # Supprimer le fichier local pour économiser l'espace
            rm "/data/musicbrainz/$table.tar.xz"
        else
            echo "✗ Erreur upload $table"
        fi
    else
        echo "✗ Erreur téléchargement $table"
    fi
done
"""

    if download_lb:
        script += r"""
echo "=========================================="
echo "Téléchargement ListenBrainz"
echo "=========================================="

# Trouver le dernier dump
echo "Recherche du dernier dump ListenBrainz..."
LB_LATEST_DIR=$(curl -s "$LB_BASE_URL" | grep -o 'href="listenbrainz-dump-[0-9]*-[0-9]*-[0-9]*-full/"' | tail -1 | cut -d'"' -f2)

if [ -z "$LB_LATEST_DIR" ]; then
    echo "✗ Aucun dump trouvé"
else
    echo "Dernier dump directory: $LB_LATEST_DIR"
    LISTENBRAINZ_URL="${LB_BASE_URL}${LB_LATEST_DIR}"

    # Trouver le fichier tar dans ce répertoire
    LATEST_DUMP=$(curl -s "$LISTENBRAINZ_URL" | grep -o 'href="listenbrainz-spark-dump-[^"]*\.tar"' | head -1 | cut -d'"' -f2)

    if [ -z "$LATEST_DUMP" ]; then
        echo "✗ Aucun fichier tar trouvé"
    else
        echo "Fichier: $LATEST_DUMP"
        echo "Téléchargement (peut prendre plusieurs heures, ~128GB)..."

        wget -q --show-progress -O "/data/listenbrainz/$LATEST_DUMP" "${LISTENBRAINZ_URL}${LATEST_DUMP}"

        if [ $? -eq 0 ]; then
            echo "✓ ListenBrainz téléchargé"
            echo "Upload vers S3..."
            aws s3 cp "/data/listenbrainz/$LATEST_DUMP" "s3://$BUCKET_NAME/raw/listenbrainz/$LATEST_DUMP"

            if [ $? -eq 0 ]; then
                echo "✓ ListenBrainz uploadé vers S3"
                rm "/data/listenbrainz/$LATEST_DUMP"
            else
                echo "✗ Erreur upload ListenBrainz"
            fi
        else
            echo "✗ Erreur téléchargement ListenBrainz"
        fi
    fi
fi
"""

    script += """
echo "=========================================="
echo "Téléchargement terminé"
echo "Date: $(date)"
echo "=========================================="

# Créer un fichier de statut
echo "COMPLETED" > /tmp/download-status
aws s3 cp /tmp/download-status "s3://$BUCKET_NAME/raw/.download-completed"

# L'instance peut maintenant être arrêtée
# (vous pouvez la terminer manuellement ou configurer l'auto-shutdown)
"""

    return script

def create_instance(region, bucket_name, download_mb, download_lb):
    """Crée et lance l'instance EC2"""
    print(f"\n🚀 Lancement de l'instance EC2...")
    print(f"   Région: {region}")
    print(f"   Type: {DEFAULT_INSTANCE_TYPE}")
    print(f"   Télécharger MusicBrainz: {'Oui' if download_mb else 'Non'}")
    print(f"   Télécharger ListenBrainz: {'Oui' if download_lb else 'Non'}")

    # Générer le user data script
    user_data = create_user_data_script(bucket_name, download_mb, download_lb)
    user_data_file = Path("/tmp/ec2-user-data.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI pour la région
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        return None

    # Créer un rôle IAM pour l'accès S3 si nécessaire
    print("\n📋 Vérification du rôle IAM...")
    role_name = "EC2-S3-Access-Role"
    instance_profile = "EC2-S3-Access-Profile"

    # Vérifier si le profil existe
    stdout, stderr, code = run_aws_command(
        f"aws iam get-instance-profile --instance-profile-name {instance_profile}",
        check=False
    )

    if code != 0:
        print("⚠️  Profil IAM non trouvé. Création...")
        print("💡 Ce profil permet à l'instance EC2 d'accéder à S3")

        # Créer le rôle
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        trust_file = Path("/tmp/trust-policy.json")
        with open(trust_file, 'w') as f:
            json.dump(trust_policy, f)

        run_aws_command(
            f"aws iam create-role --role-name {role_name} --assume-role-policy-document file://{trust_file}",
            check=False
        )

        # Attacher la politique S3
        run_aws_command(
            f"aws iam attach-role-policy --role-name {role_name} --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess",
            check=False
        )

        # Créer le profil d'instance
        run_aws_command(
            f"aws iam create-instance-profile --instance-profile-name {instance_profile}",
            check=False
        )

        # Ajouter le rôle au profil
        run_aws_command(
            f"aws iam add-role-to-instance-profile --instance-profile-name {instance_profile} --role-name {role_name}",
            check=False
        )

        print("✅ Profil IAM créé")
        print("⏳ Attente de la propagation du profil (30 secondes)...")
        time.sleep(30)
    else:
        print("✅ Profil IAM existant trouvé")

    # Utiliser directement t3.medium (plus fiable)
    instance_type = DEFAULT_INSTANCE_TYPE
    print(f"\n💡 Instance type: {instance_type}")
    print(f"   Coût estimé: ~0.20 USD pour tout le téléchargement")
    print(f"\n🚀 Lancement de l'instance {instance_type}...")

    # Essayer d'abord avec t3.small (moins restrictif que t3.medium)
    # Si t3.small échoue aussi, on utilisera t2.small
    print(f"💡 Tentative avec t3.small (2 vCPU, 2 GB RAM, ~0.025 USD/h)...")
    instance_type = "t3.small"

    # Lancer l'instance avec un volume EBS de 150 GB
    # (nécessaire pour ListenBrainz qui fait 128 GB compressé)
    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {instance_type} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":150,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
        --region {region}"""

    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code != 0:
        print(f"❌ Erreur lors du lancement: {stderr}")

        # Si erreur Free Tier, suggérer t3.medium
        if "free-tier-eligible" in stderr.lower() or "InvalidParameterCombination" in stderr:
            print("\n⚠️  L'instance demandée n'est pas disponible.")
            print("💡 Suggestions:")
            if instance_type == DEFAULT_INSTANCE_TYPE:
                print(f"  - Essayez t2.micro (Free Tier) - relancez et choisissez option 1")
            else:
                print(f"  - Essayez t3.medium (~0.05 USD/h) - relancez et choisissez option 2")
            print(f"  - Vérifiez que votre compte AWS supporte ce type d'instance dans {region}")

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
    print(f"  aws ec2 get-console-output --instance-id {instance_id} --region {region}")
    print(f"\n  # Voir le statut:")
    print(f"  aws ec2 describe-instances --instance-ids {instance_id} --region {region}")
    print(f"\n  # Arrêter l'instance:")
    print(f"  aws ec2 stop-instances --instance-ids {instance_id} --region {region}")
    print(f"\n  # Terminer l'instance (supprimer):")
    print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")

    print("\n💡 Monitoring automatique:")
    print(f"  python scripts/monitor_ec2_download.py {instance_id}")

    print("\n⏱️  Durée estimée:")
    print("  • MusicBrainz: 15-30 minutes")
    print("  • ListenBrainz: 2-4 heures")
    print("\n💰 Coût estimé: ~0.20 USD (instance t3.medium)")
    print("\n⚠️  N'oubliez pas de terminer l'instance après le téléchargement!")
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
    print("🚀 Téléchargement des données via EC2")
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
        print("⚠️  Configuration non trouvée")
        bucket_name = input("Nom du bucket S3: ").strip()
        region = input(f"Région [{DEFAULT_REGION}]: ").strip() or DEFAULT_REGION

    if not bucket_name:
        print("❌ Nom du bucket requis")
        sys.exit(1)

    # Demander ce qu'il faut télécharger
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        print("\n📦 Que souhaitez-vous télécharger?")
        print("  1. MusicBrainz uniquement (~7 GB, 15-30 min)")
        print("  2. ListenBrainz uniquement (~100 GB, 2-4h)")
        print("  3. Les deux (~107 GB, 2-4h)")
        choice = input("Votre choix (1/2/3): ").strip()

    download_mb = choice in ['1', '3']
    download_lb = choice in ['2', '3']

    if not download_mb and not download_lb:
        print("❌ Choix invalide")
        sys.exit(1)

    # Estimation des coûts
    print("\n💰 Estimation des coûts:")
    if choice == '1':
        print("  • Instance t3.small: ~0.03 USD")
        print("  • Stockage S3: ~0.16 USD/mois")
    elif choice == '2':
        print("  • Instance t3.small: ~0.20 USD")
        print("  • Stockage S3: ~2.30 USD/mois")
    else:
        print("  • Instance t3.small: ~0.20 USD")
        print("  • Stockage S3: ~2.46 USD/mois")

    if len(sys.argv) <= 1:
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("❌ Annulé")
            sys.exit(0)

    # Lancer l'instance
    instance_id = create_instance(region, bucket_name, download_mb, download_lb)

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