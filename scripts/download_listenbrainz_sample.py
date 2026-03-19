#!/usr/bin/env python3
"""
Script pour télécharger un ÉCHANTILLON des données ListenBrainz
Télécharge seulement les 10 premiers fichiers Parquet (~1-2 GB) pour tester l'algorithme

Usage: python3 scripts/download_listenbrainz_sample.py
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

def create_sample_script(bucket_name):
    """Crée le script qui télécharge un échantillon"""
    script = f"""#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Téléchargement échantillon ListenBrainz"
echo "Date début: $(date)"
echo "=========================================="

# Installation
apt-get update
apt-get install -y awscli python3 python3-pip pv

# Installer pyarrow pour extraire les Parquet
pip3 install pyarrow pandas

# Configuration
BUCKET_NAME="{bucket_name}"
INPUT_FILE="listenbrainz-spark-dump-2382-20260101-000003-full.tar"
OUTPUT_DIR="/data/listenbrainz_sample"

# Créer répertoires
mkdir -p $OUTPUT_DIR
cd /data

echo ""
echo "Étape 1/4: Téléchargement du tar complet depuis S3..."
echo "=========================================="
aws s3 cp "s3://$BUCKET_NAME/raw/listenbrainz/$INPUT_FILE" /data/$INPUT_FILE --region eu-north-1

if [ $? -ne 0 ]; then
    echo "✗ Erreur téléchargement depuis S3"
    exit 1
fi

FILE_SIZE=$(du -h /data/$INPUT_FILE | cut -f1)
echo "✓ Fichier téléchargé: $FILE_SIZE"

echo ""
echo "Étape 2/4: Extraction des 10 premiers fichiers Parquet..."
echo "=========================================="

# Extraire seulement les 10 premiers fichiers Parquet
tar -xvf /data/$INPUT_FILE -C $OUTPUT_DIR \
    --wildcards '*/[0-9].parquet' \
    --wildcards '*/SCHEMA_SEQUENCE' \
    --wildcards '*/TIMESTAMP' \
    --wildcards '*/COPYING'

if [ $? -ne 0 ]; then
    echo "✗ Erreur extraction"
    exit 1
fi

echo "✓ Extraction terminée"

echo ""
echo "Étape 3/4: Analyse de l'échantillon..."
echo "=========================================="

# Compter les fichiers extraits
PARQUET_COUNT=$(find $OUTPUT_DIR -name "*.parquet" | wc -l)
echo "✓ Fichiers Parquet extraits: $PARQUET_COUNT"

# Analyser le premier fichier avec Python
cat > /data/analyze_sample.py << 'PYEOF'
import pyarrow.parquet as pq
import os
from pathlib import Path

sample_dir = Path("/data/listenbrainz_sample")
parquet_files = list(sample_dir.rglob("*.parquet"))

if parquet_files:
    first_file = parquet_files[0]
    print(f"\\nAnalyse de: {{first_file.name}}")

    table = pq.read_table(first_file)
    print(f"  Colonnes: {{table.column_names}}")
    print(f"  Nombre de lignes: {{len(table)}}")
    print(f"  Taille: {{first_file.stat().st_size / (1024*1024):.2f}} MB")

    # Afficher un échantillon
    df = table.to_pandas().head(5)
    print(f"\\nÉchantillon des données:")
    print(df)

    # Statistiques sur les timestamps
    if 'listened_at' in table.column_names:
        import pandas as pd
        df_full = table.to_pandas()
        df_full['date'] = pd.to_datetime(df_full['listened_at'], unit='s')
        print(f"\\nPlage de dates:")
        print(f"  Plus ancien: {{df_full['date'].min()}}")
        print(f"  Plus récent: {{df_full['date'].max()}}")
        print(f"  Total écoutes dans ce fichier: {{len(df_full):,}}")
else:
    print("Aucun fichier Parquet trouvé")
PYEOF

python3 /data/analyze_sample.py

echo ""
echo "Étape 4/4: Compression et upload vers S3..."
echo "=========================================="

# Compresser l'échantillon
cd $OUTPUT_DIR/..
tar -czf listenbrainz_sample.tar.gz listenbrainz_sample/

SAMPLE_SIZE=$(du -h listenbrainz_sample.tar.gz | cut -f1)
echo "✓ Échantillon créé: $SAMPLE_SIZE"

# Upload vers S3
aws s3 cp listenbrainz_sample.tar.gz "s3://$BUCKET_NAME/processed/listenbrainz/listenbrainz_sample.tar.gz" --region eu-north-1

if [ $? -eq 0 ]; then
    echo "✓ Upload réussi!"

    # Créer métadonnées
    cat > /data/sample_metadata.json << EOF
{{
  "source_file": "$INPUT_FILE",
  "output_file": "listenbrainz_sample.tar.gz",
  "parquet_files_extracted": $PARQUET_COUNT,
  "sample_size": "$SAMPLE_SIZE",
  "processed_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "description": "Échantillon des 10 premiers fichiers Parquet pour tests"
}}
EOF

    aws s3 cp /data/sample_metadata.json "s3://$BUCKET_NAME/processed/listenbrainz/sample_metadata.json" --region eu-north-1

    echo ""
    echo "=========================================="
    echo "✅ ÉCHANTILLON CRÉÉ AVEC SUCCÈS!"
    echo "=========================================="
    echo "Taille: $SAMPLE_SIZE"
    echo "Fichiers Parquet: $PARQUET_COUNT"
    echo "Localisation: s3://$BUCKET_NAME/processed/listenbrainz/listenbrainz_sample.tar.gz"
    echo "=========================================="

    echo "SUCCESS" > /tmp/sample-status
    aws s3 cp /tmp/sample-status "s3://$BUCKET_NAME/processed/.sample-completed"
else
    echo "✗ Erreur upload vers S3"
    exit 1
fi

echo ""
echo "Nettoyage..."
rm -f /data/$INPUT_FILE

echo ""
echo "Date fin: $(date)"
echo "=========================================="
"""

    return script

def main():
    """Fonction principale"""
    print("=" * 60)
    print("📦 Téléchargement d'un échantillon ListenBrainz")
    print("=" * 60)
    print("")
    print("Ce script va:")
    print("  1. Télécharger le fichier ListenBrainz complet depuis S3")
    print("  2. Extraire SEULEMENT les 10 premiers fichiers Parquet")
    print("  3. Analyser l'échantillon")
    print("  4. L'uploader vers S3 dans /processed/")
    print("")
    print("⏱️  Durée estimée: 30-45 minutes")
    print("💰 Coût estimé: ~$0.02-0.03 USD")
    print("📦 Taille échantillon: ~1-2 GB (au lieu de 127 GB)")
    print("=" * 60)

    # Charger la config
    config = load_config()
    if not config:
        print("❌ Configuration non trouvée")
        sys.exit(1)

    bucket_name = config.get("bucket_name")
    region = config.get("region")

    print(f"\n✅ Configuration")
    print(f"   Bucket: {bucket_name}")
    print(f"   Région: {region}")

    if len(sys.argv) <= 1:
        response = input("\nContinuer? (O/n): ")
        if response.lower() == 'n':
            print("❌ Annulé")
            sys.exit(0)

    # Générer le script
    user_data = create_sample_script(bucket_name)
    user_data_file = Path("/tmp/ec2-sample-listenbrainz.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        sys.exit(1)

    # Lancer l'instance avec 150 GB (suffisant pour tar + extraction)
    print(f"\n🚀 Lancement de l'instance EC2...")
    print(f"   Type: t3.small (2 GB RAM)")
    print(f"   Stockage: 150 GB")

    instance_profile = "EC2-S3-Access-Profile"
    instance_type = "t3.small"

    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {instance_type} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":150,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
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
    print("📊 Instance d'échantillonnage lancée!")
    print("=" * 60)
    print(f"Instance ID: {instance_id}")
    print(f"Région: {region}")
    print("")
    print("📋 Pour suivre la progression:")
    print("")
    print("  # Voir les logs:")
    print(f"  aws ec2 get-console-output --instance-id {instance_id} --region {region} --output text | tail -100")
    print("")
    print("  # Vérifier le résultat sur S3:")
    print(f"  aws s3 ls s3://{bucket_name}/processed/listenbrainz/ --region {region} --human-readable")
    print("")
    print("  # Terminer l'instance quand c'est fini:")
    print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")
    print("")
    print("⏱️  Le traitement prendra ~30-45 minutes")
    print("✅ Vous verrez 'listenbrainz_sample.tar.gz' sur S3 quand c'est terminé")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)