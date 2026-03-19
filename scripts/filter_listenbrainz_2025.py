#!/usr/bin/env python3
"""
Script pour filtrer les données ListenBrainz de l'année 2025 directement dans le cloud
Lance une instance EC2 qui:
1. Télécharge le fichier tar depuis S3 (~127 GB)
2. Extrait les fichiers Parquet (format Apache Spark)
3. Lit et filtre les données avec pyarrow/pandas
4. Garde uniquement les écoutes de 2025
5. Sauvegarde le résultat en Parquet sur S3
6. Se termine automatiquement

Format: ListenBrainz Spark dump = fichiers Parquet organisés par année/mois
2025 = timestamps entre 1704067200 (2025-01-01 00:00:00) et 1735689599 (2025-12-31 23:59:59)
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

def create_filter_script(bucket_name):
    """Crée le script qui filtre les données de 2025"""
    script = f"""#!/bin/bash

# Log toutes les commandes
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "=========================================="
echo "Filtrage ListenBrainz 2025"
echo "Date début: $(date)"
echo "=========================================="

# Installation des outils
apt-get update
apt-get install -y awscli python3 python3-pip jq pv

# Installer pyarrow pour lire les fichiers Parquet
pip3 install pyarrow pandas

# Configuration
BUCKET_NAME="{bucket_name}"
INPUT_FILE="listenbrainz-spark-dump-2382-20260101-000003-full.tar"
OUTPUT_FILE="listenbrainz-2025-only.tar"

# Timestamps pour 2025
# 2025-01-01 00:00:00 UTC = 1704067200
# 2025-12-31 23:59:59 UTC = 1735689599
START_TIMESTAMP=1704067200
END_TIMESTAMP=1735689599

echo "Filtrage des écoutes entre $START_TIMESTAMP et $END_TIMESTAMP"
echo "=========================================="

# Créer répertoires
mkdir -p /data/input /data/output /data/temp
cd /data

echo "Étape 1/5: Téléchargement depuis S3..."
echo "=========================================="
aws s3 cp "s3://$BUCKET_NAME/raw/listenbrainz/$INPUT_FILE" /data/input/$INPUT_FILE --region eu-north-1

if [ $? -ne 0 ]; then
    echo "✗ Erreur téléchargement depuis S3"
    exit 1
fi

FILE_SIZE=$(du -h /data/input/$INPUT_FILE | cut -f1)
echo "✓ Fichier téléchargé: $FILE_SIZE"

echo ""
echo "Étape 2/5: Extraction et analyse..."
echo "=========================================="

# Le tar contient des fichiers Parquet (format Apache Spark)
echo "Liste du contenu du tar (premiers 20 fichiers):"
tar -tf /data/input/$INPUT_FILE | head -20

echo ""
echo "Recherche de fichiers Parquet..."
PARQUET_COUNT=$(tar -tf /data/input/$INPUT_FILE | grep -c '\.parquet$' || echo "0")
echo "✓ Fichiers Parquet trouvés: $PARQUET_COUNT"

echo ""
echo "Étape 3/5: Filtrage par date (2025)..."
echo "=========================================="

# Créer un script Python pour filtrer les fichiers Parquet
cat > /data/filter_2025.py << 'PYEOF'
import sys
import tarfile
import os
import tempfile
import shutil

try:
    import pyarrow.parquet as pq
    import pandas as pd
except ImportError:
    print("Erreur: pyarrow ou pandas non installé")
    sys.exit(1)

START_TS = 1704067200
END_TS = 1735689599

input_tar = "/data/input/listenbrainz-spark-dump-2382-20260101-000003-full.tar"
output_dir = "/data/output"
temp_dir = "/data/temp"

print(f"Ouverture du tar: {{input_tar}}")
print(f"Filtrage timestamps: {{START_TS}} - {{END_TS}}")
print(f"Format: Parquet (Apache Spark)")
print("")

total_files = 0
processed_files = 0
filtered_count = 0
error_count = 0
parquet_files = 0

try:
    with tarfile.open(input_tar, 'r') as tar:
        members = tar.getmembers()
        total_files = len(members)
        print(f"Total fichiers dans tar: {{total_files}}")
        print("")

        for i, member in enumerate(members):
            if i % 10 == 0:
                print(f"Progression: {{i}}/{{total_files}} fichiers traités... ({{parquet_files}} Parquet, {{filtered_count}} écoutes 2025)")

            # Chercher les fichiers Parquet
            if member.isfile() and member.name.endswith('.parquet'):
                parquet_files += 1
                try:
                    # Extraire temporairement le fichier Parquet
                    temp_path = os.path.join(temp_dir, f"temp_{{i}}.parquet")

                    with tar.extractfile(member) as f:
                        with open(temp_path, 'wb') as temp_f:
                            temp_f.write(f.read())

                    # Lire le fichier Parquet avec pyarrow
                    table = pq.read_table(temp_path)
                    df = table.to_pandas()

                    # Afficher le schéma au premier fichier
                    if parquet_files == 1:
                        print(f"\\nSchéma du Parquet détecté:")
                        print(f"  Colonnes: {{list(df.columns)}}")
                        print(f"  Nombre de lignes: {{len(df)}}")
                        print("")

                    # Filtrer par timestamp
                    # Chercher la colonne de timestamp (peut être 'listened_at', 'timestamp', 'ts', etc.)
                    timestamp_col = None
                    for col in ['listened_at', 'timestamp', 'ts', 'created']:
                        if col in df.columns:
                            timestamp_col = col
                            break

                    if timestamp_col:
                        # Filtrer les données de 2025
                        df_filtered = df[(df[timestamp_col] >= START_TS) & (df[timestamp_col] <= END_TS)]

                        if len(df_filtered) > 0:
                            filtered_count += len(df_filtered)

                            # Sauvegarder en Parquet
                            output_file = os.path.join(output_dir, member.name)
                            os.makedirs(os.path.dirname(output_file), exist_ok=True)

                            df_filtered.to_parquet(output_file, compression='snappy')
                    else:
                        print(f"Attention: Pas de colonne timestamp dans {{member.name}}")

                    # Nettoyer le fichier temporaire
                    os.remove(temp_path)
                    processed_files += 1

                except Exception as e:
                    print(f"Erreur traitement {{member.name}}: {{e}}")
                    error_count += 1
                    if os.path.exists(temp_path):
                        os.remove(temp_path)

    print("")
    print("========================================")
    print("Statistiques de filtrage:")
    print(f"  Total fichiers dans tar: {{total_files}}")
    print(f"  Fichiers Parquet trouvés: {{parquet_files}}")
    print(f"  Fichiers traités: {{processed_files}}")
    print(f"  Écoutes 2025 trouvées: {{filtered_count:,}}")
    print(f"  Erreurs: {{error_count}}")
    print("========================================")

except Exception as e:
    print(f"Erreur fatale: {{e}}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYEOF

echo "Lancement du filtrage..."
python3 /data/filter_2025.py

if [ $? -ne 0 ]; then
    echo "✗ Erreur lors du filtrage"
    exit 1
fi

echo ""
echo "Étape 4/5: Compression des données 2025..."
echo "=========================================="

# Compresser les données filtrées
cd /data/output
tar -cf /data/$OUTPUT_FILE .
cd /data

OUTPUT_SIZE=$(du -h /data/$OUTPUT_FILE | cut -f1)
echo "✓ Fichier 2025 créé: $OUTPUT_SIZE"

echo ""
echo "Étape 5/5: Upload vers S3..."
echo "=========================================="

aws s3 cp /data/$OUTPUT_FILE "s3://$BUCKET_NAME/processed/listenbrainz/$OUTPUT_FILE" --region eu-north-1

if [ $? -eq 0 ]; then
    echo "✓ Upload réussi!"

    # Créer un fichier de métadonnées
    cat > /data/metadata.json << EOF
{{
  "source_file": "$INPUT_FILE",
  "output_file": "$OUTPUT_FILE",
  "filter_start_timestamp": $START_TIMESTAMP,
  "filter_end_timestamp": $END_TIMESTAMP,
  "filter_year": 2025,
  "processed_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "output_size": "$OUTPUT_SIZE"
}}
EOF

    aws s3 cp /data/metadata.json "s3://$BUCKET_NAME/processed/listenbrainz/metadata-2025.json" --region eu-north-1

    echo ""
    echo "=========================================="
    echo "✅ FILTRAGE TERMINÉ AVEC SUCCÈS!"
    echo "=========================================="
    echo "Fichier créé: $OUTPUT_FILE ($OUTPUT_SIZE)"
    echo "Localisation S3: s3://$BUCKET_NAME/processed/listenbrainz/$OUTPUT_FILE"
    echo "Métadonnées: s3://$BUCKET_NAME/processed/listenbrainz/metadata-2025.json"
    echo "=========================================="

    # Marqueur de succès
    echo "SUCCESS" > /tmp/filter-status
    aws s3 cp /tmp/filter-status "s3://$BUCKET_NAME/processed/.filter-completed"
else
    echo "✗ Erreur upload vers S3"
    exit 1
fi

echo ""
echo "Nettoyage..."
rm -rf /data/input /data/temp

echo ""
echo "Date fin: $(date)"
echo "=========================================="
"""

    return script

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🔍 Filtrage ListenBrainz 2025 dans le cloud")
    print("=" * 60)
    print("")
    print("Ce script va:")
    print("  1. Télécharger le fichier ListenBrainz depuis S3 (~127 GB)")
    print("  2. Filtrer uniquement les écoutes de 2025")
    print("  3. Créer un nouveau fichier avec les données 2025")
    print("  4. L'uploader vers S3 dans /processed/")
    print("")
    print("⏱️  Durée estimée: 1-2 heures")
    print("💰 Coût estimé: ~0.05-0.10 USD")
    print("📦 Taille attendue: ~10-20 GB (au lieu de 127 GB)")
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
    user_data = create_filter_script(bucket_name)
    user_data_file = Path("/tmp/ec2-filter-2025.sh")
    with open(user_data_file, 'w') as f:
        f.write(user_data)

    # Obtenir l'AMI
    ami_id = get_ubuntu_ami(region)
    if not ami_id:
        print("❌ Impossible de trouver une AMI")
        sys.exit(1)

    # Lancer l'instance avec 200 GB (pour le fichier complet + processing)
    print(f"\n🚀 Lancement de l'instance EC2...")
    print(f"   Type: t3.small (2 GB RAM)")
    print(f"   Stockage: 200 GB")

    instance_profile = "EC2-S3-Access-Profile"
    instance_type = "t3.small"

    cmd = f"""aws ec2 run-instances \
        --image-id {ami_id} \
        --instance-type {instance_type} \
        --iam-instance-profile Name={instance_profile} \
        --user-data file://{user_data_file} \
        --block-device-mappings '[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":200,"VolumeType":"gp3","DeleteOnTermination":true}}}}]' \
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
    print("📊 Instance de filtrage lancée!")
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
    print("⏱️  Le traitement peut prendre 1-2 heures")
    print("✅ Vous verrez 'listenbrainz-2025-only.tar' sur S3 quand c'est terminé")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Opération interrompue")
        sys.exit(1)