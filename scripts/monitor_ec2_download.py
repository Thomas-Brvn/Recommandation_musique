#!/usr/bin/env python3
"""
Script pour monitorer le téléchargement sur EC2
Affiche les logs et le statut en temps réel
"""

import sys
import json
import time
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

def get_instance_status(instance_id, region):
    """Récupère le statut de l'instance"""
    cmd = f"aws ec2 describe-instances --instance-ids {instance_id} --region {region}"
    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code != 0:
        return None

    data = json.loads(stdout)
    instance = data['Reservations'][0]['Instances'][0]

    return {
        'state': instance['State']['Name'],
        'public_ip': instance.get('PublicIpAddress', 'N/A'),
        'launch_time': instance['LaunchTime'],
        'instance_type': instance['InstanceType']
    }

def get_console_output(instance_id, region):
    """Récupère les logs de la console"""
    cmd = f"aws ec2 get-console-output --instance-id {instance_id} --region {region} --output text"
    stdout, stderr, code = run_aws_command(cmd, check=False)

    if code == 0 and stdout:
        return stdout
    return None

def check_s3_files(bucket_name, region):
    """Vérifie les fichiers uploadés sur S3"""
    print("\n📦 Fichiers sur S3:")

    # MusicBrainz
    cmd = f"aws s3 ls s3://{bucket_name}/raw/musicbrainz/ --region {region} --human-readable"
    stdout, stderr, code = run_aws_command(cmd, check=False)
    if code == 0 and stdout:
        print("\n  MusicBrainz:")
        for line in stdout.strip().split('\n'):
            if line:
                print(f"    ✓ {line}")
    else:
        print("\n  MusicBrainz: Aucun fichier")

    # ListenBrainz
    cmd = f"aws s3 ls s3://{bucket_name}/raw/listenbrainz/ --region {region} --human-readable"
    stdout, stderr, code = run_aws_command(cmd, check=False)
    if code == 0 and stdout:
        print("\n  ListenBrainz:")
        for line in stdout.strip().split('\n'):
            if line:
                print(f"    ✓ {line}")
    else:
        print("\n  ListenBrainz: Aucun fichier")

def monitor_instance(instance_id, region, bucket_name=None):
    """Monitore l'instance en temps réel"""
    print("=" * 60)
    print(f"📊 Monitoring de l'instance {instance_id}")
    print("=" * 60)
    print("Appuyez sur Ctrl+C pour arrêter le monitoring\n")

    last_log_length = 0

    try:
        while True:
            # Statut de l'instance
            status = get_instance_status(instance_id, region)

            if not status:
                print("❌ Instance non trouvée ou erreur")
                break

            # Afficher le statut
            print(f"\r⏱️  État: {status['state']} | Type: {status['instance_type']} | IP: {status['public_ip']}", end='', flush=True)

            # Si l'instance est arrêtée ou terminée
            if status['state'] in ['stopped', 'terminated']:
                print(f"\n\n✅ Instance {status['state']}")
                if bucket_name:
                    check_s3_files(bucket_name, region)
                break

            # Récupérer les nouveaux logs
            logs = get_console_output(instance_id, region)
            if logs:
                current_length = len(logs)
                if current_length > last_log_length:
                    # Afficher seulement les nouveaux logs
                    new_logs = logs[last_log_length:]
                    if new_logs.strip():
                        print("\n" + "─" * 60)
                        print(new_logs)
                        print("─" * 60)
                    last_log_length = current_length

                    # Vérifier si terminé
                    if "Téléchargement terminé" in logs or "COMPLETED" in logs:
                        print("\n✅ Téléchargement terminé!")
                        if bucket_name:
                            check_s3_files(bucket_name, region)
                        print("\n💡 Vous pouvez maintenant terminer l'instance:")
                        print(f"   aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")
                        break

            time.sleep(10)  # Attendre 10 secondes avant la prochaine vérification

    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring arrêté")
        print("\nStatut actuel:")
        status = get_instance_status(instance_id, region)
        if status:
            print(f"  État: {status['state']}")
        if bucket_name:
            check_s3_files(bucket_name, region)

def load_instance_config():
    """Charge la configuration de l'instance"""
    config_file = Path("config/ec2_instance.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def load_aws_config():
    """Charge la configuration AWS"""
    config_file = Path("config/aws_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return None

def main():
    """Fonction principale"""
    # Récupérer l'instance ID depuis les arguments ou le fichier de config
    if len(sys.argv) > 1:
        instance_id = sys.argv[1]
        region = sys.argv[2] if len(sys.argv) > 2 else "eu-west-3"
    else:
        # Charger depuis le fichier
        instance_config = load_instance_config()
        if instance_config:
            instance_id = instance_config['instance_id']
            region = instance_config['region']
            print(f"✅ Configuration chargée: {instance_id}")
        else:
            print("❌ Aucune instance trouvée")
            print("\nUtilisation:")
            print("  python scripts/monitor_ec2_download.py <instance_id> [region]")
            sys.exit(1)

    # Charger la config AWS pour le bucket
    aws_config = load_aws_config()
    bucket_name = aws_config.get('bucket_name') if aws_config else None

    # Démarrer le monitoring
    monitor_instance(instance_id, region, bucket_name)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Arrêt du monitoring")
        sys.exit(0)