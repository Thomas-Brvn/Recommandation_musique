#!/usr/bin/env python3
"""
Configure un job automatique via AWS EventBridge Scheduler + Lambda.

Le job tourne chaque semaine (lundi 6h UTC) et :
1. Lance une instance EC2 t3.small
2. L'instance télécharge les nouveaux dumps ListenBrainz vers S3
3. L'instance se termine automatiquement

Coût : ~0.05$/semaine

Usage:
    python scripts/setup_scheduled_download.py           # créer/mettre à jour
    python scripts/setup_scheduled_download.py --delete  # supprimer
    python scripts/setup_scheduled_download.py --trigger # déclencher maintenant
"""
import os
import json
import time
import zipfile
import argparse
import tempfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

AWS_REGION    = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET     = os.getenv("S3_BUCKET_NAME", "brainz-data")
GITHUB_REPO   = "https://github.com/Thomas-Brvn/Recommandation_musique.git"
INSTANCE_TYPE = "t3.small"

LAMBDA_NAME   = "listenbrainz-download-trigger"
SCHEDULE_NAME = "listenbrainz-weekly-download"
LAMBDA_ROLE     = "Lambda-EC2-Launcher-Role"
SCHEDULER_ROLE  = "EventBridge-LB-Scheduler-Role"

# Schedule : tous les lundis à 6h UTC
SCHEDULE_EXPRESSION = "cron(0 6 ? * MON *)"


# ── Code de la Lambda ──────────────────────────────────────

LAMBDA_CODE = f"""
import os
import json
import boto3

REGION        = "{AWS_REGION}"
S3_BUCKET     = "{S3_BUCKET}"
INSTANCE_TYPE = "{INSTANCE_TYPE}"
GITHUB_REPO   = "{GITHUB_REPO}"

USER_DATA = '''#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1
echo "=== DÉMARRAGE TÉLÉCHARGEMENT ==="
date

sudo yum update -y -q
sudo yum install -y git python3-pip -q

mkdir -p /home/ec2-user/recommendation
cd /home/ec2-user/recommendation
git clone ''' + GITHUB_REPO + ''' .

python3 -m venv venv
source venv/bin/activate
pip install boto3 requests tqdm -q

S3_BUCKET_NAME=''' + S3_BUCKET + ''' python scripts/download_incrementals.py

echo "=== TERMINÉ ===" && date
echo "COMPLETED $(date)" > /tmp/done
aws s3 cp /tmp/done s3://''' + S3_BUCKET + '''/status/download_completed --region ''' + AWS_REGION + '''
sudo shutdown -h now
'''

def handler(event, context):
    ec2 = boto3.client("ec2", region_name=REGION)
    iam = boto3.client("iam", region_name=REGION)

    # Récupérer le profil IAM EC2 existant
    profile_name = "EC2-ML-Pipeline-Profile"
    try:
        iam.get_instance_profile(InstanceProfileName=profile_name)
    except Exception as e:
        return {{"statusCode": 500, "body": f"Profil IAM introuvable: {{e}}"}}

    # AMI Amazon Linux 2023
    resp = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {{"Name": "name",  "Values": ["al2023-ami-*-x86_64"]}},
            {{"Name": "state", "Values": ["available"]}},
        ]
    )
    ami_id = sorted(resp["Images"], key=lambda x: x["CreationDate"], reverse=True)[0]["ImageId"]

    response = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=INSTANCE_TYPE,
        MinCount=1, MaxCount=1,
        IamInstanceProfile={{"Name": profile_name}},
        UserData=USER_DATA,
        BlockDeviceMappings=[{{
            "DeviceName": "/dev/xvda",
            "Ebs": {{"VolumeSize": 30, "VolumeType": "gp3", "DeleteOnTermination": True}}
        }}],
        TagSpecifications=[{{
            "ResourceType": "instance",
            "Tags": [
                {{"Key": "Name",    "Value": "LB-Scheduled-Download"}},
                {{"Key": "Project", "Value": "MusicRecommendation"}},
            ]
        }}],
        InstanceInitiatedShutdownBehavior="terminate",
    )

    instance_id = response["Instances"][0]["InstanceId"]
    print(f"Instance lancée : {{instance_id}}")
    return {{"statusCode": 200, "body": f"Instance {{instance_id}} lancée"}}
"""


# ── Helpers IAM ───────────────────────────────────────────

def get_or_create_lambda_role(iam_client) -> str:
    """Crée le rôle IAM pour la Lambda (droits EC2 + IAM PassRole)."""
    try:
        resp = iam_client.get_role(RoleName=LAMBDA_ROLE)
        print(f"Rôle Lambda existant : {LAMBDA_ROLE}")
        return resp["Role"]["Arn"]
    except ClientError:
        pass

    print(f"Création du rôle Lambda : {LAMBDA_ROLE}")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow",
                       "Principal": {"Service": "lambda.amazonaws.com"},
                       "Action": "sts:AssumeRole"}]
    }
    resp = iam_client.create_role(
        RoleName=LAMBDA_ROLE,
        AssumeRolePolicyDocument=json.dumps(trust),
        Description="Lambda role to launch EC2 download instances"
    )
    role_arn = resp["Role"]["Arn"]

    for policy in [
        "arn:aws:iam::aws:policy/AmazonEC2FullAccess",
        "arn:aws:iam::aws:policy/IAMReadOnlyAccess",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    ]:
        iam_client.attach_role_policy(RoleName=LAMBDA_ROLE, PolicyArn=policy)

    # PassRole inline pour que Lambda puisse assigner le rôle EC2
    iam_client.put_role_policy(
        RoleName=LAMBDA_ROLE,
        PolicyName="PassEC2Role",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "iam:PassRole", "Resource": "*"}]
        })
    )

    time.sleep(10)  # propagation IAM
    return role_arn


def create_lambda_zip() -> bytes:
    """Crée le zip du code Lambda en mémoire."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        zip_path = f.name

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("lambda_function.py", LAMBDA_CODE)

    with open(zip_path, "rb") as f:
        return f.read()


def get_or_create_lambda(lambda_client, role_arn: str) -> str:
    """Crée ou met à jour la fonction Lambda."""
    zip_bytes = create_lambda_zip()

    try:
        lambda_client.get_function(FunctionName=LAMBDA_NAME)
        print(f"Mise à jour de la Lambda : {LAMBDA_NAME}")
        lambda_client.update_function_code(
            FunctionName=LAMBDA_NAME,
            ZipFile=zip_bytes
        )
        resp = lambda_client.get_function(FunctionName=LAMBDA_NAME)
        return resp["Configuration"]["FunctionArn"]

    except ClientError:
        print(f"Création de la Lambda : {LAMBDA_NAME}")
        resp = lambda_client.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=60,
            Description="Lance l'EC2 de téléchargement ListenBrainz",
        )
        return resp["FunctionArn"]


def get_or_create_scheduler_role(iam_client, lambda_arn: str) -> str:
    """Crée le rôle IAM pour EventBridge Scheduler (invoke Lambda)."""
    try:
        resp = iam_client.get_role(RoleName=SCHEDULER_ROLE)
        print(f"Rôle Scheduler existant : {SCHEDULER_ROLE}")
        return resp["Role"]["Arn"]
    except ClientError:
        pass

    print(f"Création du rôle Scheduler : {SCHEDULER_ROLE}")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow",
                       "Principal": {"Service": "scheduler.amazonaws.com"},
                       "Action": "sts:AssumeRole"}]
    }
    resp = iam_client.create_role(
        RoleName=SCHEDULER_ROLE,
        AssumeRolePolicyDocument=json.dumps(trust),
        Description="EventBridge Scheduler role to invoke Lambda"
    )
    role_arn = resp["Role"]["Arn"]

    iam_client.put_role_policy(
        RoleName=SCHEDULER_ROLE,
        PolicyName="InvokeLambda",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                           "Action": "lambda:InvokeFunction",
                           "Resource": lambda_arn}]
        })
    )
    time.sleep(10)
    return role_arn


def setup_eventbridge(scheduler_client, lambda_arn: str, scheduler_role_arn: str):
    """Crée ou met à jour la règle EventBridge Scheduler."""
    try:
        scheduler_client.get_schedule(Name=SCHEDULE_NAME)
        print(f"Mise à jour du schedule : {SCHEDULE_NAME}")
        action = scheduler_client.update_schedule
    except ClientError:
        print(f"Création du schedule : {SCHEDULE_NAME}")
        action = scheduler_client.create_schedule

    action(
        Name=SCHEDULE_NAME,
        ScheduleExpression=SCHEDULE_EXPRESSION,
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": lambda_arn,
            "RoleArn": scheduler_role_arn,
        },
        Description="Téléchargement hebdomadaire des dumps ListenBrainz",
        State="ENABLED",
    )


# ── Main ──────────────────────────────────────────────────

def setup():
    iam_client       = boto3.client("iam",                region_name=AWS_REGION)
    lambda_client    = boto3.client("lambda",             region_name=AWS_REGION)
    scheduler_client = boto3.client("scheduler",          region_name=AWS_REGION)

    print("=" * 50)
    print("CONFIGURATION DU JOB AUTOMATIQUE")
    print("=" * 50)
    print(f"Schedule : chaque lundi à 6h UTC")
    print(f"Bucket   : {S3_BUCKET}")
    print("=" * 50)

    role_arn           = get_or_create_lambda_role(iam_client)
    lambda_arn         = get_or_create_lambda(lambda_client, role_arn)
    scheduler_role_arn = get_or_create_scheduler_role(iam_client, lambda_arn)
    setup_eventbridge(scheduler_client, lambda_arn, scheduler_role_arn)

    print("\nJob configuré avec succès !")
    print(f"Schedule    : {SCHEDULE_EXPRESSION} (chaque lundi 6h UTC)")
    print(f"Lambda      : {LAMBDA_NAME}")
    print(f"Prochain run: lundi prochain à 6h UTC")


def delete():
    lambda_client    = boto3.client("lambda",   region_name=AWS_REGION)
    scheduler_client = boto3.client("scheduler", region_name=AWS_REGION)

    try:
        scheduler_client.delete_schedule(Name=SCHEDULE_NAME)
        print(f"Schedule supprimé : {SCHEDULE_NAME}")
    except ClientError:
        print(f"Schedule introuvable : {SCHEDULE_NAME}")

    try:
        lambda_client.delete_function(FunctionName=LAMBDA_NAME)
        print(f"Lambda supprimée : {LAMBDA_NAME}")
    except ClientError:
        print(f"Lambda introuvable : {LAMBDA_NAME}")


def trigger_now():
    """Déclenche la Lambda immédiatement (sans attendre le schedule)."""
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    print(f"Déclenchement immédiat de {LAMBDA_NAME}...")
    resp = lambda_client.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
    )
    payload = json.loads(resp["Payload"].read())
    print(f"Réponse : {payload}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete",  action="store_true", help="Supprimer le job")
    parser.add_argument("--trigger", action="store_true", help="Déclencher maintenant")
    args = parser.parse_args()

    if args.delete:
        delete()
    elif args.trigger:
        trigger_now()
    else:
        setup()


if __name__ == "__main__":
    main()
