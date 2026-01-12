# dags/listenbrainz_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta

# Configuration
MUSICBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
LISTENBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/"

MB_TABLES = ["artist", "recording", "release", "release-group"]

S3_BUCKET = "your-bucket"
S3_RAW_PREFIX = "raw"
S3_EXTRACTED_PREFIX = "extracted"
S3_PROCESSED_PREFIX = "processed"

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'listenbrainz_full_pipeline',
    default_args=default_args,
    description='Pipeline complète ListenBrainz/MusicBrainz',
    schedule_interval='0 2 1,15 * *',  # 1er et 15 du mois à 2h
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['listenbrainz', 'musicbrainz', 'recommendation'],
) as dag:

    # === TASK 1: Download MusicBrainz dumps ===
    def download_musicbrainz_dumps(**context):
        import subprocess
        import os
        
        execution_date = context['ds']
        
        for table in MB_TABLES:
            url = f"{MUSICBRAINZ_BASE_URL}{table}.tar.xz"
            checksum_url = f"{MUSICBRAINZ_BASE_URL}SHA256SUMS"
            
            local_path = f"/tmp/{table}.tar.xz"
            s3_path = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}/mb/{execution_date}/{table}.tar.xz"
            
            # Download
            subprocess.run(['wget', '-q', '-O', local_path, url], check=True)
            
            # Verify checksum
            subprocess.run(['wget', '-q', '-O', '/tmp/SHA256SUMS', checksum_url], check=True)
            result = subprocess.run(
                f"grep {table}.tar.xz /tmp/SHA256SUMS | sha256sum -c",
                shell=True, capture_output=True
            )
            if result.returncode != 0:
                raise ValueError(f"Checksum failed for {table}")
            
            # Upload to S3
            subprocess.run(['aws', 's3', 'cp', local_path, s3_path], check=True)
            os.remove(local_path)
            
        return f"Downloaded {len(MB_TABLES)} MusicBrainz dumps"

    download_mb = PythonOperator(
        task_id='download_musicbrainz',
        python_callable=download_musicbrainz_dumps,
    )

    # === TASK 2: Download ListenBrainz dump ===
    def download_listenbrainz_dump(**context):
        import subprocess
        import requests
        import re
        
        execution_date = context['ds']
        
        # Find latest full dump
        response = requests.get(LISTENBRAINZ_BASE_URL)
        dumps = re.findall(r'listenbrainz-listens-dump-\d+-\d+-full\.tar\.zst', response.text)
        latest_dump = sorted(dumps)[-1]
        
        url = f"{LISTENBRAINZ_BASE_URL}{latest_dump}"
        local_path = f"/tmp/{latest_dump}"
        s3_path = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}/lb/{execution_date}/{latest_dump}"
        
        # Download (peut prendre plusieurs heures pour ~50-100GB)
        subprocess.run(['wget', '-q', '-O', local_path, url], check=True)
        
        # Upload to S3
        subprocess.run(['aws', 's3', 'cp', local_path, s3_path], check=True)
        
        return latest_dump

    download_lb = PythonOperator(
        task_id='download_listenbrainz',
        python_callable=download_listenbrainz_dump,
    )

    # === TASK 3: Extract and process with Spark ===
    spark_steps = [
        {
            'Name': 'Extract MusicBrainz JSON',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    f's3://{S3_BUCKET}/scripts/extract_musicbrainz.py',
                    '--input', f's3://{S3_BUCKET}/{S3_RAW_PREFIX}/mb/{{{{ ds }}}}/',
                    '--output', f's3://{S3_BUCKET}/{S3_EXTRACTED_PREFIX}/mb/{{{{ ds }}}}/',
                ]
            }
        },
        {
            'Name': 'Extract ListenBrainz JSON',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    f's3://{S3_BUCKET}/scripts/extract_listenbrainz.py',
                    '--input', f's3://{S3_BUCKET}/{S3_RAW_PREFIX}/lb/{{{{ ds }}}}/',
                    '--output', f's3://{S3_BUCKET}/{S3_EXTRACTED_PREFIX}/lb/{{{{ ds }}}}/',
                ]
            }
        },
        {
            'Name': 'Process and Join Data',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '--conf', 'spark.sql.shuffle.partitions=200',
                    '--conf', 'spark.driver.memory=8g',
                    '--conf', 'spark.executor.memory=16g',
                    f's3://{S3_BUCKET}/scripts/process_data.py',
                    '--mb-input', f's3://{S3_BUCKET}/{S3_EXTRACTED_PREFIX}/mb/{{{{ ds }}}}/',
                    '--lb-input', f's3://{S3_BUCKET}/{S3_EXTRACTED_PREFIX}/lb/{{{{ ds }}}}/',
                    '--output', f's3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/{{{{ ds }}}}/',
                ]
            }
        },
        {
            'Name': 'Generate Features',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    f's3://{S3_BUCKET}/scripts/generate_features.py',
                    '--input', f's3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/{{{{ ds }}}}/',
                    '--output', f's3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/features/{{{{ ds }}}}/',
                ]
            }
        },
    ]

    process_spark = EmrAddStepsOperator(
        task_id='process_with_spark',
        job_flow_id='{{ var.value.emr_cluster_id }}',  # Ou créer cluster on-demand
        steps=spark_steps,
    )

    wait_spark = EmrStepSensor(
        task_id='wait_spark_completion',
        job_flow_id='{{ var.value.emr_cluster_id }}',
        step_id='{{ task_instance.xcom_pull(task_ids="process_with_spark")[0] }}',
    )

    # Dépendances
    [download_mb, download_lb] >> process_spark >> wait_spark