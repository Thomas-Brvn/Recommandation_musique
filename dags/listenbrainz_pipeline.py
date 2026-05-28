# dags/listenbrainz_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from datetime import datetime, timedelta
import os

# Configuration
MUSICBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
LISTENBRAINZ_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/"

MB_TABLES = ["artist", "recording", "release", "release-group"]

GCP_PROJECT    = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION     = os.getenv("GCP_REGION", "europe-north1")
GCS_RAW_LB     = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
GCS_RAW_MB     = os.getenv("GCS_BUCKET_RAW_MB", "brainz-raw-musicbrainz")
GCS_PROCESSED  = os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed")
DATAPROC_CLUSTER = "brainz-spark-cluster"

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
            gcs_path = f"gs://{GCS_RAW_MB}/{execution_date}/{table}.tar.xz"

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

            # Upload to GCS
            subprocess.run(['gsutil', 'cp', local_path, gcs_path], check=True)
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
        gcs_path = f"gs://{GCS_RAW_LB}/{execution_date}/{latest_dump}"

        # Download (peut prendre plusieurs heures pour ~50-100GB)
        subprocess.run(['wget', '-q', '-O', local_path, url], check=True)

        # Upload to GCS
        subprocess.run(['gsutil', '-o', 'GSUtil:parallel_composite_upload_threshold=150M',
                        'cp', local_path, gcs_path], check=True)

        return latest_dump

    download_lb = PythonOperator(
        task_id='download_listenbrainz',
        python_callable=download_listenbrainz_dump,
    )

    # === TASK 3: Créer le cluster Dataproc ===
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=GCP_PROJECT,
        cluster_config={
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-4',
                'disk_config': {'boot_disk_type': 'pd-ssd', 'boot_disk_size_gb': 100},
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-4',
                'disk_config': {'boot_disk_type': 'pd-ssd', 'boot_disk_size_gb': 100},
            },
            'software_config': {'image_version': '2.1-debian11'},
        },
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
    )

    # === TASK 4: Jobs Spark sur Dataproc ===
    extract_mb_job = DataprocSubmitJobOperator(
        task_id='extract_musicbrainz',
        job={
            'reference': {'project_id': GCP_PROJECT},
            'placement': {'cluster_name': DATAPROC_CLUSTER},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{GCS_PROCESSED}/scripts/extract_musicbrainz.py',
                'args': [
                    '--input',  f'gs://{GCS_RAW_MB}/{{{{ ds }}}}/',
                    '--output', f'gs://{GCS_PROCESSED}/extracted/mb/{{{{ ds }}}}/',
                ],
            },
        },
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    extract_lb_job = DataprocSubmitJobOperator(
        task_id='extract_listenbrainz',
        job={
            'reference': {'project_id': GCP_PROJECT},
            'placement': {'cluster_name': DATAPROC_CLUSTER},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{GCS_PROCESSED}/scripts/extract_listenbrainz.py',
                'args': [
                    '--input',  f'gs://{GCS_RAW_LB}/{{{{ ds }}}}/',
                    '--output', f'gs://{GCS_PROCESSED}/extracted/lb/{{{{ ds }}}}/',
                ],
            },
        },
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    process_job = DataprocSubmitJobOperator(
        task_id='process_data',
        job={
            'reference': {'project_id': GCP_PROJECT},
            'placement': {'cluster_name': DATAPROC_CLUSTER},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{GCS_PROCESSED}/scripts/process_data.py',
                'args': [
                    '--mb-input', f'gs://{GCS_PROCESSED}/extracted/mb/{{{{ ds }}}}/',
                    '--lb-input', f'gs://{GCS_PROCESSED}/extracted/lb/{{{{ ds }}}}/',
                    '--output',   f'gs://{GCS_PROCESSED}/{{{{ ds }}}}/',
                ],
                'properties': {
                    'spark.sql.shuffle.partitions': '200',
                    'spark.driver.memory': '8g',
                    'spark.executor.memory': '16g',
                },
            },
        },
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    features_job = DataprocSubmitJobOperator(
        task_id='generate_features',
        job={
            'reference': {'project_id': GCP_PROJECT},
            'placement': {'cluster_name': DATAPROC_CLUSTER},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{GCS_PROCESSED}/scripts/generate_features.py',
                'args': [
                    '--input',  f'gs://{GCS_PROCESSED}/{{{{ ds }}}}/',
                    '--output', f'gs://{GCS_PROCESSED}/features/{{{{ ds }}}}/',
                ],
            },
        },
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    # === TASK 5: Supprimer le cluster Dataproc ===
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=GCP_PROJECT,
        cluster_name=DATAPROC_CLUSTER,
        region=GCP_REGION,
        trigger_rule='all_done',  # Supprimer même si un job échoue
    )

    # Dépendances
    [download_mb, download_lb] >> create_cluster
    create_cluster >> [extract_mb_job, extract_lb_job] >> process_job >> features_job >> delete_cluster
