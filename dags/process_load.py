"""
DAG for the processing and loading parts of the pipeline
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from config.process_load_utils import (
    process_data,
    upload_files_to_gcs,
    load_data_to_bigquery,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "process_load",
    default_args=default_args,
    description="Process and upload extracted Spotify playlist data",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # Sensor to wait for the `spotify_top_50_playlist_extraction` DAG
    wait_for_extraction = ExternalTaskSensor(
        task_id="wait_for_extraction",
        external_dag_id="extraction",
        external_task_id="save_tracks_to_gcs",  # Last task in extraction DAG
        timeout=600,  # 10 minutes timeout
        mode="reschedule",
        poke_interval=60,
    )

    # Task for data processing
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Task to upload processed files to GCS
    upload_files_task = PythonOperator(
        task_id="upload_files_to_gcs",
        python_callable=upload_files_to_gcs,
    )

    # Task to load data from GCS to BigQuery
    load_data_task = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=load_data_to_bigquery,
    )

    # Define task dependencies
    wait_for_extraction >> process_data_task >> upload_files_task >> load_data_task
