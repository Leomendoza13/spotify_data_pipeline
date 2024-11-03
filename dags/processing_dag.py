from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import process  # assuming the process function is in this module

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spotify_top_50_playlist_processing",
    default_args=default_args,
    description="Process and upload extracted Spotify playlist data",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
)

# Sensor to wait for the `spotify_top_50_playlist_extraction` DAG
wait_for_extraction = ExternalTaskSensor(
    task_id="wait_for_extraction",
    external_dag_id="spotify_top_50_playlist_extraction",
    external_task_id="save_tracks_to_gcs",  # Last task in extraction DAG
    dag=dag,
    timeout=600,  # 10 minutes timeout
    mode="reschedule",
    poke_interval=60,
)

# Task for data processing
process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process.process_data,
    dag=dag,
)

# Task to upload processed files to GCS
upload_files_task = PythonOperator(
    task_id="upload_files_to_gcs",
    python_callable=process.upload_files_to_gcs,
    dag=dag,
)

# Task to load data from GCS to BigQuery
load_data_task = PythonOperator(
    task_id="load_data_to_bigquery",
    python_callable=process.load_data_to_bigquery,
    dag=dag,
)

# Define task dependencies
wait_for_extraction >> process_data_task >> upload_files_task >> load_data_task
