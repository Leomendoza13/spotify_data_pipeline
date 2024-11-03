"""
DAG for the extraction part of the pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config.extraction_utils import (
    load_variables,
    get_access_token,
    get_top_50_country_ids,
    save_tracks_to_gcs
)

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extraction",
    default_args=default_args,
    description="Extract Top 50 Spotify playlists and save to GCS",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # Task in Airflow

    t1 = PythonOperator(
        task_id="load_variables",
        python_callable=load_variables,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="get_access_token",
        python_callable=get_access_token,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="get_top_50_country_ids",
        python_callable=get_top_50_country_ids,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="save_tracks_to_gcs",
        python_callable=save_tracks_to_gcs,
        provide_context=True,
    )

    # Define the order
    t1 >> t2 >> t3 >> t4
