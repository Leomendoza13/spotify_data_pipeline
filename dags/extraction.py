from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from google.cloud import storage
import json

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_top_50_playlist_extraction',
    default_args=default_args,
    description='Extract Top 50 Spotify playlists and save to GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
)

# 1. Function to get token access
def get_access_token(**kwargs):
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": Variable.get("SPOTIFY_CLIENT_ID"),
        "client_secret": Variable.get("SPOTIFY_CLIENT_SECRET")
    }
    res = requests.post("https://accounts.spotify.com/api/token", data=payload, headers=headers)
    token = res.json().get('access_token')

    kwargs['ti'].xcom_push(key='access_token', value=token)

# 2. Fonction to get ids of top50 playlists
def get_top_50_country_ids(**kwargs):
    token = kwargs['ti'].xcom_pull(task_ids='get_access_token', key='access_token')
    codes_array = [
        "US", "GB", "FR", "DE", "IT", "ES", "BR", "CA", "AU", 
        "NL", "SE", "NO", "FI", "JP", "MX", "AR", "CL", 
        "CO", "IN", "ID", "NZ", "ZA", "SA", "AE", "PH",
         "TR", "TH", "VN", "SG",  "MY",
        "IE", "PT", "HU", "CZ", "SK", "DK", "BE", "CH", 
        "AT", "GR", "RO", "BG", "HR", "RS", "SI", "EE", 
        "LT", "LV", "KR"
    ]
    countries_array = ["USA", "United Kingdom", "France", "Germany", "Italy", 
                       "Spain", "Brazil", "Canada", "Australia", "Netherlands", 
                       "Sweden", "Norway", "Finland", "Japan", "Mexico", 
                       "Argentina", "Chile", "Colombia", "India", "Indonesia", 
                       "New Zealand", "South Africa", "Saudi Arabia", "UAE", 
                       "Philippines", "Turkey", "Thailand", "Vietnam", 
                       "Singapore", "Malaysia", "Ireland", "Portugal", 
                       "Hungary", "Czech Republic", "Slovakia", "Denmark", 
                       "Belgium", "Switzerland", "Austria", "Greece", 
                       "Romania", "Bulgaria", "Croatia", "Serbia", "Slovenia", 
                       "Estonia", "Lithuania", "Latvia", "South Korea"]

    headers = {'Authorization': f'Bearer {token}'}
    ids_array = []
    for i, code in enumerate(codes_array):
        params = {'q': 'Top 50', 'type': 'playlist', 'market': code}
        response = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
        if response.status_code == 200:
            playlists = response.json().get('playlists', {}).get('items', [])
            for playlist in playlists:
                if "Top 50" in playlist['name'] and countries_array[i] in playlist['name']:
                    ids_array.append(playlist['id'])
                    break
    
    kwargs['ti'].xcom_push(key='playlist_ids', value=ids_array)

# 3. Function to get tracks and store in GCS
def save_tracks_to_gcs(**kwargs):
    client = storage.Client()
    bucket = client.bucket('spotify-raw-playlist-bucket1')
    token = kwargs['ti'].xcom_pull(task_ids='get_access_token', key='access_token')
    playlist_ids = kwargs['ti'].xcom_pull(task_ids='get_top_50_country_ids', key='playlist_ids')

    headers = {'Authorization': f'Bearer {token}'}
    for playlist_id in playlist_ids:
        response = requests.get(f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks", headers=headers)
        track_data = response.json()
        filename = f"spotify_tracks_{playlist_id}.json"
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(track_data), content_type='application/json')
        print(f"Données pour la playlist {playlist_id} chargées dans GCS sous le nom {filename}")

# Task in Airflow
t1 = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='get_top_50_country_ids',
    python_callable=get_top_50_country_ids,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='save_tracks_to_gcs',
    python_callable=save_tracks_to_gcs,
    provide_context=True,
    dag=dag,
)

# Define the order
t1 >> t2 >> t3