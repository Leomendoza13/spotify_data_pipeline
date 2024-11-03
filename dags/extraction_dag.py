"""
DAG for the extraction part of the pipeline
"""

import json
from datetime import datetime, timedelta
import requests
from google.cloud import storage
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

VARIABLE_FILE = "/opt/airflow/config/ids.json"

countries_data = {
    "US": "USA",
    "GB": "United Kingdom",
    "FR": "France",
    "DE": "Germany",
    "IT": "Italy",
    "ES": "Spain",
    "BR": "Brazil",
    "CA": "Canada",
    "AU": "Australia",
    "NL": "Netherlands",
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "JP": "Japan",
    "MX": "Mexico",
    "AR": "Argentina",
    "CL": "Chile",
    "CO": "Colombia",
    "IN": "India",
    "ID": "Indonesia",
    "NZ": "New Zealand",
    "ZA": "South Africa",
    "SA": "Saudi Arabia",
    "AE": "UAE",
    "PH": "Philippines",
    "TR": "Turkey",
    "TH": "Thailand",
    "VN": "Vietnam",
    "SG": "Singapore",
    "MY": "Malaysia",
    "IE": "Ireland",
    "PT": "Portugal",
    "HU": "Hungary",
    "CZ": "Czech Republic",
    "SK": "Slovakia",
    "DK": "Denmark",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "GR": "Greece",
    "RO": "Romania",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "RS": "Serbia",
    "SI": "Slovenia",
    "EE": "Estonia",
    "LT": "Lithuania",
    "LV": "Latvia",
    "KR": "South Korea",
}

codes_array = list(countries_data.keys())
countries_array = list(countries_data.values())
countries_files_array = [country.lower() for country in countries_array]


def load_variables():
    """
    Load and set variables from a JSON file into the environment.

    This function reads a JSON file specified by the global `VARIABLE_FILE`
    constant and loads its contents. Each key-value pair in the JSON file is
    set as an environment variable using the `Variable.set()` method. Prints a
    success message when variables are loaded successfully and error messages
    if the file is not found or fails to decode.

    Raises:
        FileNotFoundError: If the specified JSON file does not exist.
        json.JSONDecodeError: If the JSON file cannot be decoded.
        Exception: For any other unexpected errors.
    """
    try:
        with open(VARIABLE_FILE, encoding="utf-8") as f:
            variables = json.load(f)
        for key, value in variables.items():
            Variable.set(key, value)
        print("Variables loaded successfully.")
    except FileNotFoundError:
        print("Error: The variables file was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode the JSON file.")
    except Exception as e:
        print(f"Unexpected error: {e}")


def get_access_token(**kwargs):
    """
    Obtain an access token from the Spotify API using client credentials.

    This function sends a POST request to the Spotify Accounts service to
    retrieve an access token for authentication. The client ID and client
    secret are fetched from stored environment variables using the `Variable.get()`
    method. The token is then pushed to XCom for use in downstream tasks. Prints
    a success message when the token is obtained and an error message if the
    request fails.

    Args:
        **kwargs: Arbitrary keyword arguments that include `ti` for interacting
                  with XCom (e.g., provided in an Airflow task context).

    Raises:
        requests.exceptions.RequestException: If the POST request fails.
        Exception: For any other unexpected errors.
    """
    try:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "grant_type": "client_credentials",
            "client_id": Variable.get("SPOTIFY_CLIENT_ID"),
            "client_secret": Variable.get("SPOTIFY_CLIENT_SECRET"),
        }
        res = requests.post(
            "https://accounts.spotify.com/api/token",
            data=payload,
            headers=headers,
            timeout=10,
        )
        token = res.json().get("access_token")
        if token:
            kwargs["ti"].xcom_push(key="access_token", value=token)
            print("Access token obtained successfully.")
        else:
            print("Error: Failed to obtain access token.")
    except requests.exceptions.RequestException as e:
        print(f"Error obtaining access token: {e}")


def get_top_50_country_ids(**kwargs):
    """
    Retrieve Spotify playlist IDs for the 'Top 50' playlists from various countries.

    This function pulls an access token from XCom, then queries the Spotify API to
    find playlist IDs for 'Top 50' playlists for a predefined list of country codes.
    The function searches for playlists whose names match 'Top 50' along with the
    specific country name and pushes the found playlist IDs to XCom for downstream
    tasks. Prints a success message when IDs are retrieved and handles errors with
    informative messages.

    Args:
        **kwargs: Arbitrary keyword arguments that include `ti` for interacting
                  with XCom (e.g., provided in an Airflow task context).

    Pushes:
        XCom key `playlist_ids`: A list of playlist IDs found for the 'Top 50'
        playlists in the specified countries.

    Raises:
        requests.exceptions.RequestException: If the GET request to the Spotify API fails.
        KeyError: If the expected keys are not present in the Spotify API response.
        Exception: For any other unexpected errors.
    """
    try:
        token = kwargs["ti"].xcom_pull(task_ids="get_access_token", key="access_token")
        if not token:
            print("Error: Access token is missing.")
            return

        headers = {"Authorization": f"Bearer {token}"}
        ids_array = []
        for i, code in enumerate(codes_array):
            params = {"q": "Top 50", "type": "playlist", "market": code}
            response = requests.get(
                "https://api.spotify.com/v1/search",
                params=params,
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()

            playlists = response.json().get("playlists", {}).get("items", [])
            for playlist in playlists:
                if (
                    "Top 50" in playlist["name"]
                    and countries_array[i] in playlist["name"]
                ):
                    ids_array.append(playlist["id"])
                    break

        kwargs["ti"].xcom_push(key="playlist_ids", value=ids_array)
        print(f"Playlist IDs for {len(ids_array)} countries retrieved successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving playlists: {e}")
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def save_tracks_to_gcs(**kwargs):
    """
    Save track data from Spotify playlists to Google Cloud Storage (GCS).

    This function retrieves an access token and a list of playlist IDs from
    XCom, then uses the Spotify API to fetch the track data for each playlist.
    The track data is then saved as JSON files to a specified GCS bucket.
    Prints success messages when data is uploaded and error messages when issues
    occur.

    Args:
        **kwargs: Arbitrary keyword arguments that include `ti` for interacting
                  with XCom (e.g., provided in an Airflow task context).

    Pushes:
        None

    Prints:
        Confirmation messages indicating that data for each playlist has been
        uploaded to GCS or error messages if any issue occurs.

    Raises:
        requests.exceptions.RequestException: If the GET request to the Spotify API fails.
        google.cloud.exceptions.GoogleCloudError: If an error occurs when interacting with GCS.
        Exception: For any other unexpected errors.
    """
    try:
        client = storage.Client()
        bucket = client.bucket("spotify-raw-playlist-bucket1")
        token = kwargs["ti"].xcom_pull(task_ids="get_access_token", key="access_token")
        if not token:
            print("Error: Missing access token or playlist IDs.")
            return
        playlist_ids = kwargs["ti"].xcom_pull(
            task_ids="get_top_50_country_ids", key="playlist_ids"
        )

        headers = {"Authorization": f"Bearer {token}"}
        for count, playlist_id in enumerate(playlist_ids, start=0):
            response = requests.get(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()

            track_data = response.json()
            filename = f"spotify_tracks_{countries_files_array[count]}.json"
            blob = bucket.blob(filename)
            blob.upload_from_string(
                json.dumps(track_data), content_type="application/json"
            )
            print(f"Data for playlist {playlist_id} uploaded to GCS as {filename}.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching track data: {e}")
    except storage.exceptions.GoogleCloudError as e:
        print(f"Error uploading data to GCS: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


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
    "spotify_top_50_playlist_extraction",
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
