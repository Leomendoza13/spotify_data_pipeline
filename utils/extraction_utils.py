"""
File that contains functions for the extraction
"""

import json
import requests
import time
from config.config import COUNTRIES_DATA
from config.config import VARIABLE_FILE
from airflow.models import Variable
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

codes_array = list(COUNTRIES_DATA.keys())
countries_array = list(COUNTRIES_DATA.values())
countries_files_array = [country.lower() for country in countries_array]
genre_cache = {}


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

def get_artist_genre(artist_id, token):
    """
    Get the genre of an artist from Spotify using the artist's ID.
    
    Args:
        artist_id (str): The ID of the artist.
        token (str): The access token for Spotify API.

    Returns:
        list: A list of genres associated with the artist.
    """
    if artist_id in genre_cache:
        return genre_cache[artist_id]
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(
                f"https://api.spotify.com/v1/artists/{artist_id}",
                headers=headers,
                timeout=10,)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
            response = requests.get(f"https://api.spotify.com/v1/artists/{artist_id}", headers=headers, timeout=10)
        
        response.raise_for_status()
        artist_data = response.json()
        genres = artist_data.get('genres', [])
        genre_cache[artist_id] = genres
        return genres
    except requests.exceptions.RequestException as e:
        print(f"Error fetching genres for artist {artist_id}: {e}")
        return []

def save_tracks_to_gcs(**kwargs):
    """
    Save track data from Spotify playlists to Google Cloud Storage (GCS) with artist genre enrichment.

    This function retrieves an access token and a list of playlist IDs from XCom,
    then queries the Spotify API to fetch the track data for each playlist. Each track's
    associated artists are enriched with their genres before the data is saved as a JSON
    file to a specified GCS bucket.

    Args:
        **kwargs: Arbitrary keyword arguments that include `ti` for interacting with XCom 
                  (e.g., provided in an Airflow task context).

    Pushes:
        None

    Prints:
        Success messages indicating that data for each playlist has been successfully 
        uploaded to GCS, as well as error messages if any issues occur.

    Raises:
        requests.exceptions.RequestException: If the GET request to the Spotify API fails.
        google.cloud.exceptions.GoogleCloudError: If an error occurs when interacting with GCS.
        Exception: For any other unexpected errors.

    Process:
        - Retrieves an access token and playlist IDs from XCom.
        - Fetches track data for each playlist from the Spotify API.
        - Enriches track data with artist genres by making additional API calls for each artist.
        - Saves the enriched track data as a JSON file in GCS.

    Example usage:
        This function is designed to be called as part of an Airflow DAG and uses `kwargs`
        to access the task instance (ti) for XCom interactions.
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

            for item in track_data.get("items", []):
                if "track" in item and "artists" in item["track"]:
                    for artist in item['track']['artists']:
                        genres = get_artist_genre(artist['id'], token)
                        artist['genres'] = genres

            filename = f"spotify_tracks_{countries_files_array[count]}.json"
            blob = bucket.blob(filename)
            blob.upload_from_string(
                json.dumps(track_data), content_type="application/json"
            )           
            print(f"Data for playlist {playlist_id} uploaded to GCS as {filename}.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching track data: {e}")
    except GoogleCloudError as e:
        print(f"Error uploading data to GCS: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")