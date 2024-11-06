"""
File that contains functions for the extraction process from Spotify API.
"""

import logging
from random import uniform
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
    Load and set variables from a JSON file into Airflow's environment variables.

    Reads a JSON file specified by `VARIABLE_FILE`, loading each key-value pair
    into the Airflow environment. Prints a success message if successful,
    and error messages if issues occur.

    Raises:
        FileNotFoundError: If the JSON file is missing.
        json.JSONDecodeError: If the file content is not valid JSON.
        Exception: For any other unexpected errors.
    """
    try:
        with open(VARIABLE_FILE, encoding="utf-8") as f:
            variables = json.load(f)
        for key, value in variables.items():
            Variable.set(key, value)
        with open('/opt/airflow/config/project_id.json', encoding="utf-8") as f:
            variable = json.load(f)
        for key, value in variable.items():
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

    Sends a POST request to Spotify's Accounts service to retrieve an access
    token, using stored client ID and secret. Pushes the token to XCom for
    downstream tasks.

    Args:
        **kwargs: Keyword arguments including `ti` for Airflow XCom interaction.

    Raises:
        requests.exceptions.RequestException: If the POST request fails.
        Exception: For other unexpected errors.
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
    Retrieve Spotify playlist IDs for 'Top 50' playlists across specified countries.

    Pulls an access token from XCom and uses it to search Spotify's API
    for 'Top 50' playlists based on a list of country codes. The playlist IDs
    are then pushed to XCom.

    Args:
        **kwargs: Keyword arguments including `ti` for Airflow XCom interaction.

    Pushes:
        XCom key `playlist_ids`: List of playlist IDs for 'Top 50' playlists.

    Raises:
        requests.exceptions.RequestException: If the Spotify API request fails.
        KeyError: If expected keys are missing in the response.
        Exception: For other unexpected errors.
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
            print(f"Searching for playlists for country: {countries_array[i]}")
            for playlist in playlists:
                print(f"Found playlist: {playlist['name']}")
                if (
                    "Top 50" in playlist["name"]
                    and countries_array[i].lower() in playlist["name"].lower()
                ):
                    ids_array.append(playlist["id"])
                    print("This is it")
                    break
                else:
                    print(f"No matching playlist found for {countries_array[i]}")

        kwargs["ti"].xcom_push(key="playlist_ids", value=ids_array)
        print(f"Playlist IDs for {len(ids_array)} countries retrieved successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving playlists: {e}")
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_artists_genres_batch(artist_ids, token, max_retries=5):
    """
    Retrieve genres for a batch of artists from Spotify using exponential backoff.

    Args:
        artist_ids (list): List of Spotify artist IDs (up to 50).
        token (str): Access token for the Spotify API.
        max_retries (int): Max retry attempts on failure (default is 5).

    Returns:
        dict: Mapping of artist IDs to their associated genres, or empty on failure.
    """
    if not artist_ids:
        logging.warning("No artist IDs provided.")
        return {}

    attempt = 0
    while attempt <= max_retries:
        try:
            url = f"https://api.spotify.com/v1/artists?ids={','.join(artist_ids)}"
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logging.warning(
                    f"Rate limit exceeded. Retrying after {retry_after} seconds."
                )
                time.sleep(retry_after)
                attempt += 1
                continue

            response.raise_for_status()
            artist_data = response.json()
            artist_genres = {}

            for artist in artist_data["artists"]:
                artist_id = artist["id"]
                genres = artist.get("genres", [])
                if genres:
                    logging.info(f"Genres found for artist {artist_id}: {genres}")
                else:
                    logging.info(f"No genres found for artist {artist_id}.")
                artist_genres[artist_id] = genres

            return artist_genres

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching genres on attempt {attempt + 1}: {e}")
            if attempt < max_retries:
                # Exponential backoff with jitter
                backoff_time = 2**attempt + uniform(0, 1)
                logging.info(f"Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                attempt += 1
            else:
                logging.critical("Maximum retry attempts reached. Exiting.")
                return {}
        except Exception as e:
            logging.critical(f"Unexpected error: {e}")
            return {}

    logging.error("Failed to fetch genres after multiple attempts.")
    return {}


def save_tracks_to_gcs(**kwargs):
    """
    Save Spotify track data to Google Cloud Storage (GCS) with enriched artist genre data.

    Retrieves an access token and playlist IDs from XCom, fetches track data for
    each playlist, enriches artist data with genre information, and saves the
    result as JSON files in GCS.

    Args:
        **kwargs: Keyword arguments including `ti` for Airflow XCom interaction.

    Pushes:
        None

    Raises:
        requests.exceptions.RequestException: If the Spotify API request fails.
        google.cloud.exceptions.GoogleCloudError: If GCS interaction fails.
        Exception: For other unexpected errors.
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
        genre_cache = {}

        for count, playlist_id in enumerate(playlist_ids, start=0):
            response = requests.get(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()

            track_data = response.json()
            artist_ids = []

            for item in track_data.get("items", []):
                if "track" in item and "artists" in item["track"]:
                    for artist in item["track"]["artists"]:
                        if (
                            artist["id"] not in genre_cache
                            and artist["id"] not in artist_ids
                        ):
                            artist_ids.append(artist["id"])

            # Process artist IDs in batches of 50
            for i in range(0, len(artist_ids), 50):
                batch = artist_ids[i : i + 50]
                genres_batch = get_artists_genres_batch(batch, token)
                genre_cache.update(genres_batch)

            for item in track_data.get("items", []):
                if "track" in item and "artists" in item["track"]:
                    for artist in item["track"]["artists"]:
                        artist["genres"] = genre_cache.get(artist["id"], [])

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
