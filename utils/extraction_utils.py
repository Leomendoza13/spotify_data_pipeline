"""
File that contains functions for the extraction
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_artists_genres_batch(artist_ids, token, max_retries=5):
    """
    Get genres for a batch of artists from Spotify using their IDs with exponential backoff and logging.

    Args:
        artist_ids (list): A list of artist IDs (up to 50).
        token (str): The access token for Spotify API.
        max_retries (int): Maximum number of retry attempts.

    Returns:
        dict: A dictionary mapping artist IDs to their genres.
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
                logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                attempt += 1
                continue

            response.raise_for_status()
            artist_data = response.json()
            artist_genres = {}

            for artist in artist_data['artists']:
                artist_id = artist['id']
                genres = artist.get('genres', [])
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
                backoff_time = 2 ** attempt + uniform(0, 1)
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
                    for artist in item['track']['artists']:
                        if artist['id'] not in genre_cache and artist['id'] not in artist_ids:
                            artist_ids.append(artist['id'])

            # Process artist IDs in batches of 50
            for i in range(0, len(artist_ids), 50):
                batch = artist_ids[i:i + 50]
                genres_batch = get_artists_genres_batch(batch, token)
                genre_cache.update(genres_batch)

            for item in track_data.get("items", []):
                if "track" in item and "artists" in item["track"]:
                    for artist in item["track"]["artists"]:
                        artist['genres'] = genre_cache.get(artist['id'], [])

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