"""
File that contains GCS Functions
"""

import json
import requests
from google.cloud import storage

from config.config import COUNTRIES_DATA

codes_array = list(COUNTRIES_DATA.keys())
countries_array = list(COUNTRIES_DATA.values())
countries_files_array = [country.lower() for country in countries_array]


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
