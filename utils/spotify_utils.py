"""
File that contains important functions for the project
"""

import json
import requests
from config.config import COUNTRIES_DATA
from config.config import VARIABLE_FILE
from airflow.models import Variable

codes_array = list(COUNTRIES_DATA.keys())
countries_array = list(COUNTRIES_DATA.values())


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
