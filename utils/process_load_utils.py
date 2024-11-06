"""
File that contains functions for the processing and loading parts of the Spotify data pipeline.
"""

# Copyright (c) 2024 Léo Mendoza
# Licensed under the MIT License. See LICENSE file in the project root for detail

from google.cloud import storage, bigquery
import pandas as pd
import json
import re
from config.config import schemas
from airflow.models import Variable
from google.api_core.exceptions import GoogleAPIError

def extract_data_from_blob(blob):
    """
    Extract data from a GCS JSON blob and structure it for multiple tables.

    Processes JSON data from a GCS blob representing playlist tracks, 
    creating lists of dictionaries for each target table, such as `top_tracks`,
    `albums`, `artists`, `available_markets`, and `playlists`.

    Args:
        blob (Blob): The GCS blob containing JSON playlist data.
    
    Returns:
        dict: A dictionary with structured lists of dictionaries for each table, or None if an error occurs.
    
    Raises:
        json.JSONDecodeError: If the JSON data cannot be decoded.
        KeyError: If expected fields are missing from the data structure.

    Prints:
        - Status message for each major operation and error message if an error occurs.
    """
    try:
        data = json.loads(blob.download_as_text())
        print(f"Processing data from {blob.name}...")

        top_tracks, albums, artists, available_markets, playlists = [], [], [], [], []

        href = data["href"]
        playlist_id = re.search(r"playlists/([a-zA-Z0-9]+)/tracks", href).group(1)
        
        for position, item in enumerate(data["items"], start=1):
            track, album = item["track"], item["track"]["album"]

            # Add top track data
            top_tracks.append({
                "track_id": track["id"],
                "track_name": track["name"],
                "added_at": item["added_at"],
                "album_id": album["id"],
                "artist_id": track["artists"][0]["id"],
                "playlist_id": playlist_id,
                "popularity": track.get("popularity", 0),
                "preview_url": track.get("preview_url"),
                "is_explicit": track.get("explicit", False),
                "position": position,
            })

            # Add album data
            albums.append({
                "album_id": album["id"],
                "album_name": album["name"],
                "release_date": album.get("release_date"),
                "total_tracks": album.get("total_tracks"),
                "album_type": album.get("album_type"),
                "album_image_url": (album["images"][0]["url"] if album.get("images") else None),
            })

            # Add artist data
            for artist in track["artists"]:
                artists.append({
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "artist_url": artist["external_urls"]["spotify"],
                    "genres": artist['genres'],
                })

            # Add available markets data
            for market in track.get("available_markets", []):
                available_markets.append({
                    "track_id": track["id"],
                    "market": market,
                })

            playlist_name = re.search(r"spotify_tracks_(.*?).json", blob.name).group(1)

            # Add playlist data (example data)
            playlists.append({
                "playlist_id": playlist_id,
                "playlist_name": playlist_name.capitalize(),
                "playlist_description": "Top 50 - " + playlist_name.capitalize(),
                "playlist_owner": "spotify",
            })

        return {
            "top_tracks": top_tracks,
            "albums": albums,
            "artists": artists,
            "available_markets": available_markets,
            "playlists": playlists,
        }

    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {blob.name}")
        return None
    except KeyError as e:
        print(f"Key error while processing {blob.name}: {e}")
        return None


def process_data():
    """
    Process raw Spotify playlist data from GCS and save it as CSV files locally.

    Connects to a GCS bucket, retrieves JSON files with Spotify playlist data, 
    extracts and transforms data for tables, and saves the processed data as 
    CSV files in the local `/tmp` directory for further processing.

    CSV files created:
        - /tmp/top_tracks.csv
        - /tmp/albums.csv
        - /tmp/artists.csv
        - /tmp/available_markets.csv
        - /tmp/playlists.csv

    Raises:
        google.cloud.exceptions.GoogleCloudError: If GCS access fails.
        json.JSONDecodeError: If JSON data cannot be parsed.
        KeyError: If expected fields are missing in JSON data.
        Exception: For other unexpected errors.

    Prints:
        - Progress and confirmation messages for each step and error messages if issues occur.
    """
    try:
        client = storage.Client()
        bucket = client.bucket("spotify-raw-playlist-bucket1")
        blobs = bucket.list_blobs()
        print("Connected to GCS bucket successfully.")

        # Initialize lists to collect data
        all_top_tracks, all_albums, all_artists, all_available_markets, all_playlists = [], [], [], [], []

        for blob in blobs:
            if blob.name.endswith(".json"):
                data = extract_data_from_blob(blob)
                if data:
                    all_top_tracks.extend(data["top_tracks"])
                    all_albums.extend(data["albums"])
                    all_artists.extend(data["artists"])
                    all_available_markets.extend(data["available_markets"])
                    all_playlists.extend(data["playlists"])
            

        # Convert lists to DataFrames
        df_top_tracks = pd.DataFrame(all_top_tracks)
        df_albums = pd.DataFrame(all_albums)
        df_artists = pd.DataFrame(all_artists)
        df_available_markets = pd.DataFrame(all_available_markets)
        df_playlists = pd.DataFrame(all_playlists)

        df_albums = df_albums.drop_duplicates(subset='album_id', keep='first')
        df_artists = df_artists.drop_duplicates(subset='artist_id', keep='first')
        df_playlists = df_playlists.drop_duplicates(subset='playlist_id', keep='first')

        # Apply transformations
        df_albums["release_date"] = df_albums["release_date"].apply(
            lambda x: f"{x}-01-01" if len(x) == 4 else (f"{x}-01" if len(x) == 7 else x)
        )   

        # Save data locally
        df_top_tracks.to_csv("/tmp/top_tracks.csv", index=False)
        df_albums.to_csv("/tmp/albums.csv", index=False)
        df_artists.to_csv("/tmp/artists.csv", index=False)
        df_available_markets.to_csv("/tmp/available_markets.csv", index=False)
        df_playlists.to_csv("/tmp/playlists.csv", index=False)
        print("CSV files saved successfully.")

    except Exception as e:
        print(f"Unexpected error during data processing: {e}")


# 2. Upload to GCS
def upload_to_gcs(local_path, blob_name, bucket_name="spotify-playlist-bucket1"):
    """
    Upload a local file to a specified GCS bucket.

    Args:
        local_path (str): Path to the local file to be uploaded.
        blob_name (str): Destination filename in the GCS bucket.
        bucket_name (str): GCS bucket name (default is "spotify-playlist-bucket1").

    Raises:
        google.cloud.exceptions.GoogleCloudError: If file upload fails.
        FileNotFoundError: If the local file does not exist.

    Prints:
        - Confirmation message if the file is successfully uploaded or error message if upload fails.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the local file exists before attempting to upload
        with open(local_path, 'rb') as f:
            pass  # Just to check if the file exists

        blob.upload_from_filename(local_path)
        print(f"{local_path} uploaded to {bucket_name}/{blob_name} successfully.")
    except FileNotFoundError:
        print(f"Error: The local file {local_path} does not exist.")
    except storage.exceptions.GoogleCloudError as e:
        print(f"Error uploading {local_path} to {bucket_name}/{blob_name}: {e}")
    except Exception as e:
        print(f"Unexpected error during upload of {local_path} to {bucket_name}/{blob_name}: {e}")


def upload_files_to_gcs():
    """
    Upload all processed CSV files to a GCS bucket.

    Uses `upload_to_gcs()` to upload each CSV file created in `process_data()` 
    to the specified GCS bucket.

    Files uploaded:
        - /tmp/top_tracks.csv
        - /tmp/albums.csv
        - /tmp/artists.csv
        - /tmp/available_markets.csv
        - /tmp/playlists.csv

    Prints:
        - Success messages for each file uploaded or error messages if upload fails.
    """
    files_to_upload = [
        ("/tmp/top_tracks.csv", "top_tracks.csv"),
        ("/tmp/albums.csv", "albums.csv"),
        ("/tmp/artists.csv", "artists.csv"),
        ("/tmp/available_markets.csv", "available_markets.csv"),
        ("/tmp/playlists.csv", "playlists.csv"),
    ]

    for local_path, blob_name in files_to_upload:
        try:
            upload_to_gcs(local_path, blob_name)
            print(f"Successfully uploaded {local_path} to GCS as {blob_name}.")
        except FileNotFoundError:
            print(f"Error: The local file {local_path} does not exist.")
        except storage.exceptions.GoogleCloudError as e:
            print(f"Error uploading {local_path} to GCS as {blob_name}: {e}")
        except Exception as e:
            print(f"Unexpected error uploading {local_path} to GCS as {blob_name}: {e}")

# 3. Load CSV to BigQuery
def load_csv_to_bigquery(gcs_uri, table_id):
    """
    Load a CSV file from GCS into a BigQuery table.

    Args:
        gcs_uri (str): URI of the CSV file in GCS.
        table_id (str): BigQuery table ID (format: "project_id.dataset_id.table_id").

    Raises:
        google.cloud.exceptions.GoogleCloudError: If data load into BigQuery fails.
        ValueError: If schema for the table is undefined.

    Prints:
        - Confirmation message if the table is successfully loaded or error message if load fails.
    """
    try:
        client = bigquery.Client()
        table_name = table_id.split(".")[-1]
        schema = schemas.get(table_name)

        if schema is None:
            raise ValueError(f"Schema not defined for table {table_name}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema=schema
        )

        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the load job to complete
        print(f"Table {table_id} successfully loaded from {gcs_uri}")
    except ValueError as e:
        print(f"Error: {e}")
    except GoogleAPIError as e:  # Utilisation de GoogleAPIError à la place
        print(f"Error loading data from {gcs_uri} to table {table_id}: {e}")
    except Exception as e:
        print(f"Unexpected error loading data from {gcs_uri} to table {table_id}: {e}")


def load_data_to_bigquery():
    """
    Load all processed CSV files from GCS into corresponding BigQuery tables.

    Calls `load_csv_to_bigquery()` to load each CSV file into a specific 
    BigQuery table.

    BigQuery tables:
        - top_tracks
        - albums
        - artists
        - available_markets
        - playlists

    Prints:
        - Success message for each table loaded or error message if loading fails.
    """
    try:
        bucket_path = f"gs://spotify-playlist-bucket1/"
        project_id = Variable.get("PROJECT_ID")
        dataset_id = "spotify_country_rankings"

        load_csv_to_bigquery(f"{bucket_path}top_tracks.csv", f"{project_id}.{dataset_id}.top_tracks")
        load_csv_to_bigquery(f"{bucket_path}albums.csv", f"{project_id}.{dataset_id}.albums")
        load_csv_to_bigquery(f"{bucket_path}artists.csv", f"{project_id}.{dataset_id}.artists")
        load_csv_to_bigquery(f"{bucket_path}available_markets.csv", f"{project_id}.{dataset_id}.available_markets")
        load_csv_to_bigquery(f"{bucket_path}playlists.csv", f"{project_id}.{dataset_id}.playlists")
        print("All tables loaded to BigQuery successfully.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
