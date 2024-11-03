"""
File that contains functions for the processing and loading parts
"""

from google.cloud import storage, bigquery
import pandas as pd
import json
from config.config import schemas

def extract_data_from_blob(blob):
    """
    Extracts data from a single JSON blob and returns lists of dictionaries
    representing records for each table.
    
    Args:
        blob (Blob): The GCS blob containing JSON data.
    
    Returns:
        dict: A dictionary with lists of dictionaries for each table.
    """
    try:
        data = json.loads(blob.download_as_text())
        print(f"Processing data from {blob.name}...")

        top_tracks, albums, artists, available_markets, playlists = [], [], [], [], []
        
        for position, item in enumerate(data["items"], start=1):
            track, album = item["track"], item["track"]["album"]

            # Add top track data
            top_tracks.append({
                "track_id": track["id"],
                "track_name": track["name"],
                "added_at": item["added_at"],
                "album_id": album["id"],
                "artist_id": track["artists"][0]["id"],
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
                    "genres": "",
                })

            # Add available markets data
            for market in track.get("available_markets", []):
                available_markets.append({
                    "track_id": track["id"],
                    "market": market,
                })

            # Add playlist data (example data)
            playlists.append({
                "playlist_id": "example_playlist_id",
                "playlist_name": "Top 50",
                "playlist_description": "Top 50 songs",
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
    Process raw playlist data from Google Cloud Storage (GCS) and save it as CSV files.

    This function connects to a GCS bucket, retrieves JSON files containing Spotify 
    playlist data, and processes them to extract information about tracks, albums, 
    artists, available markets, and playlists. The extracted data is stored in 
    separate Pandas DataFrames, which are then saved as CSV files in the local `/tmp` 
    directory.

    Steps performed:
        1. Connect to the GCS bucket and list all JSON blobs.
        2. Extract relevant data from each blob using the `extract_data_from_blob` function.
        3. Collect data into lists for each table and convert these lists into DataFrames.
        4. Apply necessary transformations (e.g., formatting release dates).
        5. Save the DataFrames as CSV files in the local `/tmp` directory.

    Outputs:
        - CSV files saved locally:
            - /tmp/top_tracks.csv
            - /tmp/albums.csv
            - /tmp/artists.csv
            - /tmp/available_markets.csv
            - /tmp/playlists.csv

    Raises:
        google.cloud.exceptions.GoogleCloudError: If there is an issue accessing GCS.
        json.JSONDecodeError: If there is an issue parsing JSON data.
        KeyError: If expected keys are missing in the JSON structure.
        Exception: For any unexpected errors during processing.

    Prints:
        - Confirmation message when connected to GCS.
        - Progress messages during data extraction and processing.
        - Confirmation message when CSV files are saved.
        - Error message if an unexpected issue occurs during processing.
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
            break

        # Convert lists to DataFrames
        df_top_tracks = pd.DataFrame(all_top_tracks)
        df_albums = pd.DataFrame(all_albums)
        df_artists = pd.DataFrame(all_artists)
        df_available_markets = pd.DataFrame(all_available_markets)
        df_playlists = pd.DataFrame(all_playlists)

        # Apply transformations
        df_albums["release_date"] = df_albums["release_date"].apply(lambda x: f"{x}-01-01" if len(x) == 4 else x)

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
    Upload a local file to a specified Google Cloud Storage (GCS) bucket.

    Args:
        local_path (str): The path to the local file to be uploaded.
        blob_name (str): The name of the file in the GCS bucket.
        bucket_name (str): The name of the GCS bucket (default is "spotify-playlist-bucket1").

    Raises:
        google.cloud.exceptions.GoogleCloudError: If there is an issue uploading the file to GCS.
        FileNotFoundError: If the local file to be uploaded does not exist.

    Prints:
        Confirmation message when the file is successfully uploaded.
        Error message if the upload fails.
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
    Upload all processed CSV files to the specified Google Cloud Storage (GCS) bucket.

    This function calls `upload_to_gcs()` to upload each CSV file generated in the 
    `process_data()` function to the GCS bucket.

    Files uploaded:
        - /tmp/top_tracks.csv -> GCS
        - /tmp/albums.csv -> GCS
        - /tmp/artists.csv -> GCS
        - /tmp/available_markets.csv -> GCS
        - /tmp/playlists.csv -> GCS

    Prints:
        Confirmation message when each file is uploaded or an error message if an upload fails.
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
    Load a CSV file from Google Cloud Storage (GCS) into a BigQuery table.

    Args:
        gcs_uri (str): The URI of the CSV file in GCS (e.g., "gs://bucket_name/file.csv").
        table_id (str): The ID of the BigQuery table to load data into (format: "project_id.dataset_id.table_id").

    Raises:
        google.cloud.exceptions.GoogleCloudError: If there is an issue loading data into BigQuery.

    Prints:
        Confirmation message when the table is successfully loaded.
        Error message if the loading fails.
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
    except bigquery.exceptions.GoogleCloudError as e:
        print(f"Error loading data from {gcs_uri} to table {table_id}: {e}")
    except Exception as e:
        print(f"Unexpected error loading data from {gcs_uri} to table {table_id}: {e}")


def load_data_to_bigquery():
    """
    Load all processed CSV files from the GCS bucket into corresponding BigQuery tables.

    This function calls `load_csv_to_bigquery()` for each CSV file to load it into 
    a specified BigQuery table.

    BigQuery tables:
        - top_tracks
        - albums
        - artists
        - available_markets
        - playlists

    Prints:
        Confirmation message when each table is successfully loaded or an error message if it fails.
    """
    try:
        bucket_path = f"gs://spotify-playlist-bucket1/"
        project_id = "spotify-pipeline1"
        dataset_id = "spotify_country_rankings"

        load_csv_to_bigquery(f"{bucket_path}top_tracks.csv", f"{project_id}.{dataset_id}.top_tracks")
        load_csv_to_bigquery(f"{bucket_path}albums.csv", f"{project_id}.{dataset_id}.albums")
        load_csv_to_bigquery(f"{bucket_path}artists.csv", f"{project_id}.{dataset_id}.artists")
        load_csv_to_bigquery(f"{bucket_path}available_markets.csv", f"{project_id}.{dataset_id}.available_markets")
        load_csv_to_bigquery(f"{bucket_path}playlists.csv", f"{project_id}.{dataset_id}.playlists")
        print("All tables loaded to BigQuery successfully.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
