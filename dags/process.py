from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import pandas as pd
import json

schemas = {
    "top_tracks": [
        bigquery.SchemaField("track_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("track_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("added_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("album_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("artist_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("popularity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("preview_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_explicit", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("position", "INTEGER", mode="NULLABLE"),
    ],
    "albums": [
        bigquery.SchemaField("album_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("album_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("release_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("total_tracks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("album_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("album_image_url", "STRING", mode="NULLABLE"),
    ],
    "artists": [
        bigquery.SchemaField("artist_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("artist_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("artist_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("genres", "STRING", mode="NULLABLE"),
    ],
    "available_markets": [
        bigquery.SchemaField("track_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("market", "STRING", mode="REQUIRED"),
    ],
    "playlists": [
        bigquery.SchemaField("playlist_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("playlist_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("playlist_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("playlist_owner", "STRING", mode="NULLABLE"),
    ],
}


# 1. Process function to load and transform data
def process_data():
    client = storage.Client()
    bucket = client.bucket("spotify-raw-playlist-bucket1")
    blobs = bucket.list_blobs()

    # Define dataframes for each table
    df_top_tracks, df_albums, df_artists, df_available_markets, df_playlists = (
        pd.DataFrame(columns=columns)
        for columns in [
            [
                "track_id",
                "track_name",
                "added_at",
                "album_id",
                "artist_id",
                "popularity",
                "preview_url",
                "is_explicit",
                "position",
            ],
            [
                "album_id",
                "album_name",
                "release_date",
                "total_tracks",
                "album_type",
                "album_image_url",
            ],
            ["artist_id", "artist_name", "artist_url", "genres"],
            ["track_id", "market"],
            ["playlist_id", "playlist_name", "playlist_description", "playlist_owner"],
        ]
    )

    for blob in blobs:
        if blob.name.endswith(".json"):
            data = json.loads(blob.download_as_text())
            for position, item in enumerate(data["items"], start=1):
                track, album = item["track"], item["track"]["album"]
                # Populate top tracks
                df_top_tracks = pd.concat(
                    [
                        df_top_tracks,
                        pd.DataFrame(
                            [
                                {
                                    "track_id": track["id"],
                                    "track_name": track["name"],
                                    "added_at": item["added_at"],
                                    "album_id": album["id"],
                                    "artist_id": track["artists"][0]["id"],
                                    "popularity": track.get("popularity", 0),
                                    "preview_url": track.get("preview_url"),
                                    "is_explicit": track.get("explicit", False),
                                    "position": position,
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
                # Populate `df_albums`
                df_albums = pd.concat(
                    [
                        df_albums,
                        pd.DataFrame(
                            [
                                {
                                    "album_id": album["id"],
                                    "album_name": album["name"],
                                    "release_date": album.get("release_date"),
                                    "total_tracks": album.get("total_tracks"),
                                    "album_type": album.get("album_type"),
                                    "album_image_url": (
                                        album["images"][0]["url"]
                                        if album.get("images")
                                        else None
                                    ),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

                # Append each artist to `df_artists`
                for artist in track["artists"]:
                    df_artists = pd.concat(
                        [
                            df_artists,
                            pd.DataFrame(
                                [
                                    {
                                        "artist_id": artist["id"],
                                        "artist_name": artist["name"],
                                        "artist_url": artist["external_urls"][
                                            "spotify"
                                        ],
                                        "genres": "",
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

                # Append each available market to `df_available_markets`
                for market in track.get("available_markets", []):
                    df_available_markets = pd.concat(
                        [
                            df_available_markets,
                            pd.DataFrame([{"track_id": track["id"], "market": market}]),
                        ],
                        ignore_index=True,
                    )

                # Append playlist example data to `df_playlists`
                df_playlists = pd.concat(
                    [
                        df_playlists,
                        pd.DataFrame(
                            [
                                {
                                    "playlist_id": "example_playlist_id",
                                    "playlist_name": "Top 50",
                                    "playlist_description": "Top 50 des chansons",
                                    "playlist_owner": "spotify",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
        break
        ##

    # applies
    df_albums["release_date"] = df_albums["release_date"].apply(
        lambda x: f"{x}-01-01" if len(x) == 4 else x
    )

    # Save data locally
    df_top_tracks.to_csv("/tmp/top_tracks.csv", index=False)
    df_albums.to_csv("/tmp/albums.csv", index=False)
    df_artists.to_csv("/tmp/artists.csv", index=False)
    df_available_markets.to_csv("/tmp/available_markets.csv", index=False)
    df_playlists.to_csv("/tmp/playlists.csv", index=False)
    print("csv saved")


# 2. Upload to GCS
def upload_to_gcs(local_path, blob_name, bucket_name="spotify-playlist-bucket1"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"{local_path} uploaded to {bucket_name}/{blob_name}")


def upload_files_to_gcs():
    upload_to_gcs("/tmp/top_tracks.csv", "top_tracks.csv")
    upload_to_gcs("/tmp/albums.csv", "albums.csv")
    upload_to_gcs("/tmp/artists.csv", "artists.csv")
    upload_to_gcs("/tmp/available_markets.csv", "available_markets.csv")
    upload_to_gcs("/tmp/playlists.csv", "playlists.csv")


# 3. Load CSV to BigQuery
def load_csv_to_bigquery(gcs_uri, table_id):

    client = bigquery.Client()
    table_name = table_id.split(".")[-1]
    schema = schemas.get(table_name)

    if schema is None:
        raise ValueError(f"Schéma non défini pour la table {table_name}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema=schema
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Attend la fin du job de chargement
    print(f"Table {table_id} loaded from {gcs_uri}")


def load_data_to_bigquery():
    bucket_path = f"gs://spotify-playlist-bucket1/"
    project_id = "spotify-pipeline1"
    dataset_id = "spotify_country_rankings"

    load_csv_to_bigquery(
        f"{bucket_path}top_tracks.csv", f"{project_id}.{dataset_id}.top_tracks"
    )
    load_csv_to_bigquery(
        f"{bucket_path}albums.csv", f"{project_id}.{dataset_id}.albums"
    )
    load_csv_to_bigquery(
        f"{bucket_path}artists.csv", f"{project_id}.{dataset_id}.artists"
    )
    load_csv_to_bigquery(
        f"{bucket_path}available_markets.csv",
        f"{project_id}.{dataset_id}.available_markets",
    )
    load_csv_to_bigquery(
        f"{bucket_path}playlists.csv", f"{project_id}.{dataset_id}.playlists"
    )
