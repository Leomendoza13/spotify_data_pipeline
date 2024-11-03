"""
    Global variables
"""

from google.cloud import bigquery

COUNTRIES_DATA = {
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

VARIABLE_FILE = "/opt/airflow/config/ids.json"