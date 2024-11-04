resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = "spotify_country_rankings"
  location   = "EU"
}

resource "google_bigquery_table" "top_tracks" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "top_tracks"
  deletion_protection=false

  schema = jsonencode([
    { name = "track_id", type = "STRING", mode = "REQUIRED" },
    { name = "track_name", type = "STRING", mode = "NULLABLE" },
    { name = "added_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "album_id", type = "STRING", mode = "REQUIRED" },
    { name = "artist_id", type = "STRING", mode = "REQUIRED" },
    { name = "playlist_id", type = "STRING", mode = "REQUIRED" },
    { name = "popularity", type = "INTEGER", mode = "NULLABLE" },
    { name = "preview_url", type = "STRING", mode = "NULLABLE" },
    { name = "is_explicit", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "position", type = "INTEGER", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "albums" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "albums"
  deletion_protection=false

  schema = jsonencode([
    { name = "album_id", type = "STRING", mode = "REQUIRED" },
    { name = "album_name", type = "STRING", mode = "NULLABLE" },
    { name = "release_date", type = "DATE", mode = "NULLABLE" },
    { name = "total_tracks", type = "INTEGER", mode = "NULLABLE" },
    { name = "album_type", type = "STRING", mode = "NULLABLE" },
    { name = "album_image_url", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "artists" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "artists"
  deletion_protection=false

  schema = jsonencode([
    { name = "artist_id", type = "STRING", mode = "REQUIRED" },
    { name = "artist_name", type = "STRING", mode = "NULLABLE" },
    { name = "artist_url", type = "STRING", mode = "NULLABLE" },
    { name = "genres", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "available_markets" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "available_markets"
  deletion_protection=false

  schema = jsonencode([
    { name = "track_id", type = "STRING", mode = "REQUIRED" },
    { name = "market", type = "STRING", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "playlists" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "playlists"
  deletion_protection=false

  schema = jsonencode([
    { name = "playlist_id", type = "STRING", mode = "REQUIRED" },
    { name = "playlist_name", type = "STRING", mode = "NULLABLE" },
    { name = "playlist_description", type = "STRING", mode = "NULLABLE" },
    { name = "playlist_owner", type = "STRING", mode = "NULLABLE" }
  ])
}