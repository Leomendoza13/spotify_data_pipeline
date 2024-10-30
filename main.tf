provider "google" {
  project = "spotify-pipeline1" 
  region  = "europe-west9" 
}


# Creates a Google Cloud Storage bucket for raw Spotify playlists data
resource "google_storage_bucket" "spotify_raw_bucket" {
  name          = "spotify-raw-playlist-bucket1"
  location      = "EU"  # Stockage dans l'Union Européenne
  #force_destroy = true
}

# Creates another Google Cloud Storage bucket for processed playlists data
resource "google_storage_bucket" "spotify_bucket" {
  name          = "spotify-playlist-bucket1"
  location      = "EU"  # Stockage dans l'Union Européenne
}

# Creates a service account for use by the VM that runs Airflow
resource "google_service_account" "airflow_service_account" {
  account_id   = "airflow-service-account"
  display_name = "Airflow Service Account"
}

# Assigns the storage admin role to the Airflow service account for bucket access
resource "google_project_iam_member" "airflow_service_account_permissions" {
  project = "spotify-pipeline1"
  role    = "roles/storage.admin" # ou "roles/bigquery.admin" si BigQuery est requis
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

# Configures a Compute Engine VM instance for Airflow
resource "google_compute_instance" "airflow_instance" {
  name         = "airflow-instance"
  machine_type = "n2-standard-2"
  zone         = "europe-west9-a" 

  # Sets up the VM's boot disk with Ubuntu
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-focal-v20220712"
    }
  }

  # Attaches the Airflow service account with cloud platform access to the VM
  service_account {
    email  = google_service_account.airflow_service_account.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  # Configures network access for the VM
  network_interface {
    network = "default"
    access_config {}
  }

  # Specifies a startup script to run when the VM starts
  metadata_startup_script = file("${path.module}/scripts/startup-script.sh")

}

# Creates another service account with permissions for data handling in GCS and BigQuery
resource "google_service_account" "sa" {
  account_id   = "spotify-service-account"
  display_name = "Service Account for Spotify Project"
}

# Grants storage admin permissions to the created service account
resource "google_project_iam_member" "storage_access" {
  project = "spotify-pipeline1"  # Terraform va utiliser automatiquement le Project ID via gcloud
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}


# Grants BigQuery admin permissions to the created service account
resource "google_project_iam_member" "bigquery_access" {
  project = "spotify-pipeline1"  # Terraform va utiliser automatiquement le Project ID via gcloud
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

# Assigns specific IAM roles for BigQuery data editing to the service account
resource "google_project_iam_member" "bigquery_data_editor" {
  project = "spotify-pipeline1"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

# Grants BigQuery job user permissions for running queries
resource "google_project_iam_member" "bigquery_job_user" {
  project = "spotify-pipeline1"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

# Assigns viewer role for the service account on the source storage bucket
resource "google_storage_bucket_iam_member" "source_viewer" {
  bucket = google_storage_bucket.spotify_bucket.name  # Assure-toi que spotify_raw_bucket est bien défini comme le bucket source
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:spotify-service-account@spotify-pipeline1.iam.gserviceaccount.com"  # Remplace par l'email du compte de service utilisé
}

# Assigns admin role for the service account on the destination storage bucket
resource "google_storage_bucket_iam_member" "destination_admin" {
  bucket = google_storage_bucket.spotify.name  # Assure-toi que spotify_playlist_bucket est bien défini comme le bucket destination
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:spotify-service-account@spotify-pipeline1.iam.gserviceaccount.com"  # Remplace par l'email du compte de service utilisé
}

# Creates a BigQuery dataset for Spotify country ranking data
resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = "spotify_country_rankings"  # Remplace par le nom de ton dataset
  location   = "EU"
}

# Defines a BigQuery table to store the top tracks
resource "google_bigquery_table" "top_tracks" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  table_id   = "top_tracks"
  deletion_protection=false

   # Schema for top tracks table
  schema = jsonencode([
    { name = "track_id", type = "STRING", mode = "REQUIRED" },
    { name = "added_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "album_id", type = "STRING", mode = "REQUIRED" },
    { name = "artist_id", type = "STRING", mode = "REQUIRED" },
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
    { name = "genres", type = "STRING", mode = "NULLABLE" }  # Format CSV pour les genres
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