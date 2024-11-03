resource "google_storage_bucket" "spotify_raw_bucket" {
  name          = "spotify-raw-playlist-bucket1"
  location      = "EU"
  force_destroy = true
}

resource "google_storage_bucket" "spotify_bucket" {
  name          = "spotify-playlist-bucket1"
  location      = "EU"
  force_destroy = true
}