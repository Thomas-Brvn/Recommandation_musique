resource "google_storage_bucket" "tfstate" {
  name          = "tfstate-${var.project_id}"
  location      = "US-CENTRAL1"
  force_destroy = false
}

resource "google_storage_bucket" "raw_listenbrainz" {
  name          = "brainz-raw-listenbrainz"
  location      = "EU"
  force_destroy = false
}

resource "google_storage_bucket" "raw_musicbrainz" {
  name          = "brainz-raw-musicbrainz"
  location      = "EU"
  force_destroy = false
}

resource "google_storage_bucket" "processed" {
  name          = "brainz-processed"
  location      = "EU"
  force_destroy = false
}

resource "google_storage_bucket" "festival" {
  name          = "projet-etude-m2"
  location      = "europe-west3"
  force_destroy = false
}
