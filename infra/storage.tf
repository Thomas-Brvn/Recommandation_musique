resource "google_storage_bucket" "tfstate" {
  name          = "tfstate-${var.project_id}"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 5
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "raw_listenbrainz" {
  name          = "brainz-raw-listenbrainz"
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "raw_musicbrainz" {
  name          = "brainz-raw-musicbrainz"
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "processed" {
  name          = "brainz-processed"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "festival" {
  name          = "projet-etude-m2"
  location      = "europe-west3"
  force_destroy = false
}
