terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "tfstate-projetetude-497218"
    prefix = "vm-spotify"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
