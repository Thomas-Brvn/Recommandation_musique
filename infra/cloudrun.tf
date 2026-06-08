resource "google_artifact_registry_repository" "api" {
  repository_id = "music-api"
  location      = var.region
  format        = "DOCKER"
}

resource "google_service_account" "cloudrun" {
  account_id   = "cloudrun-api"
  display_name = "Cloud Run API Service Account"
}

resource "google_project_iam_member" "cloudrun_gcs" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.cloudrun.email}"
}

resource "google_cloud_run_v2_service" "api" {
  name     = "music-api"
  location = var.region

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
    ]
  }

  template {
    service_account = google_service_account.cloudrun.email

    containers {
      image = "gcr.io/cloudrun/placeholder:latest"

      ports {
        container_port = 8000
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "2Gi"
        }
        startup_cpu_boost = true
      }

      env {
        name  = "GCS_BUCKET_PROCESSED"
        value = "brainz-processed"
      }
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name = "PINECONE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.pinecone.secret_id
            version = "latest"
          }
        }
      }
      env {
        name = "OPENAI_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.openai.secret_id
            version = "latest"
          }
        }
      }
      env {
        name = "GOOGLE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.google_api.secret_id
            version = "latest"
          }
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_v2_service" "dashboard" {
  name     = "music-dashboard"
  location = var.region

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
    ]
  }

  template {
    service_account = google_service_account.cloudrun.email

    containers {
      image = "gcr.io/cloudrun/placeholder:latest"

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        startup_cpu_boost = true
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "GCP_REGION"
        value = var.region
      }
      env {
        name  = "GCS_BUCKET_PROCESSED"
        value = "brainz-processed"
      }
      env {
        name = "GOOGLE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.google_api.secret_id
            version = "latest"
          }
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

output "api_url" {
  description = "URL publique de l'API Cloud Run"
  value       = google_cloud_run_v2_service.api.uri
}

output "dashboard_url" {
  description = "URL publique du Dashboard Cloud Run"
  value       = google_cloud_run_v2_service.dashboard.uri
}
