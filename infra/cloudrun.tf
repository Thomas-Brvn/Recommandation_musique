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
      image = "us-docker.pkg.dev/cloudrun-samples/hello:latest"

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

output "api_url" {
  description = "URL publique de l'API Cloud Run"
  value       = google_cloud_run_v2_service.api.uri
}
