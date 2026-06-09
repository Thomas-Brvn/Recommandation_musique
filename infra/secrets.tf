data "google_secret_manager_secret" "pinecone" {
  secret_id = "PINECONE_API_KEY"
  project   = var.project_id
}

data "google_secret_manager_secret" "openai" {
  secret_id = "OPENAI_API_KEY"
  project   = var.project_id
}

data "google_secret_manager_secret" "google_api" {
  secret_id = "GOOGLE_API_KEY"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "cloudrun_pinecone" {
  secret_id = data.google_secret_manager_secret.pinecone.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloudrun.email}"
}

resource "google_secret_manager_secret_iam_member" "cloudrun_openai" {
  secret_id = data.google_secret_manager_secret.openai.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloudrun.email}"
}

resource "google_secret_manager_secret_iam_member" "cloudrun_google_api" {
  secret_id = data.google_secret_manager_secret.google_api.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloudrun.email}"
}
