resource "google_service_account" "airflow_service_account" {
  account_id   = "airflow-service-account"
  display_name = "Airflow Service Account"
}

resource "google_project_iam_member" "airflow_service_account_permissions" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}