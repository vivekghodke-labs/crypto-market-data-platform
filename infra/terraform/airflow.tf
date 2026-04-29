###############################################################################
# Service account and IAM bindings for the Airflow orchestration layer.
#
# sa-airflow is used by the Airflow scheduler running on OrbStack locally
# and would be used by Cloud Composer in a production GCP deployment.
#
# Permissions follow least-privilege:
#   - BigQuery dataViewer on Bronze + Silver (health monitor queries)
#   - BigQuery dataEditor on Gold (dbt run writes Gold tables)
#   - BigQuery dataEditor on dbt_test_failures (dbt test --store-failures)
#   - BigQuery jobUser on project (required to execute any BQ query)
#   - Secret Manager secretAccessor (reads connection strings at runtime)
#   - Cloud Run invoker on ingestor service (health endpoint HTTP check)
###############################################################################

# ─── Airflow Service Account ──────────────────────────────────────────────────

resource "google_service_account" "sa_airflow" {
  account_id   = "sa-airflow"
  display_name = "Crypto Platform — Airflow Orchestration"
  description  = "Runtime identity for Airflow scheduler. Orchestrates dbt and monitors platform health."
  project      = var.project_id
}

# ─── BigQuery Permissions ─────────────────────────────────────────────────────

resource "google_bigquery_dataset_iam_member" "airflow_bronze_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa_airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_silver_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa_airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_gold_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_test_failures_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.dbt_test_failures.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa_airflow.email}"
}

# ─── Secret Manager ───────────────────────────────────────────────────────────

resource "google_project_iam_member" "airflow_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.sa_airflow.email}"
}

# ─── Cloud Run Invoker ────────────────────────────────────────────────────────
# Required for health monitor DAG to call the ingestor /health endpoint.

resource "google_cloud_run_v2_service_iam_member" "airflow_ingestor_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sa_airflow.email}"
}

# ─── Airflow SA Key (for local OrbStack usage) ────────────────────────────────
# Generates a JSON key for the Airflow SA for local development.
# In production (Cloud Composer), Workload Identity is used instead.
#
# IMPORTANT: The generated key file must be stored securely.
# Store in Secret Manager and retrieve via:
#   gcloud secrets versions access latest --secret="airflow-sa-key" > sa-airflow-key.json
#
# After apply, create the Secret Manager entry:
#   gcloud iam service-accounts keys create sa-airflow-key.json \
#     --iam-account=sa-airflow@vg-ind-2026.iam.gserviceaccount.com
#   gcloud secrets create airflow-sa-key --data-file=sa-airflow-key.json
#   rm sa-airflow-key.json   # Never store the key file locally

# Allow CI/CD SA to act as Airflow SA (for future Cloud Composer deployment)
resource "google_service_account_iam_member" "cicd_act_as_airflow" {
  service_account_id = google_service_account.sa_airflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_cicd.email}"
}
