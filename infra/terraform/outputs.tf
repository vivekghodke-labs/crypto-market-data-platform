###############################################################################
# Exposes key resource identifiers after terraform apply.
# These values are used in:
#   - CI/CD pipeline environment variables
#   - Local development configuration
#   - README documentation
###############################################################################

output "ingestor_cloud_run_url" {
  description = "Public HTTPS URL of the deployed ingestor Cloud Run service."
  value       = google_cloud_run_v2_service.ingestor.uri
}

output "artifact_registry_repo_url" {
  description = "Full Artifact Registry repository URL for Docker push/pull."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repo}"
}

output "ingestor_service_account_email" {
  description = "Email of the ingestor runtime service account."
  value       = google_service_account.sa_ingestor.email
}

output "cicd_service_account_email" {
  description = "Email of the CI/CD service account (used by GitHub Actions)."
  value       = google_service_account.sa_cicd.email
}

output "workload_identity_provider" {
  description = <<EOT
Workload Identity Federation provider resource name.
Set this as GCP_WORKLOAD_IDENTITY_PROVIDER in GitHub Actions secrets.
EOT
  value = google_iam_workload_identity_pool_provider.github_provider.name
}

output "pubsub_raw_trades_topic_id" {
  description = "Full resource ID of the raw trades Pub/Sub topic."
  value       = google_pubsub_topic.raw_trades.id
}

output "pubsub_dead_letter_topic_id" {
  description = "Full resource ID of the dead-letter Pub/Sub topic."
  value       = google_pubsub_topic.dead_letter.id
}

output "gcs_bronze_bucket_name" {
  description = "GCS bucket name for the Bronze raw data landing zone."
  value       = google_storage_bucket.bronze_raw.name
}

output "dataflow_service_account_email" {
  description = "Email of the Dataflow worker service account (used for burst demo runs)."
  value       = google_service_account.sa_dataflow.email
}

output "dataflow_staging_bucket" {
  description = "GCS bucket for Dataflow staging artefacts."
  value       = google_storage_bucket.dataflow_staging.name
}

output "silver_ohlcv_table" {
  description = "Full BigQuery table ID for silver_curated.ohlcv_1min."
  value       = "${var.project_id}:${google_bigquery_dataset.silver.dataset_id}.${google_bigquery_table.silver_ohlcv_1min.table_id}"
}

output "dbt_service_account_email" {
  description = "Email of the dbt transformation runner service account."
  value       = google_service_account.sa_dbt.email
}

output "bigquery_bronze_dataset" {
  description = "BigQuery Bronze dataset ID."
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "bigquery_silver_dataset" {
  description = "BigQuery Silver dataset ID."
  value       = google_bigquery_dataset.silver.dataset_id
}

output "bigquery_gold_dataset" {
  description = "BigQuery Gold dataset ID."
  value       = google_bigquery_dataset.gold.dataset_id
}

output "airflow_service_account_email" {
  description = "Email of the Airflow orchestration service account."
  value       = google_service_account.sa_airflow.email
}

output "gold_moving_averages_table" {
  description = "Full BigQuery table ID for gold_analytics.gold_moving_averages."
  value       = "${var.project_id}:${google_bigquery_dataset.gold.dataset_id}.${google_bigquery_table.gold_moving_averages.table_id}"
}
