###############################################################################
# All input variables for the crypto-market-data-platform infrastructure.
# Sensitive values (API keys, passwords) are NEVER defined here —
# they live in Secret Manager and are referenced at runtime.
###############################################################################

variable "project_id" {
  description = "GCP project ID. Passed via -var flag or tfvars file."
  type        = string
  default     = "vg-ind-2026"

  validation {
    condition     = length(var.project_id) > 0
    error_message = "project_id must not be empty."
  }
}

variable "region" {
  description = "GCP region for all regional resources."
  type        = string
  default     = "us-central1"
}

variable "artifact_registry_repo" {
  description = "Artifact Registry repository name for Docker images."
  type        = string
  default     = "crypto-platform"
}

variable "ingestor_image_tag" {
  description = "Docker image tag to deploy to Cloud Run. Set by CD pipeline."
  type        = string
  default     = "latest"
}

variable "pubsub_raw_trades_topic" {
  description = "Pub/Sub topic name for validated BTC trade events."
  type        = string
  default     = "btc-raw-trades"
}

variable "pubsub_dead_letter_topic" {
  description = "Pub/Sub topic name for rejected/malformed messages."
  type        = string
  default     = "btc-dead-letter"
}

variable "pubsub_subscription_name" {
  description = "Pub/Sub pull subscription for the Beam pipeline (Sprint 2)."
  type        = string
  default     = "btc-raw-trades-beam-sub"
}

variable "pubsub_dead_letter_subscription" {
  description = "Pull subscription on dead-letter topic for triage/monitoring."
  type        = string
  default     = "btc-dead-letter-triage-sub"
}

variable "gcs_bronze_bucket" {
  description = "GCS bucket name for Bronze layer raw data landing zone."
  type        = string
  default     = "vg-ind-2026-bronze-raw"
}

variable "cloud_run_service_name" {
  description = "Cloud Run service name for the ingestor."
  type        = string
  default     = "crypto-ingestor"
}

variable "cloud_run_min_instances" {
  description = <<EOT
Minimum Cloud Run instances. Set to 1 to maintain persistent WebSocket
connection. Set to 0 only if you want the ingestor to scale to zero
(NOT recommended — cold start kills the WebSocket stream).
EOT
  type        = number
  default     = 1
}

variable "cloud_run_max_instances" {
  description = "Maximum Cloud Run instances. 1 is sufficient for portfolio scale."
  type        = number
  default     = 1
}

variable "bigquery_dataset_bronze" {
  description = "BigQuery dataset ID for Bronze (raw) layer."
  type        = string
  default     = "bronze_raw"
}

variable "bigquery_dataset_silver" {
  description = "BigQuery dataset ID for Silver (curated/aggregated) layer."
  type        = string
  default     = "silver_curated"
}

variable "bigquery_dataset_gold" {
  description = "BigQuery dataset ID for Gold (analytics-ready) layer."
  type        = string
  default     = "gold_analytics"
}

variable "github_repo" {
  description = <<EOT
GitHub repository in 'owner/repo' format.
Used to scope the Workload Identity Federation principal binding —
only this specific repository can impersonate the CI/CD service account.
EOT
  type        = string
  default     = "vivekghodke-labs/crypto-market-data-platform"
}
