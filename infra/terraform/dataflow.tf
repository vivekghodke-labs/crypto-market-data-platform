###############################################################################
# Dataflow service account and GCS staging bucket.
#
# These resources support the DataflowRunner burst demo run only.
# They incur NO ongoing cost — Dataflow jobs are billed per vCPU-second
# only while running. After the demo, the job is cancelled and costs stop.
#
# Estimated demo cost: ~$0.20–0.50 for a 10-minute burst run.
#
# To run a Dataflow demo:
#   BEAM_RUNNER=dataflow python -m beam.src.pipeline
#
# To destroy the Dataflow job after demo:
#   gcloud dataflow jobs cancel <JOB_ID> --region=us-central1
#
# The GCS staging bucket and SA remain after job cancellation (no ongoing cost).
###############################################################################

# ─── Dataflow Service Account ─────────────────────────────────────────────────

resource "google_service_account" "sa_dataflow" {
  account_id   = "sa-dataflow"
  display_name = "Crypto Platform — Dataflow Worker"
  description  = "Runtime identity for Dataflow worker VMs. Least-privilege scoped to BQ + GCS + Pub/Sub."
  project      = var.project_id
}

# Read from Pub/Sub raw-trades subscription
resource "google_pubsub_subscription_iam_member" "dataflow_beam_sub_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.beam_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.sa_dataflow.email}"
}

# Write to BigQuery Silver dataset
resource "google_bigquery_dataset_iam_member" "dataflow_silver_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_dataflow.email}"
}

# Write to BigQuery Bronze (dead-letter log)
resource "google_bigquery_dataset_iam_member" "dataflow_bronze_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_dataflow.email}"
}

# Read/write GCS staging and temp locations
resource "google_storage_bucket_iam_member" "dataflow_staging_rw" {
  bucket = google_storage_bucket.dataflow_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.sa_dataflow.email}"
}

# Dataflow worker self-permission (required by Dataflow internals)
resource "google_project_iam_member" "dataflow_worker_role" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.sa_dataflow.email}"
}

# Allow CI/CD SA to act as Dataflow SA (for pipeline submission via GitHub Actions)
resource "google_service_account_iam_member" "cicd_act_as_dataflow" {
  service_account_id = google_service_account.sa_dataflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_cicd.email}"
}

# ─── Dataflow GCS Staging Bucket ──────────────────────────────────────────────

resource "google_storage_bucket" "dataflow_staging" {
  name          = "${var.project_id}-dataflow-staging"
  location      = var.region
  project       = var.project_id
  force_destroy = true  # Safe to destroy — staging files are transient

  uniform_bucket_level_access = true

  # Auto-delete staging files older than 7 days.
  # Dataflow staging artefacts (JARs, Python packages) are re-uploaded
  # on each job run and have no long-term value.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7 # days
    }
  }

  labels = {
    purpose    = "dataflow-staging"
    managed_by = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}
