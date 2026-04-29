###############################################################################
# GCS buckets for the data platform.
#
# Bronze bucket: raw landing zone for unprocessed data files.
# In Sprint 2, the Beam pipeline will optionally write raw messages here
# as a durable backup before BigQuery ingestion (ADR-003).
#
# Free tier: GCS provides 5 GB free storage in us-central1 per month.
###############################################################################

resource "google_storage_bucket" "bronze_raw" {
  name          = var.gcs_bronze_bucket
  location      = var.region
  project       = var.project_id
  force_destroy = false # Prevent accidental data loss on terraform destroy

  # Uniform bucket-level access: disables per-object ACLs.
  # All access controlled via IAM — GCP best practice.
  uniform_bucket_level_access = true

  # Lifecycle rule: delete objects older than 30 days to control storage costs.
  # At portfolio scale, raw trade messages accumulate quickly.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }

  # Versioning disabled — raw ingest data is append-only and not updated in place.
  versioning {
    enabled = false
  }

  labels = {
    layer       = "bronze"
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}

# Allow ingestor SA to write objects to Bronze bucket (for Beam Sprint 2)
resource "google_storage_bucket_iam_member" "ingestor_bronze_writer" {
  bucket = google_storage_bucket.bronze_raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.sa_ingestor.email}"
}

# Allow Terraform state bucket to exist — this is the bootstrap bucket
# created manually before terraform init. We reference it here for documentation
# purposes only; it is NOT managed by this Terraform config (would be circular).
# Manual creation command:
#   gsutil mb -p vg-ind-2026 -l us-central1 gs://vg-ind-2026-tf-state
#   gsutil versioning set on gs://vg-ind-2026-tf-state
