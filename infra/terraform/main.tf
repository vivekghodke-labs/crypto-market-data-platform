###############################################################################
# Provider configuration and Terraform backend.
#
# Backend: GCS bucket for remote state — ensures state is not lost if your
# local machine is wiped and enables future team collaboration.
#
# Bootstrap note (one-time manual step before terraform init):
#   gsutil mb -p vg-ind-2026 -l us-central1 gs://vg-ind-2026-tf-state
#   gsutil versioning set on gs://vg-ind-2026-tf-state
###############################################################################

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }

  backend "gcs" {
    bucket = "vg-ind-2026-tf-state"
    prefix = "crypto-platform/sprint-1"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

###############################################################################
# Enable required GCP APIs
# All APIs are disabled by default on new projects.
# Terraform manages them declaratively — destroying this resource disables
# the API, which is usually NOT desired. lifecycle.prevent_destroy guards this.
###############################################################################

locals {
  required_apis = [
    "run.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "bigquery.googleapis.com",
  ]
}

resource "google_project_service" "enabled_apis" {
  for_each = toset(local.required_apis)

  project                    = var.project_id
  service                    = each.value
  disable_dependent_services = false

  # Prevent accidental API disablement on terraform destroy.
  # APIs take ~5 minutes to re-enable and can break CI/CD pipelines.
  lifecycle {
    prevent_destroy = true
  }
}
