###############################################################################
# Service accounts, IAM bindings, and Workload Identity Federation.
#
# Design: Principle of Least Privilege throughout.
#   sa-ingestor  → publish to specific Pub/Sub topics only
#   sa-cicd      → deploy Cloud Run + push to Artifact Registry only
#
# Workload Identity Federation (WIF):
#   GitHub Actions authenticates to GCP without storing any long-lived
#   service account keys. WIF exchanges a short-lived GitHub OIDC token
#   for a GCP access token scoped to sa-cicd only.
#   This is the GCP-recommended keyless CI/CD pattern.
###############################################################################

# ─── Ingestor Runtime Service Account ─────────────────────────────────────────

resource "google_service_account" "sa_ingestor" {
  account_id   = "sa-ingestor"
  display_name = "Crypto Ingestor — Cloud Run Runtime"
  description  = "Runtime identity for the ingestor Cloud Run service. Publish-only to Pub/Sub."
  project      = var.project_id
}

# Allow ingestor SA to publish to the raw-trades topic ONLY
resource "google_pubsub_topic_iam_member" "ingestor_publish_raw" {
  project = var.project_id
  topic   = google_pubsub_topic.raw_trades.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.sa_ingestor.email}"
}

# Allow ingestor SA to publish to the dead-letter topic ONLY
resource "google_pubsub_topic_iam_member" "ingestor_publish_dead_letter" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.sa_ingestor.email}"
}

# Allow ingestor SA to read secrets from Secret Manager
# (Binance API key may be needed in future sprints for authenticated endpoints)
resource "google_project_iam_member" "ingestor_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.sa_ingestor.email}"
}

# ─── CI/CD Service Account ────────────────────────────────────────────────────

resource "google_service_account" "sa_cicd" {
  account_id   = "sa-cicd"
  display_name = "Crypto Platform — CI/CD Deploy"
  description  = "Used by GitHub Actions via Workload Identity Federation. Deploy-only."
  project      = var.project_id
}

# Allow CI/CD SA to push Docker images to Artifact Registry
resource "google_project_iam_member" "cicd_artifact_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.sa_cicd.email}"
}

# Allow CI/CD SA to deploy new revisions to Cloud Run
resource "google_project_iam_member" "cicd_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.sa_cicd.email}"
}

# Allow CI/CD SA to act as the ingestor SA when deploying Cloud Run
# (Cloud Run deploy sets --service-account; CI/CD must have iam.serviceAccounts.actAs)
resource "google_service_account_iam_member" "cicd_act_as_ingestor" {
  service_account_id = google_service_account.sa_ingestor.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_cicd.email}"
}

# ─── Workload Identity Federation ─────────────────────────────────────────────

# Step 1: Create the WIF pool (a container for identity providers)
resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions Pool"
  description               = "WIF pool for GitHub Actions OIDC authentication"
  project                   = var.project_id
  disabled                  = false
}

# Step 2: Create the OIDC provider within the pool
# GitHub's OIDC issuer URL is public and well-known.
resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc-provider"
  display_name                       = "GitHub OIDC Provider"
  project                            = var.project_id

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Attribute mapping: maps GitHub OIDC token claims to Google attributes.
  # google.subject   → used in the IAM binding below to scope to this repo only.
  # attribute.actor  → the GitHub user who triggered the workflow (for audit logs).
  # attribute.repository → full repo name (owner/repo).
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Attribute condition: ONLY tokens from this specific GitHub repository
  # are accepted. This is the critical security constraint — without it,
  # any GitHub repository could impersonate sa-cicd.
  attribute_condition = "assertion.repository == \"${var.github_repo}\""
}

# Step 3: Allow the WIF provider to impersonate sa-cicd
# The principal format uses the attribute_mapping defined above.
resource "google_service_account_iam_member" "wif_cicd_binding" {
  service_account_id = google_service_account.sa_cicd.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/${var.github_repo}"
}

# ─── Cloud Run Invoker (public health endpoint) ────────────────────────────────
# Allow unauthenticated HTTP requests to the Cloud Run service.
# Required for Cloud Run health probes and public /health endpoint access.
# The service itself publishes to Pub/Sub using sa-ingestor — this binding
# only controls who can call the HTTP endpoint.
resource "google_cloud_run_v2_service_iam_member" "allow_unauthenticated" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.ingestor.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
