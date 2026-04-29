###############################################################################
# Docker image registry for all platform service images.
#
# Free tier: Artifact Registry provides 0.5 GB free storage per month.
# At portfolio scale (single image ~150MB), this is well within limits.
###############################################################################

resource "google_artifact_registry_repository" "platform_repo" {
  repository_id = var.artifact_registry_repo
  location      = var.region
  format        = "DOCKER"
  project       = var.project_id
  description   = "Docker images for the crypto-market-data-platform services"

  labels = {
    environment = "portfolio"
    project     = "crypto-market-data-platform"
  }

  # Cleanup policy: retain only the 5 most recent tagged images per service.
  # Prevents storage cost creep on a free-tier project.
  cleanup_policies {
    id     = "keep-5-most-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }

  depends_on = [google_project_service.enabled_apis]
}
