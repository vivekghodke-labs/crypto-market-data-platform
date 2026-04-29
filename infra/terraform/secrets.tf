###############################################################################
# Secret Manager secret resources.
#
# IMPORTANT: Terraform creates the secret *containers* here.
# The actual secret *values* are populated manually via gcloud CLI or
# the GCP Console — they are never stored in code or Terraform state.
#
# Post-apply bootstrap command (one-time):
#   echo -n "vg-ind-2026" | \
#     gcloud secrets versions add crypto-gcp-project-id --data-file=-
#
# Free tier: 6 active secret versions free per month. We stay well within this.
###############################################################################

locals {
  secrets = {
    "crypto-gcp-project-id" = {
      description = "GCP project ID — injected into Cloud Run at runtime"
    }
    "crypto-pubsub-raw-trades-topic" = {
      description = "Pub/Sub raw trades topic name"
    }
    "crypto-pubsub-dead-letter-topic" = {
      description = "Pub/Sub dead-letter topic name"
    }
    "crypto-binance-ws-url" = {
      description = "Binance WebSocket stream URL for BTC/USDT trades"
    }
  }
}

resource "google_secret_manager_secret" "platform_secrets" {
  for_each = local.secrets

  secret_id = each.key
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}
