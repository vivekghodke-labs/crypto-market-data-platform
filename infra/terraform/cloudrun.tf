###############################################################################
# Cloud Run v2 service for the BTC/USDT ingestor.
#
# Key design decisions:
#   min_instance_count = 1  — prevents scale-to-zero which would kill the
#                             persistent WebSocket connection to Binance.
#   max_instance_count = 1  — single instance sufficient at portfolio scale.
#                             Multiple instances would create duplicate trade events.
#   cpu_idle = true         — CPU allocated only during request processing.
#                             Background asyncio task (WebSocket) keeps running
#                             because Cloud Run v2 supports always-on CPU for
#                             min-instances > 0.
#   timeout = 3600s         — Long timeout to support persistent connections.
#
# Free tier: Cloud Run provides 2M requests/month + 360,000 GB-seconds free.
# At 1 instance, 256MB RAM → ~40 days free compute per month. Well within limits.
###############################################################################

locals {
  ingestor_image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repo}/ingestor:${var.ingestor_image_tag}"
}

resource "google_cloud_run_v2_service" "ingestor" {
  name     = var.cloud_run_service_name
  location = var.region
  project  = var.project_id

  # Ingress: allow all traffic (needed for health probes from any source)
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    # Assign the ingestor runtime service account
    service_account = google_service_account.sa_ingestor.email

    # Scaling: min 1 to keep WebSocket alive, max 1 to prevent duplicate events
    scaling {
      min_instance_count = var.cloud_run_min_instances
      max_instance_count = var.cloud_run_max_instances
    }

    containers {
      image = local.ingestor_image

      # Resource allocation — 256Mi RAM is sufficient for a single WebSocket
      # connection publishing to Pub/Sub.
      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        # CPU is NOT throttled when idle — required for background asyncio task.
        # Without this, the WebSocket loop would pause between HTTP requests.
        cpu_idle = false
        startup_cpu_boost = true
      }

      # Environment variables — non-sensitive configuration
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PUBSUB_TOPIC_RAW_TRADES"
        value = var.pubsub_raw_trades_topic
      }
      env {
        name  = "PUBSUB_TOPIC_DEAD_LETTER"
        value = var.pubsub_dead_letter_topic
      }
      env {
        name  = "BINANCE_WS_URL"
        value = "wss://stream.binance.com:9443/ws/btcusdt@trade"
      }
      env {
        name  = "ASSET_SYMBOL"
        value = "BTCUSDT"
      }
      env {
        name  = "LOG_LEVEL"
        value = "INFO"
      }
      env {
        name  = "ENVIRONMENT"
        value = "production"
      }
      env {
        name  = "PORT"
        value = "8080"
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      # Liveness probe: Cloud Run restarts the container if /health returns
      # non-200 for 3 consecutive checks (i.e., WebSocket task has died).
      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 15
        period_seconds        = 30
        failure_threshold     = 3
        timeout_seconds       = 5
      }

      # Startup probe: gives the container 60s to start before liveness kicks in.
      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 5
        period_seconds        = 5
        failure_threshold     = 12 # 12 × 5s = 60s total startup window
        timeout_seconds       = 3
      }
    }

    # Request timeout — set high to avoid premature connection termination.
    timeout = "3600s"
  }

  labels = {
    layer       = "ingestion"
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [
    google_project_service.enabled_apis,
    google_artifact_registry_repository.platform_repo,
    google_pubsub_topic.raw_trades,
    google_pubsub_topic.dead_letter,
  ]
}
