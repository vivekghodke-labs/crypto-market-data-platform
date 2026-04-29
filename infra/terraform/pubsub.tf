###############################################################################
# Pub/Sub topics and subscriptions for the ingestion pipeline.
#
# Topics:
#   btc-raw-trades   → valid, schema-confirmed BTC/USDT trade events
#   btc-dead-letter  → malformed/rejected messages for triage
#
# Subscriptions:
#   btc-raw-trades-beam-sub      → pull subscription for Sprint 2 Beam pipeline
#   btc-dead-letter-triage-sub   → pull subscription for manual triage/monitoring
#
# Message retention: 7 days on both topics (Pub/Sub maximum for free tier).
# This gives a 7-day replay window if the Beam consumer falls behind.
###############################################################################

# ─── Raw Trades Topic ─────────────────────────────────────────────────────────

resource "google_pubsub_topic" "raw_trades" {
  name    = var.pubsub_raw_trades_topic
  project = var.project_id

  message_retention_duration = "604800s" # 7 days

  labels = {
    layer       = "ingestion"
    asset       = "btcusdt"
    environment = "portfolio"
  }

  depends_on = [google_project_service.enabled_apis]
}

# Pull subscription for the Apache Beam pipeline (Sprint 2).
# Beam's PubsubIO uses pull subscriptions with streaming semantics.
resource "google_pubsub_subscription" "beam_sub" {
  name    = var.pubsub_subscription_name
  topic   = google_pubsub_topic.raw_trades.name
  project = var.project_id

  # How long Pub/Sub retains unacknowledged messages.
  # 7 days gives the Beam pipeline recovery window after an outage.
  message_retention_duration = "604800s"

  # If a message is not acknowledged within this window, it is redelivered.
  # 60s is appropriate for a streaming pipeline that processes in near-real-time.
  ack_deadline_seconds = 60

  # Retain acknowledged messages for the retention duration.
  # Allows replay from a specific timestamp if needed.
  retain_acked_messages = true

  # Exponential backoff retry policy for redelivery.
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  labels = {
    consumer = "beam-pipeline"
    sprint   = "2"
  }
}

# ─── Dead Letter Topic ────────────────────────────────────────────────────────

resource "google_pubsub_topic" "dead_letter" {
  name    = var.pubsub_dead_letter_topic
  project = var.project_id

  message_retention_duration = "604800s" # 7 days

  labels = {
    layer       = "ingestion"
    purpose     = "dead-letter"
    environment = "portfolio"
  }

  depends_on = [google_project_service.enabled_apis]
}

# Pull subscription for manual triage of rejected messages.
resource "google_pubsub_subscription" "dead_letter_triage" {
  name    = var.pubsub_dead_letter_subscription
  topic   = google_pubsub_topic.dead_letter.name
  project = var.project_id

  message_retention_duration = "604800s"
  ack_deadline_seconds       = 120

  retain_acked_messages = true

  labels = {
    purpose = "triage-monitoring"
  }
}
