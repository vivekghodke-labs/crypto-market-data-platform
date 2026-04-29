"""
FastAPI application entry point for the ingestor service.

Responsibilities:
  1. Expose a /health endpoint — required by Cloud Run for liveness probes.
  2. On startup, initialise PubSubPublisher and spawn the WebSocket client
     as a background asyncio task.
  3. On shutdown, cancel the WebSocket task cleanly.

Cloud Run deployment note:
  Cloud Run keeps the container alive as long as there is an active HTTP
  server. FastAPI/Uvicorn satisfies this. The WebSocket client runs as a
  background asyncio task — not a thread — so it shares the event loop
  with FastAPI with no concurrency issues.

Environment variables (loaded via pydantic-settings):
  GCP_PROJECT_ID          — GCP project ID
  PUBSUB_TOPIC_RAW_TRADES — Pub/Sub topic for valid trade events
  PUBSUB_TOPIC_DEAD_LETTER— Pub/Sub topic for rejected messages
  BINANCE_WS_URL          — Binance WebSocket stream URL
  LOG_LEVEL               — Logging level (default: INFO)
  PORT                    — HTTP server port (default: 8080)
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict

from .logger import get_logger
from .publisher import PubSubPublisher
from .websocket_client import BinanceWebSocketClient

logger = get_logger(__name__)


# ─── Settings ─────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    Pydantic-settings validates types and raises on startup if required
    variables are missing — fail-fast is intentional.
    """

    gcp_project_id: str
    pubsub_topic_raw_trades: str = "btc-raw-trades"
    pubsub_topic_dead_letter: str = "btc-dead-letter"
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    asset_symbol: str = "BTCUSDT"
    log_level: str = "INFO"
    port: int = 8080

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# ─── Application State ────────────────────────────────────────────────────────

class AppState:
    """
    Holds references to shared application components.
    Stored on app.state so it is accessible in route handlers
    without global variables.
    """

    publisher: PubSubPublisher | None = None
    ws_client: BinanceWebSocketClient | None = None
    ws_task: asyncio.Task | None = None


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manages application startup and shutdown lifecycle.

    Startup:
      - Validate settings (fail-fast on missing env vars).
      - Initialise Pub/Sub publisher.
      - Spawn WebSocket client as background asyncio task.

    Shutdown:
      - Cancel the WebSocket task cleanly.
      - Allow in-flight publishes to complete (short grace period).
    """
    settings = Settings()
    logger.info(
        "Ingestor service starting",
        extra={
            "project_id": settings.gcp_project_id,
            "raw_trades_topic": settings.pubsub_topic_raw_trades,
            "dead_letter_topic": settings.pubsub_topic_dead_letter,
            "ws_url": settings.binance_ws_url,
            "log_level": settings.log_level,
        },
    )

    # Configure log level from settings
    import logging
    logging.getLogger().setLevel(settings.log_level.upper())

    # Initialise publisher
    publisher = PubSubPublisher(
        project_id=settings.gcp_project_id,
        raw_trades_topic=settings.pubsub_topic_raw_trades,
        dead_letter_topic=settings.pubsub_topic_dead_letter,
    )

    # Initialise WebSocket client
    ws_client = BinanceWebSocketClient(
        ws_url=settings.binance_ws_url,
        publisher=publisher,
    )

    # Attach to app state for health endpoint access
    app.state.publisher = publisher
    app.state.ws_client = ws_client

    # Launch WebSocket client as background task
    ws_task = asyncio.create_task(
        ws_client.run(),
        name="binance-ws-client",
    )
    app.state.ws_task = ws_task

    logger.info("WebSocket ingestion task started")

    yield  # Application is running — serve HTTP requests

    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("Ingestor service shutting down — cancelling WebSocket task")

    ws_task.cancel()
    try:
        await asyncio.wait_for(ws_task, timeout=5.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass  # Expected on clean shutdown

    logger.info("Ingestor service stopped cleanly")


# ─── FastAPI App ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="Crypto Market Data Ingestor",
    description=(
        "Layer 1 of the Near Real-Time Market Data Platform. "
        "Ingests BTC/USDT trade events from Binance WebSocket "
        "and publishes to Google Cloud Pub/Sub."
    ),
    version="1.0.0",
    lifespan=lifespan,
    # Disable docs in production — enable locally for debugging
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url=None,
)


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["observability"])
async def health_check() -> JSONResponse:
    """
    Liveness probe endpoint for Cloud Run.
    Returns 200 if the WebSocket task is alive, 503 if it has died.

    Cloud Run health check configuration (terraform):
      path: /health
      initial_delay: 10s
      period: 30s
      failure_threshold: 3
    """
    ws_client: BinanceWebSocketClient | None = getattr(
        app.state, "ws_client", None
    )
    ws_task: asyncio.Task | None = getattr(app.state, "ws_task", None)

    task_alive = ws_task is not None and not ws_task.done()

    response_body = {
        "status": "healthy" if task_alive else "degraded",
        "service": "ingestor",
        "version": "1.0.0",
    }

    if ws_client is not None:
        response_body["stats"] = ws_client.stats

    status_code = 200 if task_alive else 503

    if not task_alive:
        logger.error(
            "Health check failed — WebSocket task is not running",
            extra={"task_done": ws_task.done() if ws_task else True},
        )

    return JSONResponse(content=response_body, status_code=status_code)


@app.get("/", include_in_schema=False)
async def root() -> JSONResponse:
    """Root redirect — returns service identity for quick sanity checks."""
    return JSONResponse(
        content={
            "service": "crypto-market-data-ingestor",
            "layer": "Layer 1 — Event Ingestion",
            "health": "/health",
        }
    )