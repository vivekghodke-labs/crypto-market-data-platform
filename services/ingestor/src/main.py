"""
FastAPI application entry point for the ingestor service.

Changes from GCP version:
- Replaced PubSubPublisher with KafkaPublisher
- Updated Settings to load Kafka credentials
- Added /ping endpoint for Render health check (always 200)
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict

from .logger import get_logger
from .publisher import KafkaPublisher
from .websocket_client import BinanceWebSocketClient

logger = get_logger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Kafka (Redpanda Cloud)
    kafka_bootstrap_servers: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    kafka_topic_raw_trades: str = "btc-raw-trades"
    kafka_topic_dead_letter: str = "btc-dead-letter"

    # Binance
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    # App
    log_level: str = "INFO"
    port: int = 8080

    model_config = SettingsConfigDict(
        env_file=".env.local",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manages application startup and shutdown lifecycle."""
    settings = Settings()
    logger.info(
        "Ingestor service starting",
        extra={
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "raw_trades_topic": settings.kafka_topic_raw_trades,
            "dead_letter_topic": settings.kafka_topic_dead_letter,
            "ws_url": settings.binance_ws_url,
        },
    )

    import logging

    logging.getLogger().setLevel(settings.log_level.upper())

    publisher = KafkaPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
        raw_trades_topic=settings.kafka_topic_raw_trades,
        dead_letter_topic=settings.kafka_topic_dead_letter,
    )

    ws_client = BinanceWebSocketClient(
        ws_url=settings.binance_ws_url,
        publisher=publisher,
    )

    app.state.publisher = publisher
    app.state.ws_client = ws_client

    ws_task = asyncio.create_task(ws_client.run(), name="binance-ws-client")
    app.state.ws_task = ws_task

    logger.info("WebSocket ingestion task started")

    yield

    logger.info("Ingestor service shutting down")
    ws_task.cancel()
    try:
        await asyncio.wait_for(ws_task, timeout=5.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    publisher.flush()
    logger.info("Ingestor service stopped cleanly")


app = FastAPI(
    title="Crypto Market Data Ingestor",
    description="Layer 1 — Binance WebSocket → Redpanda Cloud",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url=None,
)


@app.get("/ping", tags=["observability"])
async def ping() -> JSONResponse:
    """
    Render health check endpoint. Always returns 200.
    No dependency checks — never triggers a restart.
    UptimeRobot uses /health. Render uses /ping.
    """
    return JSONResponse(content={"status": "ok"})


@app.get("/health", tags=["observability"])
async def health_check() -> JSONResponse:
    """
    Full liveness probe. Returns 503 when WebSocket task is dead.
    Monitored by UptimeRobot for alerting. Not used by Render.
    """
    ws_client = getattr(app.state, "ws_client", None)
    ws_task = getattr(app.state, "ws_task", None)

    task_alive = ws_task is not None and not ws_task.done()

    response_body = {
        "status": "healthy" if task_alive else "degraded",
        "service": "ingestor",
        "version": "2.0.0",
        "backend": "redpanda-cloud",
    }

    if ws_client:
        response_body["stats"] = ws_client.stats

    status_code = 200 if task_alive else 503
    return JSONResponse(content=response_body, status_code=status_code)


@app.get("/", include_in_schema=False)
async def root() -> JSONResponse:
    return JSONResponse(
        content={
            "service": "crypto-market-data-ingestor",
            "layer": "Layer 1 — Event Ingestion",
            "backend": "Redpanda Cloud (Kafka)",
            "health": "/health",
            "ping": "/ping",
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=True)
