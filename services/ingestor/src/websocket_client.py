"""
Maintains a persistent WebSocket connection to the Binance trade stream.

ADR-001: wss://stream.binance.com:9443/ws/btcusdt@trade
ADR-002: This runs as an asyncio task inside the Cloud Run container.
         Cloud Run (min-instances: 1) keeps the connection alive.

Reconnection strategy:
  - On any disconnect or error, reconnect with exponential backoff.
  - Base delay: 1s. Max delay: 60s. Jitter: ±20% of calculated delay.
  - This prevents thundering herd if Binance restarts and many instances
    reconnect simultaneously.

Message processing contract:
  1. Receive raw JSON string from WebSocket.
  2. Attempt Pydantic validation → BinanceTradeEvent.
  3. On success  → publisher.publish_trade(event)
  4. On failure  → publisher.publish_dead_letter(raw, error)
  5. Loop. Never block. Never crash the loop on a single bad message.
"""

import asyncio
import json
import random
from typing import TYPE_CHECKING

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from pydantic import ValidationError

from .logger import get_logger
from .schema import BinanceTradeEvent

if TYPE_CHECKING:
    from .publisher import PubSubPublisher

logger = get_logger(__name__)

# Reconnection backoff parameters
_BACKOFF_BASE_SECONDS: float = 1.0
_BACKOFF_MAX_SECONDS: float = 60.0
_BACKOFF_JITTER_FACTOR: float = 0.2


class BinanceWebSocketClient:
    """
    Async WebSocket client for the Binance public trade stream.
    Designed to run as a long-lived asyncio task.
    """

    def __init__(
        self,
        ws_url: str,
        publisher: "PubSubPublisher",
    ) -> None:
        self._ws_url = ws_url
        self._publisher = publisher
        self._reconnect_attempt: int = 0
        self._running: bool = False
        self._messages_processed: int = 0
        self._messages_rejected: int = 0

    async def run(self) -> None:
        """
        Entry point. Runs the connection loop indefinitely.
        Call this as an asyncio.create_task().
        """
        self._running = True
        logger.info(
            "WebSocket client starting",
            extra={"url": self._ws_url},
        )

        while self._running:
            try:
                await self._connect_and_consume()
                # If _connect_and_consume returns cleanly (shouldn't happen
                # in normal operation), treat as a disconnect.
                logger.warning("WebSocket connection closed cleanly — reconnecting")
            except asyncio.CancelledError:
                # Graceful shutdown via task cancellation
                logger.info("WebSocket client received cancellation — shutting down")
                self._running = False
                break
            except Exception as exc:
                logger.error(
                    "Unexpected error in WebSocket loop",
                    extra={"error": str(exc), "attempt": self._reconnect_attempt},
                    exc_info=True,
                )

            if self._running:
                await self._backoff_sleep()

        logger.info("WebSocket client stopped")

    async def stop(self) -> None:
        """Signal the client to stop after the current reconnect cycle."""
        self._running = False

    @property
    def stats(self) -> dict:
        """Returns runtime statistics for health endpoint reporting."""
        return {
            "messages_processed": self._messages_processed,
            "messages_rejected": self._messages_rejected,
            "reconnect_attempts": self._reconnect_attempt,
            "running": self._running,
        }

    async def _connect_and_consume(self) -> None:
        """
        Establishes the WebSocket connection and processes messages
        until the connection drops or an unrecoverable error occurs.
        """
        logger.info(
            "Connecting to Binance WebSocket",
            extra={
                "url": self._ws_url,
                "attempt": self._reconnect_attempt,
            },
        )

        async with websockets.connect(
            self._ws_url,
            ping_interval=20,    # Send keepalive ping every 20s
            ping_timeout=10,     # Treat as disconnected if no pong in 10s
            close_timeout=5,
        ) as websocket:
            # Reset backoff counter on successful connection
            self._reconnect_attempt = 0
            logger.info("WebSocket connection established")

            async for raw_message in websocket:
                await self._process_message(str(raw_message))

    async def _process_message(self, raw_message: str) -> None:
        """
        Processes a single raw message from the WebSocket stream.
        Validates against the Pydantic schema and routes accordingly.
        Never raises — all errors are caught and dead-lettered.

        Args:
            raw_message: Raw JSON string from the Binance stream.
        """
        try:
            payload = json.loads(raw_message)
            event = BinanceTradeEvent.model_validate(payload)
            self._publisher.publish_trade(event)
            self._messages_processed += 1

            logger.debug(
                "Trade event processed",
                extra={
                    "trade_id": event.trade_id,
                    "price": str(event.price),
                    "quantity": str(event.quantity),
                },
            )

        except (ValidationError, ValueError, json.JSONDecodeError) as exc:
            self._messages_rejected += 1
            error_detail = str(exc)
            logger.warning(
                "Message failed schema validation — routing to dead-letter",
                extra={"error": error_detail},
            )
            self._publisher.publish_dead_letter(
                raw_message=raw_message,
                error=error_detail,
            )

        except Exception as exc:
            # Catch-all: log and continue. Never crash the message loop.
            self._messages_rejected += 1
            logger.error(
                "Unhandled error processing message",
                extra={"error": str(exc)},
                exc_info=True,
            )
            self._publisher.publish_dead_letter(
                raw_message=raw_message,
                error=f"Unhandled error: {exc}",
            )

    async def _backoff_sleep(self) -> None:
        """
        Sleeps for an exponentially increasing duration before reconnecting.
        Adds ±jitter to avoid thundering herd on mass reconnect events.
        """
        self._reconnect_attempt += 1
        delay = min(
            _BACKOFF_BASE_SECONDS * (2 ** (self._reconnect_attempt - 1)),
            _BACKOFF_MAX_SECONDS,
        )
        # Apply jitter: delay ± 20%
        jitter = delay * _BACKOFF_JITTER_FACTOR
        sleep_duration = delay + random.uniform(-jitter, jitter)

        logger.info(
            "Reconnecting after backoff",
            extra={
                "attempt": self._reconnect_attempt,
                "sleep_seconds": round(sleep_duration, 2),
            },
        )
        await asyncio.sleep(sleep_duration)