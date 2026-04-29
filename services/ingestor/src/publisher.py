"""
Handles all Pub/Sub publish operations for the ingestor service.

Design decisions:
- PublisherClient is instantiated once and reused (thread-safe, connection pooled).
- Valid trade events → btc-raw-trades topic.
- Invalid/rejected messages → btc-dead-letter topic with error context.
- publish() is async-friendly: the Pub/Sub client's publish() returns a Future;
  we call result() with a timeout to surface errors immediately rather than
  silently dropping failed publishes.
- Ordering keys are not used at this stage — Dataflow handles deduplication
  via trade_id in the Silver layer (ADR-004).
"""

import json
from decimal import Decimal

from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError

from .logger import get_logger
from .schema import BinanceTradeEvent, DeadLetterEnvelope

logger = get_logger(__name__)

# Pub/Sub publish timeout — if a message isn't acknowledged within this
# window, we treat it as a failure and log the error.
_PUBLISH_TIMEOUT_SECONDS: int = 10


class PubSubPublisher:
    """
    Encapsulates all Pub/Sub publish operations for the ingestor.
    Instantiate once at application startup and inject where needed.
    """

    def __init__(
        self,
        project_id: str,
        raw_trades_topic: str,
        dead_letter_topic: str,
    ) -> None:
        self._client = pubsub_v1.PublisherClient()
        self._raw_trades_path = self._client.topic_path(project_id, raw_trades_topic)
        self._dead_letter_path = self._client.topic_path(project_id, dead_letter_topic)

        logger.info(
            "PubSubPublisher initialised",
            extra={
                "raw_trades_topic": self._raw_trades_path,
                "dead_letter_topic": self._dead_letter_path,
            },
        )

    def publish_trade(self, event: BinanceTradeEvent) -> None:
        """
        Serialises a validated BinanceTradeEvent and publishes to the
        raw-trades topic.

        The payload is JSON with Decimal fields converted to strings
        to preserve precision — floats are prohibited in financial data.

        Args:
            event: A validated BinanceTradeEvent instance.

        Raises:
            GoogleAPICallError: If the Pub/Sub publish call fails.
            TimeoutError: If the publish future does not resolve within timeout.
        """
        payload = json.dumps(
            event.model_dump(by_alias=False),
            default=_decimal_serialiser,
        ).encode("utf-8")

        try:
            future = self._client.publish(
                self._raw_trades_path,
                payload,
                # Attributes are indexed by Pub/Sub and queryable
                symbol=event.symbol,
                trade_id=str(event.trade_id),
            )
            message_id = future.result(timeout=_PUBLISH_TIMEOUT_SECONDS)
            logger.debug(
                "Trade event published",
                extra={
                    "message_id": message_id,
                    "trade_id": event.trade_id,
                    "price": str(event.price),
                },
            )
        except (GoogleAPICallError, TimeoutError) as exc:
            logger.error(
                "Failed to publish trade event",
                extra={"trade_id": event.trade_id, "error": str(exc)},
                exc_info=True,
            )
            raise

    def publish_dead_letter(self, raw_message: str, error: str) -> None:
        """
        Publishes a rejected message to the dead-letter topic.
        Never raises — a failure to publish to dead-letter is logged
        but must not crash the ingestion loop.

        Args:
            raw_message: The original raw string from the WebSocket.
            error:       The validation or parsing error description.
        """
        envelope = DeadLetterEnvelope(raw_message=raw_message, error=error)
        payload = envelope.model_dump_json().encode("utf-8")

        try:
            future = self._client.publish(
                self._dead_letter_path,
                payload,
                rejection_reason="schema_validation_failure",
            )
            future.result(timeout=_PUBLISH_TIMEOUT_SECONDS)
            logger.warning(
                "Message routed to dead-letter topic",
                extra={"error": error},
            )
        except Exception as exc:
            # Dead-letter publish failure must not propagate — log and continue.
            logger.error(
                "Failed to publish to dead-letter topic",
                extra={"error": str(exc)},
                exc_info=True,
            )


def _decimal_serialiser(obj: object) -> str:
    """JSON serialiser for Decimal — converts to string to preserve precision."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")