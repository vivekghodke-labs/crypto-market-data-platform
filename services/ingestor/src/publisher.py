"""
Kafka producer for Redpanda Cloud.
Replaces google-cloud-pubsub with confluent-kafka.

Design:
- Synchronous produce with delivery callbacks
- JSON serialization (same as Pub/Sub)
- Topic routing: valid → btc-raw-trades, rejected → btc-dead-letter
"""

import json
from decimal import Decimal

from confluent_kafka import Producer, KafkaException

from .logger import get_logger
from .schema import BinanceTradeEvent, DeadLetterEnvelope

logger = get_logger(__name__)


class KafkaPublisher:
    """Publishes validated trades and dead-letter messages to Redpanda Cloud."""

    def __init__(
        self,
        bootstrap_servers: str,
        sasl_username: str,
        sasl_password: str,
        raw_trades_topic: str,
        dead_letter_topic: str,
    ) -> None:
        self._raw_trades_topic = raw_trades_topic
        self._dead_letter_topic = dead_letter_topic

        config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-256',
            'sasl.username': sasl_username,
            'sasl.password': sasl_password,
            'client.id': 'crypto-ingestor',
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'retry.backoff.ms': 1000,
        }

        self._producer = Producer(config)
        logger.info(
            "KafkaPublisher initialized",
            extra={
                "bootstrap_servers": bootstrap_servers,
                "raw_trades_topic": raw_trades_topic,
                "dead_letter_topic": dead_letter_topic,
            },
        )

    def publish_trade(self, event: BinanceTradeEvent) -> None:
        """Publishes a validated trade event to btc-raw-trades topic."""
        payload = json.dumps(
            event.model_dump(by_alias=False),
            default=_decimal_serializer,
        )

        try:
            self._producer.produce(
                topic=self._raw_trades_topic,
                key=str(event.trade_id).encode('utf-8'),
                value=payload.encode('utf-8'),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)  # Trigger delivery callbacks

        except KafkaException as exc:
            logger.error(
                "Failed to publish trade event",
                extra={"trade_id": event.trade_id, "error": str(exc)},
                exc_info=True,
            )
            raise

    def publish_dead_letter(self, raw_message: str, error: str) -> None:
        """Publishes a rejected message to btc-dead-letter topic."""
        envelope = DeadLetterEnvelope(raw_message=raw_message, error=error)
        payload = envelope.model_dump_json().encode('utf-8')

        try:
            self._producer.produce(
                topic=self._dead_letter_topic,
                value=payload,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
            logger.warning("Message routed to dead-letter topic", extra={"error": error})

        except Exception as exc:
            logger.error(
                "Failed to publish to dead-letter topic",
                extra={"error": str(exc)},
                exc_info=True,
            )

    def flush(self) -> None:
        """Flushes pending messages (call on shutdown)."""
        self._producer.flush()

    @staticmethod
    def _delivery_callback(err, msg):
        """Kafka delivery report callback."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} partition {msg.partition()}")


def _decimal_serializer(obj: object) -> str:
    """JSON serializer for Decimal."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")