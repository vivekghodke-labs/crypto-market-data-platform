"""
Unit tests for KafkaPublisher (confluent-kafka / Redpanda Cloud).

Strategy:
  - confluent_kafka.Producer is mocked at the class level — no broker required.
  - Tests verify: producer config, payload serialisation, topic routing,
    delivery callback behaviour, and dead-letter error swallowing.
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

import pytest

from src.publisher import KafkaPublisher, _decimal_serializer
from src.schema import BinanceTradeEvent


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_producer():
    """Patches confluent_kafka.Producer for all publisher tests."""
    with patch("src.publisher.Producer") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def publisher(mock_producer) -> KafkaPublisher:
    return KafkaPublisher(
        bootstrap_servers="pkc-abc123.us-east-1.aws.redpanda.cloud:9092",
        sasl_username="test-user",
        sasl_password="test-pass",
        raw_trades_topic="btc-raw-trades",
        dead_letter_topic="btc-dead-letter",
    )


@pytest.fixture
def valid_event() -> BinanceTradeEvent:
    return BinanceTradeEvent.model_validate({
        "e": "trade",
        "E": 1700000001000,
        "s": "BTCUSDT",
        "t": 999001,
        "p": "43500.50",
        "q": "0.002",
        "T": 1700000000800,
        "m": False,
    })


# ─── Initialisation Tests ──────────────────────────────────────────────────────

class TestKafkaPublisherInit:

    def test_producer_created_with_sasl_config(self, mock_producer) -> None:
        with patch("src.publisher.Producer") as mock_class:
            mock_class.return_value = MagicMock()
            KafkaPublisher(
                bootstrap_servers="broker:9092",
                sasl_username="user",
                sasl_password="pass",
                raw_trades_topic="btc-raw-trades",
                dead_letter_topic="btc-dead-letter",
            )
            config = mock_class.call_args[0][0]
            assert config["bootstrap.servers"] == "broker:9092"
            assert config["sasl.username"] == "user"
            assert config["sasl.password"] == "pass"
            assert config["security.protocol"] == "SASL_SSL"
            assert config["sasl.mechanism"] == "SCRAM-SHA-256"
            assert config["acks"] == "all"


# ─── publish_trade Tests ───────────────────────────────────────────────────────

class TestPublishTrade:

    def test_produce_called_once(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        assert mock_producer.produce.call_count == 1

    def test_produce_uses_raw_trades_topic(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "btc-raw-trades"

    def test_produce_key_is_trade_id(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == str(valid_event.trade_id).encode("utf-8")

    def test_payload_is_valid_json(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        raw_value: bytes = mock_producer.produce.call_args[1]["value"]
        parsed = json.loads(raw_value.decode("utf-8"))
        assert "price" in parsed
        assert "quantity" in parsed
        assert "symbol" in parsed

    def test_price_serialised_as_string(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        """Decimal must never silently become a float."""
        publisher.publish_trade(valid_event)
        raw_value: bytes = mock_producer.produce.call_args[1]["value"]
        parsed = json.loads(raw_value.decode("utf-8"))
        assert isinstance(parsed["price"], str)
        assert parsed["price"] == "43500.50"

    def test_quantity_serialised_as_string(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        raw_value: bytes = mock_producer.produce.call_args[1]["value"]
        parsed = json.loads(raw_value.decode("utf-8"))
        assert isinstance(parsed["quantity"], str)
        assert parsed["quantity"] == "0.002"

    def test_poll_called_after_produce(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        """poll(0) must be called to trigger delivery callbacks."""
        publisher.publish_trade(valid_event)
        mock_producer.poll.assert_called_once_with(0)

    def test_kafka_exception_propagates(
        self, publisher: KafkaPublisher, mock_producer, valid_event: BinanceTradeEvent
    ) -> None:
        """KafkaException must propagate — caller owns retry strategy."""
        from confluent_kafka import KafkaException
        mock_producer.produce.side_effect = KafkaException("broker unavailable")
        with pytest.raises(KafkaException):
            publisher.publish_trade(valid_event)


# ─── publish_dead_letter Tests ─────────────────────────────────────────────────

class TestPublishDeadLetter:

    def test_dead_letter_routed_to_correct_topic(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        publisher.publish_dead_letter(raw_message='{"e": "aggTrade"}', error="wrong type")
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "btc-dead-letter"

    def test_dead_letter_payload_contains_error(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        publisher.publish_dead_letter(raw_message="{}", error="missing required fields")
        raw_value: bytes = mock_producer.produce.call_args[1]["value"]
        parsed = json.loads(raw_value.decode("utf-8"))
        assert parsed["error"] == "missing required fields"
        assert parsed["raw_message"] == "{}"

    def test_dead_letter_payload_contains_source(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        publisher.publish_dead_letter(raw_message="{}", error="any")
        raw_value: bytes = mock_producer.produce.call_args[1]["value"]
        parsed = json.loads(raw_value.decode("utf-8"))
        assert parsed["source"] == "binance-ws-btcusdt"

    def test_dead_letter_publish_failure_does_not_raise(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        """Dead-letter failures must be swallowed — never crash the loop."""
        mock_producer.produce.side_effect = Exception("catastrophic failure")
        # Should not raise
        publisher.publish_dead_letter(raw_message='{"broken": true}', error="test error")

    def test_dead_letter_poll_called(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        publisher.publish_dead_letter(raw_message="{}", error="any")
        mock_producer.poll.assert_called_with(0)


# ─── flush Tests ──────────────────────────────────────────────────────────────

class TestFlush:

    def test_flush_delegates_to_producer(
        self, publisher: KafkaPublisher, mock_producer
    ) -> None:
        publisher.flush()
        mock_producer.flush.assert_called_once()


# ─── Decimal Serialiser Tests ──────────────────────────────────────────────────

class TestDecimalSerializer:

    def test_decimal_serialised_as_string(self) -> None:
        assert _decimal_serializer(Decimal("123.456")) == "123.456"

    def test_non_decimal_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            _decimal_serializer({"not": "decimal"})

    def test_high_precision_decimal_preserved(self) -> None:
        value = Decimal("43250.123456789012345678")
        assert _decimal_serializer(value) == "43250.123456789012345678"