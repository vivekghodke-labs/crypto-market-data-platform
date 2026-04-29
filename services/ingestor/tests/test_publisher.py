"""
test_publisher.py
-----------------
Unit tests for PubSubPublisher.

Strategy:
  - google-cloud-pubsub is mocked at the class level — no GCP credentials
    required and no network calls made.
  - Tests verify: topic path construction, payload serialisation, attribute
    setting, dead-letter routing, and error handling behaviour.
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

import pytest
from google.api_core.exceptions import ServiceUnavailable

from src.publisher import PubSubPublisher, _decimal_serialiser
from src.schema import BinanceTradeEvent


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_pubsub_client():
    """Patches PublisherClient at the module level for all publisher tests."""
    with patch("src.publisher.pubsub_v1.PublisherClient") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance

        # topic_path returns a deterministic string
        mock_instance.topic_path.side_effect = (
            lambda project, topic: f"projects/{project}/topics/{topic}"
        )

        # publish returns a Future-like mock that resolves to a message ID
        mock_future = MagicMock()
        mock_future.result.return_value = "mock-message-id-001"
        mock_instance.publish.return_value = mock_future

        yield mock_instance


@pytest.fixture
def publisher(mock_pubsub_client) -> PubSubPublisher:
    return PubSubPublisher(
        project_id="vg-ind-2026",
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

class TestPubSubPublisherInit:

    def test_topic_paths_constructed_correctly(self, mock_pubsub_client) -> None:
        PubSubPublisher(
            project_id="vg-ind-2026",
            raw_trades_topic="btc-raw-trades",
            dead_letter_topic="btc-dead-letter",
        )
        mock_pubsub_client.topic_path.assert_any_call("vg-ind-2026", "btc-raw-trades")
        mock_pubsub_client.topic_path.assert_any_call("vg-ind-2026", "btc-dead-letter")


# ─── publish_trade Tests ───────────────────────────────────────────────────────

class TestPublishTrade:

    def test_publish_trade_calls_publish_once(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        assert mock_pubsub_client.publish.call_count == 1

    def test_publish_trade_uses_raw_trades_topic(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        call_args = mock_pubsub_client.publish.call_args
        topic_path = call_args[0][0]
        assert "btc-raw-trades" in topic_path

    def test_publish_trade_payload_is_valid_json(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        call_args = mock_pubsub_client.publish.call_args
        raw_payload: bytes = call_args[0][1]
        parsed = json.loads(raw_payload.decode("utf-8"))
        assert "price" in parsed
        assert "quantity" in parsed
        assert "symbol" in parsed

    def test_publish_trade_price_serialised_as_string(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        """Decimal must be serialised as string — never as float."""
        publisher.publish_trade(valid_event)
        call_args = mock_pubsub_client.publish.call_args
        raw_payload: bytes = call_args[0][1]
        parsed = json.loads(raw_payload.decode("utf-8"))
        assert isinstance(parsed["price"], str)
        assert parsed["price"] == "43500.50"

    def test_publish_trade_sets_message_attributes(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        publisher.publish_trade(valid_event)
        call_kwargs = mock_pubsub_client.publish.call_args[1]
        assert call_kwargs.get("symbol") == "BTCUSDT"
        assert call_kwargs.get("trade_id") == str(valid_event.trade_id)

    def test_publish_trade_raises_on_gcp_error(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        """Publish failures must propagate — caller decides on retry strategy."""
        mock_future = MagicMock()
        mock_future.result.side_effect = ServiceUnavailable("GCP unavailable")
        mock_pubsub_client.publish.return_value = mock_future

        with pytest.raises(ServiceUnavailable):
            publisher.publish_trade(valid_event)

    def test_publish_trade_raises_on_timeout(
        self, publisher: PubSubPublisher, mock_pubsub_client, valid_event: BinanceTradeEvent
    ) -> None:
        mock_future = MagicMock()
        mock_future.result.side_effect = TimeoutError("publish timed out")
        mock_pubsub_client.publish.return_value = mock_future

        with pytest.raises(TimeoutError):
            publisher.publish_trade(valid_event)


# ─── publish_dead_letter Tests ─────────────────────────────────────────────────

class TestPublishDeadLetter:

    def test_dead_letter_routed_to_correct_topic(
        self, publisher: PubSubPublisher, mock_pubsub_client
    ) -> None:
        publisher.publish_dead_letter(
            raw_message='{"e": "aggTrade"}',
            error="wrong event_type",
        )
        call_args = mock_pubsub_client.publish.call_args
        topic_path = call_args[0][0]
        assert "btc-dead-letter" in topic_path

    def test_dead_letter_payload_contains_error(
        self, publisher: PubSubPublisher, mock_pubsub_client
    ) -> None:
        publisher.publish_dead_letter(
            raw_message='{}',
            error="missing required fields",
        )
        call_args = mock_pubsub_client.publish.call_args
        raw_payload: bytes = call_args[0][1]
        parsed = json.loads(raw_payload.decode("utf-8"))
        assert parsed["error"] == "missing required fields"
        assert parsed["raw_message"] == "{}"

    def test_dead_letter_publish_failure_does_not_raise(
        self, publisher: PubSubPublisher, mock_pubsub_client
    ) -> None:
        """Dead-letter publish failure must be swallowed — never crash the loop."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("catastrophic GCP failure")
        mock_pubsub_client.publish.return_value = mock_future

        # Should not raise
        publisher.publish_dead_letter(
            raw_message='{"broken": true}',
            error="test error",
        )

    def test_dead_letter_sets_rejection_reason_attribute(
        self, publisher: PubSubPublisher, mock_pubsub_client
    ) -> None:
        publisher.publish_dead_letter(raw_message="{}", error="bad data")
        call_kwargs = mock_pubsub_client.publish.call_args[1]
        assert call_kwargs.get("rejection_reason") == "schema_validation_failure"


# ─── Decimal Serialiser Tests ──────────────────────────────────────────────────

class TestDecimalSerialiser:

    def test_decimal_serialised_as_string(self) -> None:
        assert _decimal_serialiser(Decimal("123.456")) == "123.456"

    def test_non_decimal_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            _decimal_serialiser({"not": "decimal"})

    def test_high_precision_decimal_preserved(self) -> None:
        value = Decimal("43250.123456789012345678")
        result = _decimal_serialiser(value)
        assert result == "43250.123456789012345678"