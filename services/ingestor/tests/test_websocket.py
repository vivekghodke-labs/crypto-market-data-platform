"""
test_websocket.py
-----------------
Unit tests for BinanceWebSocketClient.

Strategy:
  - websockets.connect is mocked — no real network connections.
  - Publisher is fully mocked — tests isolate WebSocket logic only.
  - Async tests use pytest-asyncio with asyncio mode = auto.

Coverage targets:
  - Valid message → publisher.publish_trade called
  - Invalid JSON → dead-letter published, loop continues
  - Pydantic ValidationError → dead-letter published, loop continues
  - Unhandled exception in _process_message → dead-letter, loop continues
  - Backoff sleep duration increases exponentially
  - Backoff resets to 0 on successful connection
  - stats property reflects correct counters
  - stop() causes run() to exit cleanly
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
from pydantic import ValidationError

from src.websocket_client import (
    BinanceWebSocketClient,
    _BACKOFF_BASE_SECONDS,
    _BACKOFF_MAX_SECONDS,
)


# ─── pytest-asyncio configuration ─────────────────────────────────────────────
pytestmark = pytest.mark.asyncio


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_publisher() -> MagicMock:
    publisher = MagicMock()
    publisher.publish_trade = MagicMock()
    publisher.publish_dead_letter = MagicMock()
    return publisher


@pytest.fixture
def client(mock_publisher) -> BinanceWebSocketClient:
    return BinanceWebSocketClient(
        ws_url="wss://stream.binance.com:9443/ws/btcusdt@trade",
        publisher=mock_publisher,
    )


def _make_valid_raw_message() -> str:
    return json.dumps({
        "e": "trade",
        "E": 1700000001000,
        "s": "BTCUSDT",
        "t": 555001,
        "p": "43100.00",
        "q": "0.003",
        "T": 1700000000900,
        "m": False,
    })


# ─── _process_message Tests ────────────────────────────────────────────────────

class TestProcessMessage:

    async def test_valid_message_calls_publish_trade(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message(_make_valid_raw_message())
        assert mock_publisher.publish_trade.call_count == 1
        assert mock_publisher.publish_dead_letter.call_count == 0

    async def test_valid_message_increments_processed_counter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message(_make_valid_raw_message())
        assert client.stats["messages_processed"] == 1
        assert client.stats["messages_rejected"] == 0

    async def test_invalid_json_routes_to_dead_letter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message("this is not json {{{")
        assert mock_publisher.publish_dead_letter.call_count == 1
        assert mock_publisher.publish_trade.call_count == 0

    async def test_invalid_json_increments_rejected_counter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message("not json")
        assert client.stats["messages_rejected"] == 1
        assert client.stats["messages_processed"] == 0

    async def test_schema_validation_failure_routes_to_dead_letter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        """Wrong event_type → ValidationError → dead-letter."""
        bad_payload = json.dumps({
            "e": "aggTrade",  # Wrong — must be "trade"
            "E": 1700000001000,
            "s": "BTCUSDT",
            "t": 555001,
            "p": "43100.00",
            "q": "0.003",
            "T": 1700000000900,
            "m": False,
        })
        await client._process_message(bad_payload)
        assert mock_publisher.publish_dead_letter.call_count == 1
        assert mock_publisher.publish_trade.call_count == 0

    async def test_wrong_symbol_routes_to_dead_letter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        bad_payload = json.dumps({
            "e": "trade",
            "E": 1700000001000,
            "s": "ETHUSDT",
            "t": 555001,
            "p": "2400.00",
            "q": "0.1",
            "T": 1700000000900,
            "m": False,
        })
        await client._process_message(bad_payload)
        assert mock_publisher.publish_dead_letter.call_count == 1

    async def test_publish_error_routes_to_dead_letter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        """If publish_trade raises, the message goes to dead-letter."""
        mock_publisher.publish_trade.side_effect = Exception("GCP down")
        await client._process_message(_make_valid_raw_message())
        assert mock_publisher.publish_dead_letter.call_count == 1

    async def test_multiple_valid_messages_accumulate_counter(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        for _ in range(5):
            await client._process_message(_make_valid_raw_message())
        assert client.stats["messages_processed"] == 5

    async def test_mixed_valid_invalid_messages(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message(_make_valid_raw_message())
        await client._process_message("bad json")
        await client._process_message(_make_valid_raw_message())
        assert client.stats["messages_processed"] == 2
        assert client.stats["messages_rejected"] == 1


# ─── Backoff Tests ─────────────────────────────────────────────────────────────

class TestBackoffSleep:

    async def test_backoff_increments_attempt_counter(
        self, client: BinanceWebSocketClient
    ) -> None:
        with patch("src.websocket_client.asyncio.sleep", new_callable=AsyncMock):
            assert client._reconnect_attempt == 0
            await client._backoff_sleep()
            assert client._reconnect_attempt == 1
            await client._backoff_sleep()
            assert client._reconnect_attempt == 2

    async def test_backoff_sleep_duration_increases(
        self, client: BinanceWebSocketClient
    ) -> None:
        sleep_durations = []

        async def capture_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with patch("src.websocket_client.asyncio.sleep", side_effect=capture_sleep):
            with patch("src.websocket_client.random.uniform", return_value=0.0):
                await client._backoff_sleep()  # attempt 1 → 1s
                await client._backoff_sleep()  # attempt 2 → 2s
                await client._backoff_sleep()  # attempt 3 → 4s

        assert sleep_durations[0] < sleep_durations[1] < sleep_durations[2]

    async def test_backoff_capped_at_max(
        self, client: BinanceWebSocketClient
    ) -> None:
        # Simulate many failed reconnects
        client._reconnect_attempt = 100

        sleep_durations = []

        async def capture_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with patch("src.websocket_client.asyncio.sleep", side_effect=capture_sleep):
            with patch("src.websocket_client.random.uniform", return_value=0.0):
                await client._backoff_sleep()

        # With jitter = 0, sleep should equal _BACKOFF_MAX_SECONDS exactly
        assert sleep_durations[0] == _BACKOFF_MAX_SECONDS


# ─── Stats Tests ───────────────────────────────────────────────────────────────

class TestStats:

    def test_initial_stats(self, client: BinanceWebSocketClient) -> None:
        stats = client.stats
        assert stats["messages_processed"] == 0
        assert stats["messages_rejected"] == 0
        assert stats["reconnect_attempts"] == 0
        assert stats["running"] is False

    async def test_stats_reflect_processed_messages(
        self, client: BinanceWebSocketClient, mock_publisher: MagicMock
    ) -> None:
        await client._process_message(_make_valid_raw_message())
        await client._process_message("bad")
        stats = client.stats
        assert stats["messages_processed"] == 1
        assert stats["messages_rejected"] == 1


# ─── stop() Tests ─────────────────────────────────────────────────────────────

class TestStop:

    async def test_stop_sets_running_false(
        self, client: BinanceWebSocketClient
    ) -> None:
        client._running = True
        await client.stop()
        assert client._running is False