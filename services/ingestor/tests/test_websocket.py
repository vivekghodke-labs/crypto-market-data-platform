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

from src.websocket_client import (
    BinanceWebSocketClient,
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

# ─── Run & Lifecycle Tests ─────────────────────────────────────────────────────

class TestRun:

    async def test_run_handles_cancelled_error(
        self, client: BinanceWebSocketClient
    ) -> None:
        """Test that task cancellation cleanly shuts down the loop."""
        # Mock _connect_and_consume to immediately raise CancelledError
        with patch.object(
            client, "_connect_and_consume", side_effect=asyncio.CancelledError
        ):
            await client.run()
        
        # Should exit the loop and set _running to False
        assert client._running is False

    async def test_run_handles_exception_and_backs_off(
        self, client: BinanceWebSocketClient
    ) -> None:
        """Test that unhandled exceptions in the loop trigger a backoff sleep."""
        # First call raises Exception, second call stops the loop to avoid infinite loop
        call_counter = 0

        async def mock_connect():
            nonlocal call_counter
            call_counter += 1
            if call_counter == 1:
                raise Exception("Network failure")
            else:
                client._running = False

        with patch.object(client, "_connect_and_consume", side_effect=mock_connect):
            with patch.object(client, "_backoff_sleep") as mock_backoff:
                await client.run()

        # It should have caught the exception and called _backoff_sleep once
        mock_backoff.assert_called_once()
        assert client._running is False

    async def test_run_clean_exit_when_stopped(
        self, client: BinanceWebSocketClient
    ) -> None:
        """Test that if _connect_and_consume returns cleanly and running=False, it exits."""
        async def mock_connect():
            await client.stop()

        with patch.object(client, "_connect_and_consume", side_effect=mock_connect):
            with patch.object(client, "_backoff_sleep") as mock_backoff:
                await client.run()

        # Loop should exit gracefully without calling backoff
        mock_backoff.assert_not_called()
        assert client._running is False


# ─── Connect and Consume Tests ─────────────────────────────────────────────────

class TestConnectAndConsume:

    @patch("src.websocket_client.websockets.connect")
    async def test_connect_and_consume_processes_messages(
        self, mock_connect: MagicMock, client: BinanceWebSocketClient
    ) -> None:
        """Test the async context manager and async iterator of the websocket."""
        # 1. Setup mock websocket that yields two messages
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = ["message_1", "message_2"]
        
        # 2. Setup the mock context manager returned by websockets.connect
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_ws
        mock_connect.return_value = mock_context_manager

        # Set an arbitrary reconnect attempt to verify it gets reset
        client._reconnect_attempt = 5

        with patch.object(client, "_process_message") as mock_process:
            await client._connect_and_consume()

        # Assertions
        assert client._reconnect_attempt == 0
        assert mock_process.call_count == 2
        mock_process.assert_has_calls([
            call("message_1"),
            call("message_2")
        ])