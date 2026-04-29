"""
Unit tests for BinanceTradeEvent and DeadLetterEnvelope Pydantic schemas.

Coverage targets:
  - Valid payload → successful model construction
  - Each field validator (price, quantity, event_type, symbol)
  - Cross-field model validator (timestamp drift)
  - DeadLetterEnvelope construction
  - Alias-based field population (Binance uses single-char aliases)
"""

import pytest
from decimal import Decimal
from pydantic import ValidationError

from src.schema import BinanceTradeEvent, DeadLetterEnvelope


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_payload() -> dict:
    """Minimal valid Binance trade event payload using Binance field aliases."""
    return {
        "e": "trade",
        "E": 1700000001000,
        "s": "BTCUSDT",
        "t": 123456789,
        "p": "43250.75",
        "q": "0.00125",
        "T": 1700000000500,
        "m": False,
    }


# ─── Valid Payload Tests ───────────────────────────────────────────────────────

class TestBinanceTradeEventValid:

    def test_valid_payload_parses_successfully(self, valid_payload: dict) -> None:
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert event.symbol == "BTCUSDT"
        assert event.event_type == "trade"
        assert event.trade_id == 123456789
        assert event.is_market_maker is False

    def test_price_parsed_as_decimal(self, valid_payload: dict) -> None:
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert isinstance(event.price, Decimal)
        assert event.price == Decimal("43250.75")

    def test_quantity_parsed_as_decimal(self, valid_payload: dict) -> None:
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert isinstance(event.quantity, Decimal)
        assert event.quantity == Decimal("0.00125")

    def test_alias_fields_map_correctly(self, valid_payload: dict) -> None:
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert event.event_time_ms == 1700000001000
        assert event.trade_time_ms == 1700000000500

    def test_market_maker_true(self, valid_payload: dict) -> None:
        valid_payload["m"] = True
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert event.is_market_maker is True

    def test_large_price_precision_preserved(self, valid_payload: dict) -> None:
        """Decimal must preserve precision that float would lose."""
        valid_payload["p"] = "43250.123456789012345"
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert str(event.price) == "43250.123456789012345"


# ─── Field Validator Tests ─────────────────────────────────────────────────────

class TestBinanceTradeEventFieldValidation:

    def test_wrong_event_type_rejected(self, valid_payload: dict) -> None:
        valid_payload["e"] = "aggTrade"
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "trade" in str(exc_info.value)

    def test_wrong_symbol_rejected(self, valid_payload: dict) -> None:
        valid_payload["s"] = "ETHUSDT"
        with pytest.raises(ValidationError):
            BinanceTradeEvent.model_validate(valid_payload)

    def test_zero_price_rejected(self, valid_payload: dict) -> None:
        valid_payload["p"] = "0"
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "price must be > 0" in str(exc_info.value)

    def test_negative_price_rejected(self, valid_payload: dict) -> None:
        valid_payload["p"] = "-100.00"
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "price must be > 0" in str(exc_info.value)

    def test_zero_quantity_rejected(self, valid_payload: dict) -> None:
        valid_payload["q"] = "0"
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "quantity must be > 0" in str(exc_info.value)

    def test_negative_quantity_rejected(self, valid_payload: dict) -> None:
        valid_payload["q"] = "-0.001"
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "quantity must be > 0" in str(exc_info.value)

    def test_zero_trade_id_rejected(self, valid_payload: dict) -> None:
        valid_payload["t"] = 0
        with pytest.raises(ValidationError):
            BinanceTradeEvent.model_validate(valid_payload)

    def test_zero_event_time_rejected(self, valid_payload: dict) -> None:
        valid_payload["E"] = 0
        with pytest.raises(ValidationError):
            BinanceTradeEvent.model_validate(valid_payload)

    def test_missing_required_field_rejected(self, valid_payload: dict) -> None:
        del valid_payload["p"]
        with pytest.raises(ValidationError):
            BinanceTradeEvent.model_validate(valid_payload)

    def test_non_numeric_price_rejected(self, valid_payload: dict) -> None:
        valid_payload["p"] = "not-a-number"
        with pytest.raises((ValidationError, Exception)):
            BinanceTradeEvent.model_validate(valid_payload)


# ─── Cross-field Validator Tests ───────────────────────────────────────────────

class TestBinanceTradeEventModelValidator:

    def test_trade_time_within_tolerance_accepted(self, valid_payload: dict) -> None:
        """trade_time_ms up to 5s before event_time_ms is acceptable."""
        valid_payload["E"] = 1700000005000
        valid_payload["T"] = 1700000000000   # exactly 5s before — boundary
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert event is not None

    def test_trade_time_far_after_event_time_rejected(self, valid_payload: dict) -> None:
        """trade_time_ms more than 5s AFTER event_time_ms signals corrupt data."""
        valid_payload["E"] = 1700000000000
        valid_payload["T"] = 1700000006000   # 6s after event_time
        with pytest.raises(ValidationError) as exc_info:
            BinanceTradeEvent.model_validate(valid_payload)
        assert "corrupt timestamp" in str(exc_info.value)

    def test_trade_time_slightly_after_event_time_accepted(self, valid_payload: dict) -> None:
        """Small jitter (< 5s) between trade_time and event_time is acceptable."""
        valid_payload["E"] = 1700000000000
        valid_payload["T"] = 1700000004999   # 4.999s after — within tolerance
        event = BinanceTradeEvent.model_validate(valid_payload)
        assert event is not None


# ─── DeadLetterEnvelope Tests ──────────────────────────────────────────────────

class TestDeadLetterEnvelope:

    def test_dead_letter_envelope_construction(self) -> None:
        envelope = DeadLetterEnvelope(
            raw_message='{"e": "aggTrade"}',
            error="event_type must be 'trade'",
        )
        assert envelope.raw_message == '{"e": "aggTrade"}'
        assert envelope.error == "event_type must be 'trade'"
        assert envelope.source == "binance-ws-btcusdt"

    def test_dead_letter_envelope_custom_source(self) -> None:
        envelope = DeadLetterEnvelope(
            raw_message="{}",
            error="empty payload",
            source="binance-ws-ethusdt",
        )
        assert envelope.source == "binance-ws-ethusdt"

    def test_dead_letter_serialises_to_json(self) -> None:
        envelope = DeadLetterEnvelope(
            raw_message='{"bad": "data"}',
            error="schema mismatch",
        )
        json_str = envelope.model_dump_json()
        assert "raw_message" in json_str
        assert "schema mismatch" in json_str