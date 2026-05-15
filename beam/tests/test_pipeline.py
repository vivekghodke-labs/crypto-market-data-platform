"""
End-to-end pipeline tests using Apache Beam's TestPipeline.
Runs from inside the beam/ directory — imports are src-relative.
"""

import json
from decimal import Decimal

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from apache_beam.transforms.window import FixedWindows

import pytest

from src.schema import TradeRecord
from src.transforms import (
    ParseAndValidate,
    AssignEventTimestamp,
    ExtractKey,
    OHLCVCombineFn,
    FormatOHLCV,
    DEAD_LETTER_TAG,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_raw(
    trade_id: int,
    price: str,
    quantity: str,
    trade_time_ms: int,
    symbol: str = "BTCUSDT",
) -> bytes:
    return json.dumps({
        "event_type": "trade",
        "event_time_ms": trade_time_ms + 100,
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "trade_time_ms": trade_time_ms,
    }).encode("utf-8")


def _run_ohlcv_pipeline(raw_messages: list, checker_fn) -> None:
    """Runs the pipeline and applies the checker_fn using assert_that."""
    with TestPipeline() as p:
        raw = p | "Create" >> beam.Create(raw_messages)
        parsed = raw | "Parse" >> ParseAndValidate()
        valid = parsed["valid"]
        timestamped = valid | "Timestamp" >> beam.ParDo(AssignEventTimestamp())
        
        # Note: We omit accumulation_mode and allowed_lateness in the test graph 
        # so that TestPipeline's batch execution doesn't falsely drop elements as late data.
        windowed = timestamped | "Window" >> beam.WindowInto(FixedWindows(60))
        
        keyed = windowed | "Key" >> beam.ParDo(ExtractKey())
        aggregated = keyed | "Aggregate" >> beam.CombinePerKey(OHLCVCombineFn())
        formatted = aggregated | "Format" >> beam.ParDo(FormatOHLCV())
        
        assert_that(formatted, checker_fn)


# ─── End-to-End Pipeline Tests ────────────────────────────────────────────────

class TestPipelineEndToEnd:

    def test_single_trade_produces_one_ohlcv_record(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 1:
                raise AssertionError(f"Expected 1 record, got {len(results)}: {results}")
            r = results[0]
            assert r["symbol"] == "BTCUSDT"
            assert r["open"] == r["close"] == r["high"] == r["low"] == "43000.00"
            assert r["trade_count"] == 1

        _run_ohlcv_pipeline([
            _make_raw(1, "43000.00", "0.01", 1700000030000)
        ], check)

    def test_multiple_trades_same_window_aggregated(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 1:
                raise AssertionError(f"Expected 1 record, got {len(results)}: {results}")
            r = results[0]
            assert r["trade_count"] == 3
            assert r["high"] == "44000.00"
            assert r["low"] == "42500.00"
            assert r["open"] == "43000.00"
            assert r["close"] == "42500.00"

        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),
            _make_raw(2, "44000.00", "0.02", 1700000020000),
            # Changed 1700000050000 -> 1700000030000 so it falls inside the same 60s epoch window
            _make_raw(3, "42500.00", "0.01", 1700000030000), 
        ]
        _run_ohlcv_pipeline(raw, check)

    def test_volume_calculated_correctly(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 1:
                raise AssertionError(f"Expected 1 record, got {len(results)}: {results}")
            assert Decimal(results[0]["volume"]) == Decimal("1400.00")

        raw = [
            _make_raw(1, "40000.00", "0.01", 1700000010000),
            _make_raw(2, "50000.00", "0.02", 1700000020000),
        ]
        _run_ohlcv_pipeline(raw, check)

    def test_trades_different_windows_produce_separate_records(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 2:
                raise AssertionError(f"Expected 2 records, got {len(results)}: {results}")

        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),  # window [...980, ...040)
            _make_raw(2, "44000.00", "0.01", 1700000090000),  # window [...040, ...100)
        ]
        _run_ohlcv_pipeline(raw, check)

    def test_invalid_message_does_not_produce_ohlcv(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 0:
                raise AssertionError(f"Expected 0 records, got {len(results)}: {results}")

        _run_ohlcv_pipeline([b"not valid json"], check)

    def test_mixed_valid_invalid_only_valid_aggregated(self) -> None:
        def check(actual):
            results = list(actual)
            if len(results) != 1:
                raise AssertionError(f"Expected 1 record, got {len(results)}: {results}")
            assert results[0]["trade_count"] == 2

        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),
            b"garbage message",
            _make_raw(2, "44000.00", "0.01", 1700000020000),
        ]
        _run_ohlcv_pipeline(raw, check)

    def test_ohlcv_record_has_all_required_fields(self) -> None:
        def check(actual):
            results = list(actual)
            if not results:
                raise AssertionError("Results list is empty")
            required = {"window_start", "window_end", "symbol", "open", "high", "low",
                        "close", "volume", "trade_count", "ingested_at"}
            assert required.issubset(results[0].keys())
            
        _run_ohlcv_pipeline([_make_raw(1, "43000.00", "0.01", 1700000010000)], check)

    def test_window_start_before_window_end(self) -> None:
        def check(actual):
            results = list(actual)
            if not results:
                raise AssertionError("Results list is empty")
            assert results[0]["window_start"] < results[0]["window_end"]

        _run_ohlcv_pipeline([_make_raw(1, "43000.00", "0.01", 1700000010000)], check)

    def test_decimal_precision_preserved_as_string(self) -> None:
        def check(actual):
            results = list(actual)
            if not results:
                raise AssertionError("Results list is empty")
            assert isinstance(results[0]["open"], str)
            assert isinstance(results[0]["volume"], str)

        _run_ohlcv_pipeline([_make_raw(1, "43250.123456789", "0.00100001", 1700000010000)], check)


# ─── Dead Letter Routing Tests ────────────────────────────────────────────────

class TestDeadLetterRouting:

    def test_invalid_messages_routed_to_dead_letter_tag(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([b"bad json", b"also bad"])
            parsed = raw | ParseAndValidate()
            assert_that(parsed["valid"], is_empty(), label="valid_empty")
            assert_that(
                parsed[DEAD_LETTER_TAG],
                equal_to(["bad json", "also bad"]),
                label="dead_letter_check",
            )

    def test_valid_messages_not_in_dead_letter(self) -> None:
        raw_bytes = _make_raw(1, "43000.00", "0.01", 1700000010000)
        with TestPipeline() as p:
            raw = p | beam.Create([raw_bytes])
            parsed = raw | ParseAndValidate()
            assert_that(parsed[DEAD_LETTER_TAG], is_empty(), label="dl_empty")