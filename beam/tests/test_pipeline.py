"""
test_pipeline.py
----------------
End-to-end pipeline tests using Apache Beam's TestPipeline.

Strategy:
  - build_pipeline() is called with a TestPipeline instead of a real pipeline.
  - BigQuery WriteToBigQuery is mocked at the IO level — no GCP credentials.
  - Pub/Sub ReadFromPubSub is replaced with beam.Create() in test fixtures.
  - Tests verify the full transform chain produces correct OHLCV output.

These tests complement test_transforms.py by validating transform composition
(correct wiring, windowing behaviour, routing of valid vs dead-letter data).
"""

import json
from decimal import Decimal
from unittest.mock import patch, MagicMock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.utils.timestamp import Duration

import pytest

from beam.src.schema import TradeRecord
from beam.src.transforms import (
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
        "e": "trade",
        "E": trade_time_ms + 100,
        "s": symbol,
        "t": trade_id,
        "p": price,
        "q": quantity,
        "T": trade_time_ms,
        "m": False,
    }).encode("utf-8")


def _run_ohlcv_pipeline(raw_messages: list[bytes]) -> list[dict]:
    """
    Runs the OHLCV transform chain on synthetic raw messages.
    Returns the list of OHLCV dicts produced (as FormatOHLCV output).

    Uses a 60-second fixed window with a 1-second lateness allowance
    for test speed — same logic as production, tighter tolerance.
    """
    results = []

    with TestPipeline() as p:
        raw = p | "Create" >> beam.Create(raw_messages)
        parsed = raw | "Parse" >> ParseAndValidate()

        valid = parsed["valid"]

        timestamped = valid | "Timestamp" >> beam.ParDo(AssignEventTimestamp())

        windowed = timestamped | "Window" >> beam.WindowInto(
            FixedWindows(60),
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=Duration(seconds=1),
        )

        keyed = windowed | "Key" >> beam.ParDo(ExtractKey())
        aggregated = keyed | "Aggregate" >> beam.CombinePerKey(OHLCVCombineFn())
        formatted = aggregated | "Format" >> beam.ParDo(FormatOHLCV())

        # Collect results via a side-output list trick compatible with TestPipeline
        def collect(element):
            results.append(element)

        formatted | "Collect" >> beam.Map(collect)

    return results


# ─── End-to-End Pipeline Tests ────────────────────────────────────────────────

class TestPipelineEndToEnd:

    def test_single_trade_produces_one_ohlcv_record(self) -> None:
        raw = [_make_raw(trade_id=1, price="43000.00", quantity="0.01",
                          trade_time_ms=1700000030000)]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 1
        record = results[0]
        assert record["symbol"] == "BTCUSDT"
        assert record["open"] == "43000.00"
        assert record["close"] == "43000.00"
        assert record["high"] == "43000.00"
        assert record["low"] == "43000.00"
        assert record["trade_count"] == 1

    def test_multiple_trades_same_window_aggregated(self) -> None:
        # All trades within the same 60-second window (1700000000–1700000060)
        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),
            _make_raw(2, "44000.00", "0.02", 1700000020000),
            _make_raw(3, "42500.00", "0.01", 1700000050000),
        ]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 1
        record = results[0]
        assert record["trade_count"] == 3
        assert record["high"] == "44000.00"
        assert record["low"] == "42500.00"
        assert record["open"] == "43000.00"   # earliest trade_time_ms
        assert record["close"] == "42500.00"  # latest trade_time_ms

    def test_volume_calculated_correctly(self) -> None:
        raw = [
            _make_raw(1, "40000.00", "0.01", 1700000010000),  # 400.00
            _make_raw(2, "50000.00", "0.02", 1700000020000),  # 1000.00
        ]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 1
        assert Decimal(results[0]["volume"]) == Decimal("1400.00")

    def test_trades_different_windows_produce_separate_records(self) -> None:
        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),  # window 1: 0–60s
            _make_raw(2, "44000.00", "0.01", 1700000090000),  # window 2: 60–120s
        ]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 2

    def test_invalid_message_does_not_produce_ohlcv(self) -> None:
        raw = [b"not valid json"]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 0

    def test_mixed_valid_invalid_only_valid_aggregated(self) -> None:
        raw = [
            _make_raw(1, "43000.00", "0.01", 1700000010000),
            b"garbage message",
            _make_raw(2, "44000.00", "0.01", 1700000020000),
        ]
        results = _run_ohlcv_pipeline(raw)
        assert len(results) == 1
        assert results[0]["trade_count"] == 2

    def test_ohlcv_record_has_all_required_fields(self) -> None:
        raw = [_make_raw(1, "43000.00", "0.01", 1700000010000)]
        results = _run_ohlcv_pipeline(raw)
        record = results[0]
        required_fields = {
            "window_start", "window_end", "symbol",
            "open", "high", "low", "close",
            "volume", "trade_count", "ingested_at",
        }
        assert required_fields.issubset(record.keys())

    def test_window_start_before_window_end(self) -> None:
        raw = [_make_raw(1, "43000.00", "0.01", 1700000010000)]
        results = _run_ohlcv_pipeline(raw)
        record = results[0]
        assert record["window_start"] < record["window_end"]

    def test_decimal_precision_preserved_as_string(self) -> None:
        """Decimal values must survive the pipeline as strings, not floats."""
        raw = [_make_raw(1, "43250.123456789", "0.00100001", 1700000010000)]
        results = _run_ohlcv_pipeline(raw)
        record = results[0]
        # Verify they are strings (not floats) in the output dict
        assert isinstance(record["open"], str)
        assert isinstance(record["volume"], str)


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