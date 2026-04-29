"""
test_transforms.py
------------------
Unit tests for all Beam PTransforms in transforms.py.

All tests use Apache Beam's TestPipeline — an in-memory runner that
executes the pipeline synchronously with no GCP credentials required.

Test strategy:
  - Each transform is tested in isolation where possible.
  - OHLCVCombineFn is tested directly (not via pipeline) for
    accumulator logic — faster and more precise than pipeline tests.
  - ParseAndValidate is tested end-to-end through the pipeline because
    tagged outputs require the Beam runner to materialise both branches.
"""

import json
from decimal import Decimal

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from apache_beam.transforms.window import FixedWindows

import pytest

from beam.src.schema import TradeRecord, OHLCVAccumulator
from beam.src.transforms import (
    ParseAndValidate,
    AssignEventTimestamp,
    ExtractKey,
    OHLCVCombineFn,
    FormatOHLCV,
    DEAD_LETTER_TAG,
    _ms_to_iso,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_raw_message(
    event_type: str = "trade",
    symbol: str = "BTCUSDT",
    trade_id: int = 100001,
    price: str = "43000.00",
    quantity: str = "0.005",
    trade_time_ms: int = 1700000030000,
    event_time_ms: int = 1700000030100,
) -> bytes:
    payload = {
        "e": event_type,
        "E": event_time_ms,
        "s": symbol,
        "t": trade_id,
        "p": price,
        "q": quantity,
        "T": trade_time_ms,
        "m": False,
    }
    return json.dumps(payload).encode("utf-8")


def _make_trade_record(
    trade_id: int = 100001,
    symbol: str = "BTCUSDT",
    price: str = "43000.00",
    quantity: str = "0.005",
    trade_time_ms: int = 1700000030000,
) -> TradeRecord:
    return TradeRecord(
        trade_id=trade_id,
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(quantity),
        trade_time_ms=trade_time_ms,
        event_time_ms=trade_time_ms + 100,
    )


# ─── ParseAndValidate Tests ───────────────────────────────────────────────────

class TestParseAndValidate:

    def test_valid_message_produces_trade_record(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([_make_raw_message()])
            result = raw | ParseAndValidate()

            assert_that(
                result["valid"],
                equal_to([
                    TradeRecord(
                        trade_id=100001,
                        symbol="BTCUSDT",
                        price=Decimal("43000.00"),
                        quantity=Decimal("0.005"),
                        trade_time_ms=1700000030000,
                        event_time_ms=1700000030100,
                    )
                ]),
                label="valid_check",
            )
            assert_that(result[DEAD_LETTER_TAG], is_empty(), label="dead_letter_empty")

    def test_invalid_json_routes_to_dead_letter(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([b"not valid json {{{"])
            result = raw | ParseAndValidate()

            assert_that(result["valid"], is_empty(), label="valid_empty")
            assert_that(
                result[DEAD_LETTER_TAG],
                equal_to(["not valid json {{{"]),
                label="dead_letter_check",
            )

    def test_wrong_event_type_routes_to_dead_letter(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([_make_raw_message(event_type="aggTrade")])
            result = raw | ParseAndValidate()

            assert_that(result["valid"], is_empty(), label="valid_empty")
            assert_that(
                result[DEAD_LETTER_TAG],
                equal_to([_make_raw_message(event_type="aggTrade").decode()]),
                label="dead_letter_check",
            )

    def test_wrong_symbol_routes_to_dead_letter(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([_make_raw_message(symbol="ETHUSDT")])
            result = raw | ParseAndValidate()

            assert_that(result["valid"], is_empty(), label="valid_empty")
            assert_that(result[DEAD_LETTER_TAG], equal_to(
                [_make_raw_message(symbol="ETHUSDT").decode()]
            ), label="dl_check")

    def test_zero_price_routes_to_dead_letter(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([_make_raw_message(price="0")])
            result = raw | ParseAndValidate()
            assert_that(result["valid"], is_empty(), label="valid_empty")
            assert_that(result[DEAD_LETTER_TAG], equal_to(
                [_make_raw_message(price="0").decode()]
            ), label="dl_check")

    def test_negative_quantity_routes_to_dead_letter(self) -> None:
        with TestPipeline() as p:
            raw = p | beam.Create([_make_raw_message(quantity="-0.001")])
            result = raw | ParseAndValidate()
            assert_that(result["valid"], is_empty(), label="valid_empty")

    def test_missing_field_routes_to_dead_letter(self) -> None:
        incomplete = json.dumps({"e": "trade", "s": "BTCUSDT"}).encode()
        with TestPipeline() as p:
            raw = p | beam.Create([incomplete])
            result = raw | ParseAndValidate()
            assert_that(result["valid"], is_empty(), label="valid_empty")
            assert_that(result[DEAD_LETTER_TAG], equal_to(
                [incomplete.decode()]
            ), label="dl_check")

    def test_multiple_valid_messages(self) -> None:
        messages = [_make_raw_message(trade_id=i) for i in range(1, 4)]
        with TestPipeline() as p:
            raw = p | beam.Create(messages)
            result = raw | ParseAndValidate()
            assert_that(result[DEAD_LETTER_TAG], is_empty(), label="dl_empty")

    def test_mixed_valid_and_invalid(self) -> None:
        messages = [
            _make_raw_message(trade_id=1),
            b"bad json",
            _make_raw_message(trade_id=2),
        ]
        with TestPipeline() as p:
            raw = p | beam.Create(messages)
            result = raw | ParseAndValidate()
            assert_that(result[DEAD_LETTER_TAG], equal_to(["bad json"]), label="dl")


# ─── ExtractKey Tests ─────────────────────────────────────────────────────────

class TestExtractKey:

    def test_extracts_symbol_as_key(self) -> None:
        record = _make_trade_record()
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([record])
                | beam.ParDo(ExtractKey())
            )
            assert_that(result, equal_to([("BTCUSDT", record)]))

    def test_multiple_records_keyed_correctly(self) -> None:
        records = [_make_trade_record(trade_id=i) for i in range(1, 4)]
        expected = [("BTCUSDT", r) for r in records]
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(records)
                | beam.ParDo(ExtractKey())
            )
            assert_that(result, equal_to(expected))


# ─── OHLCVCombineFn Tests ─────────────────────────────────────────────────────

class TestOHLCVCombineFn:
    """
    Tests OHLCVCombineFn directly (not through the pipeline).
    This gives precise control over accumulator state and is faster
    than spinning up a TestPipeline for each case.
    """

    def setup_method(self):
        self.fn = OHLCVCombineFn()

    def _accumulate(self, trades: list[TradeRecord]) -> OHLCVAccumulator:
        acc = self.fn.create_accumulator()
        for trade in trades:
            acc = self.fn.add_input(acc, trade)
        return self.fn.extract_output(acc)

    def test_single_trade_all_ohlc_equal(self) -> None:
        acc = self._accumulate([_make_trade_record(price="43000.00")])
        assert acc.open == Decimal("43000.00")
        assert acc.high == Decimal("43000.00")
        assert acc.low == Decimal("43000.00")
        assert acc.close == Decimal("43000.00")
        assert acc.trade_count == 1

    def test_high_is_maximum_price(self) -> None:
        trades = [
            _make_trade_record(trade_id=1, price="43000.00", trade_time_ms=1000),
            _make_trade_record(trade_id=2, price="45000.00", trade_time_ms=2000),
            _make_trade_record(trade_id=3, price="42000.00", trade_time_ms=3000),
        ]
        acc = self._accumulate(trades)
        assert acc.high == Decimal("45000.00")

    def test_low_is_minimum_price(self) -> None:
        trades = [
            _make_trade_record(trade_id=1, price="43000.00", trade_time_ms=1000),
            _make_trade_record(trade_id=2, price="45000.00", trade_time_ms=2000),
            _make_trade_record(trade_id=3, price="42000.00", trade_time_ms=3000),
        ]
        acc = self._accumulate(trades)
        assert acc.low == Decimal("42000.00")

    def test_open_is_earliest_trade_price(self) -> None:
        trades = [
            _make_trade_record(trade_id=2, price="44000.00", trade_time_ms=2000),
            _make_trade_record(trade_id=1, price="43000.00", trade_time_ms=1000),  # earliest
            _make_trade_record(trade_id=3, price="45000.00", trade_time_ms=3000),
        ]
        acc = self._accumulate(trades)
        assert acc.open == Decimal("43000.00")

    def test_close_is_latest_trade_price(self) -> None:
        trades = [
            _make_trade_record(trade_id=1, price="43000.00", trade_time_ms=1000),
            _make_trade_record(trade_id=3, price="45000.00", trade_time_ms=3000),  # latest
            _make_trade_record(trade_id=2, price="44000.00", trade_time_ms=2000),
        ]
        acc = self._accumulate(trades)
        assert acc.close == Decimal("45000.00")

    def test_volume_is_sum_of_price_times_quantity(self) -> None:
        trades = [
            _make_trade_record(trade_id=1, price="40000.00", quantity="0.01",  trade_time_ms=1000),
            _make_trade_record(trade_id=2, price="50000.00", quantity="0.02",  trade_time_ms=2000),
        ]
        acc = self._accumulate(trades)
        # 40000 × 0.01 + 50000 × 0.02 = 400 + 1000 = 1400
        assert acc.volume == Decimal("1400.00")

    def test_trade_count_correct(self) -> None:
        trades = [_make_trade_record(trade_id=i, trade_time_ms=i * 1000) for i in range(1, 6)]
        acc = self._accumulate(trades)
        assert acc.trade_count == 5

    def test_empty_accumulator_is_empty(self) -> None:
        acc = self.fn.create_accumulator()
        assert acc.is_empty is True

    def test_merge_two_accumulators(self) -> None:
        acc1 = self._accumulate([
            _make_trade_record(trade_id=1, price="43000.00", trade_time_ms=1000),
        ])
        acc2 = self._accumulate([
            _make_trade_record(trade_id=2, price="45000.00", trade_time_ms=2000),
        ])
        merged = self.fn.merge_accumulators([acc1, acc2])
        assert merged.open == Decimal("43000.00")   # earliest
        assert merged.close == Decimal("45000.00")  # latest
        assert merged.high == Decimal("45000.00")
        assert merged.low == Decimal("43000.00")
        assert merged.trade_count == 2

    def test_merge_with_empty_accumulator(self) -> None:
        acc = self._accumulate([_make_trade_record(price="43000.00")])
        empty = self.fn.create_accumulator()
        merged = self.fn.merge_accumulators([acc, empty])
        assert merged.trade_count == 1
        assert not merged.is_empty

    def test_merge_all_empty_accumulators(self) -> None:
        merged = self.fn.merge_accumulators([
            self.fn.create_accumulator(),
            self.fn.create_accumulator(),
        ])
        assert merged.is_empty is True


# ─── Helper Function Tests ────────────────────────────────────────────────────

class TestHelpers:

    def test_ms_to_iso_converts_correctly(self) -> None:
        # 1700000000000ms = 2023-11-14T22:13:20+00:00
        result = _ms_to_iso(1700000000000)
        assert "2023-11-14" in result
        assert "+00:00" in result or "Z" in result or "UTC" in result

    def test_ms_to_iso_returns_string(self) -> None:
        assert isinstance(_ms_to_iso(1700000000000), str)