"""
All Apache Beam PTransforms for the OHLCV aggregation pipeline.

Transform chain:
  ParseAndValidate   → bytes → TradeRecord (valid) or str (invalid)
  ExtractKey         → TradeRecord → (symbol, TradeRecord) keyed for CombinePerKey
  OHLCVCombineFn     → CombineFn accumulating OHLCV state per (symbol, window)
  FormatOHLCV        → OHLCVAccumulator + window → OHLCVRecord dict for BigQuery

Design: Each transform is a standalone, testable unit. The pipeline.py
composes them — transforms never import from pipeline.py (one-way dependency).

CombineFn vs GroupByKey:
  We use CombineFn (not GroupByKey + map) because:
  1. CombineFn supports partial aggregation — Beam can merge accumulators
     on workers before the shuffle, reducing network I/O.
  2. CombineFn is the correct Beam primitive for associative aggregations
     like min/max/sum/first/last — which is exactly what OHLCV is.
"""

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.core import CombineFn

from .schema import TradeRecord, OHLCVAccumulator, OHLCVRecord

logger = logging.getLogger(__name__)

# PCollection tag for the dead-letter output from ParseAndValidate
DEAD_LETTER_TAG = "dead_letter"


# ─── ParseAndValidate ─────────────────────────────────────────────────────────

class ParseAndValidate(beam.PTransform):
    """
    Parses raw Pub/Sub message bytes into TradeRecord elements.

    Outputs:
      Main output  → valid TradeRecord elements
      dead_letter  → raw JSON strings that failed parsing or validation

    Tagged outputs allow the pipeline to route failures to a separate
    BigQuery dead-letter log without dropping them silently.

    Validation rules (mirrors services/ingestor/src/schema.py):
      - Must be valid JSON
      - event_type must equal "trade"
      - symbol must equal "BTCUSDT"
      - price and quantity must be parseable Decimals > 0
      - trade_id and trade_time_ms must be integers > 0
    """

    def expand(self, pcoll: beam.PCollection) -> pvalue.DoOutputsTuple:
        return pcoll | "Parse" >> beam.ParDo(
            _ParseAndValidateDoFn()
        ).with_outputs(DEAD_LETTER_TAG, main="valid")


class _ParseAndValidateDoFn(beam.DoFn):
    """Internal DoFn for ParseAndValidate. Not used directly."""

    def process(self, element: bytes, *args, **kwargs):
        raw: str = ""
        try:
            raw = element.decode("utf-8") if isinstance(element, bytes) else element
            payload: dict = json.loads(raw)

            # Field presence check
            required_fields = {"e", "E", "s", "t", "p", "q", "T", "m"}
            missing = required_fields - payload.keys()
            if missing:
                raise ValueError(f"Missing required fields: {missing}")

            # Type and value validation
            if payload["e"] != "trade":
                raise ValueError(f"event_type must be 'trade', got '{payload['e']}'")
            if payload["s"] != "BTCUSDT":
                raise ValueError(f"symbol must be 'BTCUSDT', got '{payload['s']}'")

            price = Decimal(str(payload["p"]))
            if price <= 0:
                raise ValueError(f"price must be > 0, got {price}")

            quantity = Decimal(str(payload["q"]))
            if quantity <= 0:
                raise ValueError(f"quantity must be > 0, got {quantity}")

            trade_id = int(payload["t"])
            if trade_id <= 0:
                raise ValueError(f"trade_id must be > 0, got {trade_id}")

            trade_time_ms = int(payload["T"])
            if trade_time_ms <= 0:
                raise ValueError(f"trade_time_ms must be > 0, got {trade_time_ms}")

            yield TradeRecord(
                trade_id=trade_id,
                symbol=str(payload["s"]),
                price=price,
                quantity=quantity,
                trade_time_ms=trade_time_ms,
                event_time_ms=int(payload["E"]),
            )

        except (json.JSONDecodeError, ValueError, KeyError, InvalidOperation) as exc:
            logger.warning("Message failed validation: %s | raw=%s", exc, raw[:200])
            yield pvalue.TaggedOutput(DEAD_LETTER_TAG, raw)

        except Exception as exc:
            logger.error("Unexpected error parsing message: %s", exc, exc_info=True)
            yield pvalue.TaggedOutput(DEAD_LETTER_TAG, raw)


# ─── AssignEventTimestamp ──────────────────────────────────────────────────────

class AssignEventTimestamp(beam.DoFn):
    """
    Assigns the Beam event timestamp from trade_time_ms.

    This is critical for correct windowing. Without this, Beam uses
    processing time (wall clock) — which means trades from the same
    1-minute window could land in different Beam windows depending on
    when they arrive at the pipeline, not when they occurred.

    By assigning event time from trade_time_ms, Beam windows by when
    the trade actually happened on the exchange.
    """

    def process(self, element: TradeRecord, *args, **kwargs):
        event_timestamp = element.trade_time_ms / 1000.0  # ms → seconds (Unix)
        yield beam.window.TimestampedValue(element, event_timestamp)


# ─── ExtractKey ───────────────────────────────────────────────────────────────

class ExtractKey(beam.DoFn):
    """
    Converts TradeRecord → (symbol, TradeRecord) KV pair.
    Required by CombinePerKey — each unique symbol gets its own OHLCV candle.
    At portfolio scale this is always ("BTCUSDT", record), but the pattern
    supports multi-asset expansion in future sprints without code changes.
    """

    def process(self, element: TradeRecord, *args, **kwargs):
        yield (element.symbol, element)


# ─── OHLCVCombineFn ───────────────────────────────────────────────────────────

class OHLCVCombineFn(CombineFn):
    """
    Apache Beam CombineFn that aggregates TradeRecords into OHLCV candles.

    CombineFn lifecycle (called by Beam runtime):
      create_accumulator() → initialise empty state
      add_input()          → merge one TradeRecord into accumulator
      merge_accumulators() → combine partial accumulators (distributed merge)
      extract_output()     → finalise accumulator → OHLCVAccumulator

    OHLCV logic:
      O (open)   → price of the trade with the smallest trade_time_ms
      H (high)   → maximum price across all trades in the window
      L (low)    → minimum price across all trades in the window
      C (close)  → price of the trade with the largest trade_time_ms
      V (volume) → Σ (price × quantity) for all trades in the window
    """

    def create_accumulator(self) -> OHLCVAccumulator:
        return OHLCVAccumulator()

    def add_input(
        self, accumulator: OHLCVAccumulator, element: TradeRecord
    ) -> OHLCVAccumulator:
        if accumulator.is_empty:
            # First trade — initialise all fields
            accumulator.open = element.price
            accumulator.high = element.price
            accumulator.low = element.price
            accumulator.close = element.price
            accumulator.volume = element.price * element.quantity
            accumulator.trade_count = 1
            accumulator.first_trade_time_ms = element.trade_time_ms
            accumulator.last_trade_time_ms = element.trade_time_ms
            accumulator.is_empty = False
        else:
            # Subsequent trades — update running state
            if element.price > accumulator.high:
                accumulator.high = element.price
            if element.price < accumulator.low:
                accumulator.low = element.price

            # Open: price of the earliest trade in the window
            if element.trade_time_ms < accumulator.first_trade_time_ms:
                accumulator.open = element.price
                accumulator.first_trade_time_ms = element.trade_time_ms

            # Close: price of the latest trade in the window
            if element.trade_time_ms > accumulator.last_trade_time_ms:
                accumulator.close = element.price
                accumulator.last_trade_time_ms = element.trade_time_ms

            accumulator.volume += element.price * element.quantity
            accumulator.trade_count += 1

        return accumulator

    def merge_accumulators(
        self, accumulators: Iterable[OHLCVAccumulator]
    ) -> OHLCVAccumulator:
        """
        Merges partial accumulators from different workers.
        This is called during the distributed shuffle phase.
        """
        merged = OHLCVAccumulator()
        for acc in accumulators:
            if acc.is_empty:
                continue
            if merged.is_empty:
                # Bootstrap merged from first non-empty accumulator
                merged.open = acc.open
                merged.high = acc.high
                merged.low = acc.low
                merged.close = acc.close
                merged.volume = acc.volume
                merged.trade_count = acc.trade_count
                merged.first_trade_time_ms = acc.first_trade_time_ms
                merged.last_trade_time_ms = acc.last_trade_time_ms
                merged.is_empty = False
            else:
                if acc.high > merged.high:
                    merged.high = acc.high
                if acc.low < merged.low:
                    merged.low = acc.low

                if acc.first_trade_time_ms < merged.first_trade_time_ms:
                    merged.open = acc.open
                    merged.first_trade_time_ms = acc.first_trade_time_ms

                if acc.last_trade_time_ms > merged.last_trade_time_ms:
                    merged.close = acc.close
                    merged.last_trade_time_ms = acc.last_trade_time_ms

                merged.volume += acc.volume
                merged.trade_count += acc.trade_count

        return merged

    def extract_output(self, accumulator: OHLCVAccumulator) -> OHLCVAccumulator:
        return accumulator


# ─── FormatOHLCV ──────────────────────────────────────────────────────────────

class FormatOHLCV(beam.DoFn):
    """
    Converts (symbol, OHLCVAccumulator) + window metadata → OHLCVRecord dict.

    The dict format is required by WriteToBigQuery. Decimal values are
    converted to strings — BigQuery accepts string input for NUMERIC fields
    and preserves full precision without float conversion.

    Window boundaries are injected via Beam's window.IntervalWindow context.
    """

    def process(
        self,
        element: tuple,
        window=beam.DoFn.WindowParam,
        *args,
        **kwargs,
    ):
        symbol, accumulator = element

        if accumulator.is_empty:
            # Empty window — no trades arrived. Skip. Do not write null rows.
            logger.debug("Empty window for symbol=%s — skipping", symbol)
            return

        window_start = _ms_to_iso(int(window.start * 1000))
        window_end = _ms_to_iso(int(window.end * 1000))
        ingested_at = datetime.now(timezone.utc).isoformat()

        yield {
            "window_start": window_start,
            "window_end": window_end,
            "symbol": symbol,
            "open": str(accumulator.open),
            "high": str(accumulator.high),
            "low": str(accumulator.low),
            "close": str(accumulator.close),
            "volume": str(accumulator.volume),
            "trade_count": accumulator.trade_count,
            "ingested_at": ingested_at,
        }


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ms_to_iso(timestamp_ms: int) -> str:
    """Converts a Unix millisecond timestamp to ISO 8601 UTC string."""
    return datetime.fromtimestamp(
        timestamp_ms / 1000.0, tz=timezone.utc
    ).isoformat()