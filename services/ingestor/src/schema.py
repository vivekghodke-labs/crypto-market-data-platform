"""
Pydantic v2 schema definitions for the Binance WebSocket trade stream.

We consume wss://stream.binance.com:9443/ws/btcusdt@trade.
Each message is a raw trade event (not aggregated). This module is the
schema contract boundary — malformed messages never propagate downstream.

Binance trade event payload reference:
  https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
"""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class BinanceTradeEvent(BaseModel):
    """
    Represents a single raw trade event from the Binance btcusdt@trade stream.

    Field mappings (Binance abbreviations → semantic names):
      e  → event_type
      E  → event_time_ms
      s  → symbol
      t  → trade_id
      p  → price
      q  → quantity
      T  → trade_time_ms
      m  → is_market_maker
    """

    event_type: Literal["trade"] = Field(alias="e")
    event_time_ms: int = Field(alias="E", gt=0)
    symbol: Literal["BTCUSDT"] = Field(alias="s")
    trade_id: int = Field(alias="t", gt=0)
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    trade_time_ms: int = Field(alias="T", gt=0)
    is_market_maker: bool = Field(alias="m")

    model_config = {
        # Allow both alias ("p") and field name ("price") on input
        "populate_by_name": True,
        # Ensure Decimal fields are not silently coerced from float imprecision
        "arbitrary_types_allowed": True,
    }

    @field_validator("price", mode="before")
    @classmethod
    def price_must_be_positive(cls, v: str) -> Decimal:
        """Price arrives as a string from Binance. Parse and validate > 0."""
        value = Decimal(str(v))
        if value <= 0:
            raise ValueError(f"price must be > 0, got {value}")
        return value

    @field_validator("quantity", mode="before")
    @classmethod
    def quantity_must_be_positive(cls, v: str) -> Decimal:
        """Quantity arrives as a string from Binance. Parse and validate > 0."""
        value = Decimal(str(v))
        if value <= 0:
            raise ValueError(f"quantity must be > 0, got {value}")
        return value

    @model_validator(mode="after")
    def trade_time_must_not_precede_event_time(self) -> "BinanceTradeEvent":
        """
        Trade execution time (T) should not be after event publish time (E)
        by more than 5 seconds — guards against obviously corrupt timestamps.
        """
        drift_ms = self.event_time_ms - self.trade_time_ms
        if drift_ms < -5000:
            raise ValueError(
                f"trade_time_ms ({self.trade_time_ms}) is more than 5s "
                f"after event_time_ms ({self.event_time_ms}). "
                f"Possible corrupt timestamp."
            )
        return self


class DeadLetterEnvelope(BaseModel):
    """
    Wraps a rejected raw message for publication to the dead-letter topic.
    Preserves the original payload alongside the rejection reason
    for downstream triage and reprocessing.
    """

    raw_message: str = Field(description="Original raw JSON string from Binance")
    error: str = Field(description="Validation error detail")
    source: str = Field(
        default="binance-ws-btcusdt",
        description="Source identifier for multi-asset expansion",
    )