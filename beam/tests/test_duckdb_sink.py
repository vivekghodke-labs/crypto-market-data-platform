import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal

from src.duckdb_sink import DuckDBWriter, DeadLetterDuckDBWriter, RawTradesDuckDBWriter
from src.schema import TradeRecord

class TestDuckDBSinks:

    @patch("src.duckdb_sink.duckdb.connect")
    def test_ohlcv_duckdb_writer(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = DuckDBWriter("dummy_path.db", "silver_table", "SELECT 1")
        writer.setup()
        
        batch = [{
            "window_start": "2023-11-14T00:00:00",
            "window_end": "2023-11-14T00:01:00",
            "symbol": "BTCUSDT",
            "open": "40000",
            "high": "41000",
            "low": "39000",
            "close": "40500",
            "volume": "1.5",
            "trade_count": 10,
            "ingested_at": "2023-11-14T00:01:05"
        }]
        
        writer.process(batch)
        mock_conn.executemany.assert_called_once()
        
        writer.teardown()
        mock_conn.close.assert_called_once()

    @patch("src.duckdb_sink.duckdb.connect")
    def test_dead_letter_duckdb_writer(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = DeadLetterDuckDBWriter("dummy_path.db", "SELECT 1")
        writer.setup()
        
        writer.process("invalid json message {")
        
        mock_conn.execute.assert_called_once()
        # Verify the error category was written
        call_args = mock_conn.execute.call_args[0]
        assert "beam_parse_or_validation_failure" in call_args[1]
        
        writer.teardown()
        mock_conn.close.assert_called_once()

    @patch("src.duckdb_sink.duckdb.connect")
    def test_raw_trades_duckdb_writer(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = RawTradesDuckDBWriter("dummy_path.db")
        writer.setup()
        
        trade = TradeRecord(
            trade_id=123,
            symbol="BTCUSDT",
            price=Decimal("43000.00"),
            quantity=Decimal("0.05"),
            trade_time_ms=1700000000000,
            event_time_ms=1700000000100
        )
        
        writer.process(trade)
        
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        # Make sure decimal values are cast to strings
        assert "43000.00" in call_args[1]
        
        writer.teardown()
        mock_conn.close.assert_called_once()