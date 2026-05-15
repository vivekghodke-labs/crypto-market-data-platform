import json
import logging
from src.logger import get_logger, JsonFormatter

class TestLogger:

    def test_logger_singleton_pattern(self):
        logger1 = get_logger("test_singleton_logger")
        logger2 = get_logger("test_singleton_logger")
        
        assert logger1 is logger2
        # Ensure handlers aren't duplicated on multiple calls
        assert len(logger1.handlers) == 1

    def test_json_formatter_structure(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.module",
            level=logging.WARNING,
            pathname="test_path.py",
            lineno=42,
            msg="This is a structured log test",
            args=(),
            exc_info=None
        )
        
        formatted_output = formatter.format(record)
        parsed_json = json.loads(formatted_output)
        
        assert parsed_json["severity"] == "WARNING"
        assert parsed_json["message"] == "This is a structured log test"
        assert parsed_json["logger"] == "test.module"
        assert parsed_json["lineNo"] == 42
        assert "timestamp" in parsed_json

    def test_json_formatter_with_extra_fields(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Extra fields", args=(), exc_info=None
        )
        # Simulate passing extra kwargs to the logger
        record.__dict__["custom_trade_id"] = 9999
        
        parsed_json = json.loads(formatter.format(record))
        assert parsed_json["custom_trade_id"] == 9999