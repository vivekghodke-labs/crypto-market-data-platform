"""
Structured JSON logger for the ingestor service.
Uses Python's built-in logging module with a custom JSON formatter.
Every log record is emitted as a single-line JSON object — compatible
with GCP Cloud Logging's structured log format.
"""
import os
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    GCP Cloud Logging will parse these and index all fields automatically.
    """

    # Map Python log level names to GCP severity labels
    _SEVERITY_MAP: dict[str, str] = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            # GCP Cloud Logging reserved fields
            "severity": self._SEVERITY_MAP.get(record.levelname, "DEFAULT"),
            "message": record.getMessage(),
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            # Source context
            "logger": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }

        # Attach exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Attach any extra fields passed via `extra=` kwarg
        for key, value in record.__dict__.items():
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "id", "levelname", "levelno",
                "lineno", "module", "msecs", "message", "msg", "name",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "thread", "threadName", "taskName",
            }:
                log_entry[key] = value

        return json.dumps(log_entry, default=str)


def get_logger(name: str, level: str = None) -> logging.Logger:
    """
    Returns a named logger configured with the JSON formatter.
    Safe to call multiple times — idempotent.

    Args:
        name:  Logger name, typically __name__ of the calling module.
        level: Log level string. Defaults to LOG_LEVEL env var or INFO.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    # Dynamically grab the log level from the environment
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    return logger