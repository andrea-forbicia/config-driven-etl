"""Structured JSON logging: one line per event on stderr."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

LOGGER = logging.getLogger("config_driven_etl")

# Attribute of a LogRecord that holds the event's fields.
_FIELDS = "fields"


class JsonFormatter(logging.Formatter):
    """Render a record as one JSON object: time, level, event, fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            **getattr(record, _FIELDS, {}),
        }
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging() -> None:
    """Send this package's events to stderr as JSON lines.

    Safe to call more than once: earlier handlers are replaced, so each
    CLI invocation writes to the stderr that is current at that time.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    LOGGER.handlers[:] = [handler]
    LOGGER.setLevel(logging.INFO)
    # The root logger may have its own handlers; one line per event.
    LOGGER.propagate = False


def log_event(event: str, **fields: Any) -> None:
    """Log an informational event with structured fields.

    Callers pass only run facts such as names, counts and durations;
    settings and credentials must never be logged.
    """
    LOGGER.info(event, extra={_FIELDS: fields})


def log_error(event: str, **fields: Any) -> None:
    """Log an error event with structured fields, as log_event does."""
    LOGGER.error(event, extra={_FIELDS: fields})
