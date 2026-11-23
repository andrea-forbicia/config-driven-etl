"""Structured logging: one line per event on stderr, JSON or pretty."""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal

LOGGER = logging.getLogger("config_driven_etl")

LogFormat = Literal["json", "pretty"]

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
            **_fields(record),
        }
        return json.dumps(payload, ensure_ascii=False, default=str)


class PrettyFormatter(logging.Formatter):
    """Render a record as one aligned text line for a terminal.

    The time is local, as on the reader's clock; JSON stays the format
    of record, in UTC, and the run_id ties the two together.
    """

    def format(self, record: logging.LogRecord) -> str:
        time = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        fields = dict(_fields(record))
        event = record.getMessage()
        render = _RENDERERS.get(event, _render_other)
        line = f"{time}  {render(event, fields)}".rstrip()
        # One event, one line, whatever the message holds.
        return " | ".join(line.splitlines())


def configure_logging(log_format: LogFormat = "json") -> None:
    """Send this package's events to stderr, one line per event.

    Safe to call more than once: earlier handlers are replaced, so each
    CLI invocation writes to the stderr that is current at that time.
    """
    handler = logging.StreamHandler()
    formatter = PrettyFormatter() if log_format == "pretty" else None
    handler.setFormatter(formatter or JsonFormatter())
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


def _fields(record: logging.LogRecord) -> Mapping[str, Any]:
    return getattr(record, _FIELDS, {})


def _render_stage(event: str, fields: dict[str, Any]) -> str:
    parts = [_column(fields.get("stage")) + str(fields.get("dataset"))]
    # Extract only counts what it read.
    if fields.get("rows_in") is not None:
        parts.append(f"in {fields['rows_in']}")
    parts.append(f"out {fields.get('rows_out')}")
    # Only the transform rejects rows, and only it logs the reasons.
    if "rejections" in fields:
        parts.append(f"rejected {fields.get('rows_rejected', 0)}")
    if fields.get("rejections"):
        reasons = ", ".join(
            f"{reason} {rows}" for reason, rows in fields["rejections"].items()
        )
        parts.append(f"({reasons})")
    if fields.get("dropped_columns"):
        names = ", ".join(f"'{name}'" for name in fields["dropped_columns"])
        parts.append(f"dropped {names}")
    if fields.get("schema_changes"):
        changes = "; ".join(fields["schema_changes"])
        parts.append(f"schema changed ({changes})")
    if "rejected_rows_loaded" in fields:
        parts.append(
            f"rejected loaded {fields['rejected_rows_loaded']} "
            f"into {fields.get('rejected_table')}"
        )
    parts.append(_seconds(fields.get("duration_s")))
    if event == "stage_failed":
        parts.append("FAILED")
    return "  ".join(parts)


def _render_run_failed(event: str, fields: dict[str, Any]) -> str:
    return (
        f"{_column('error')}{fields.get('category')}, exit "
        f"{fields.get('exit_code')}: {fields.get('error')}"
    )


def _render_audit_failed(event: str, fields: dict[str, Any]) -> str:
    return f"{_column('error')}audit: {fields.get('error')}"


def _render_run_finished(event: str, fields: dict[str, Any]) -> str:
    parts = [
        # No dataset when the settings or the dataset file failed first.
        _column(fields.get("command")) + (fields.get("dataset") or "-"),
        str(fields.get("outcome")),
        f"exit {fields.get('exit_code')}",
    ]
    if fields.get("category"):
        where = fields.get("failed_stage")
        parts.append(
            f"{fields['category']} in {where}" if where else fields["category"]
        )
    parts += [
        _seconds(fields.get("duration_s")),
        f"run {fields.get('run_id')}",
    ]
    return "  ".join(parts)


def _render_other(event: str, fields: dict[str, Any]) -> str:
    pairs = " ".join(f"{key}={value}" for key, value in fields.items())
    return f"{_column(event)}{pairs}"


def _column(value: object) -> str:
    # Wide enough for every stage and command name, and always spaced.
    return f"{value:<9}  "


def _seconds(value: object) -> str:
    return f"{value:.1f}s" if isinstance(value, int | float) else ""


_RENDERERS = {
    "stage_completed": _render_stage,
    "stage_failed": _render_stage,
    "run_failed": _render_run_failed,
    "audit_failed": _render_audit_failed,
    "run_finished": _render_run_finished,
}
