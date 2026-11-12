import io
import json
import logging
import unittest
from datetime import datetime, timedelta
from unittest import mock

from config_driven_etl.logging import (
    LOGGER,
    configure_logging,
    log_error,
    log_event,
)


class LoggingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.stream = io.StringIO()
        patcher = mock.patch("sys.stderr", self.stream)
        patcher.start()
        self.addCleanup(patcher.stop)
        handlers = LOGGER.handlers[:]
        self.addCleanup(setattr, LOGGER, "handlers", handlers)
        configure_logging()

    def lines(self) -> list[dict[str, object]]:
        return [
            json.loads(line) for line in self.stream.getvalue().splitlines()
        ]

    def test_event_is_one_json_line_with_its_fields(self) -> None:
        log_event("stage_completed", stage="load", rows_out=4, rows_in=None)

        (line,) = self.lines()
        self.assertEqual(
            {key: value for key, value in line.items() if key != "time"},
            {
                "level": "info",
                "event": "stage_completed",
                "stage": "load",
                "rows_out": 4,
                "rows_in": None,
            },
        )

    def test_time_is_utc_iso_8601(self) -> None:
        log_event("x")

        (line,) = self.lines()
        assert isinstance(line["time"], str)
        self.assertEqual(
            datetime.fromisoformat(line["time"]).utcoffset(), timedelta(0)
        )

    def test_error_level(self) -> None:
        log_error("run_failed", error="boom")

        (line,) = self.lines()
        self.assertEqual(line["level"], "error")
        self.assertEqual(line["error"], "boom")

    def test_nested_and_unicode_values(self) -> None:
        log_event("x", rejections={"città: null": 2})

        self.assertIn("città", self.stream.getvalue())
        (line,) = self.lines()
        self.assertEqual(line["rejections"], {"città: null": 2})

    def test_configuring_twice_logs_once(self) -> None:
        configure_logging()

        log_event("x")

        self.assertEqual(len(self.lines()), 1)

    def test_does_not_propagate_to_the_root_logger(self) -> None:
        root = io.StringIO()
        handler = logging.StreamHandler(root)
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)

        log_event("x")

        self.assertEqual(root.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
