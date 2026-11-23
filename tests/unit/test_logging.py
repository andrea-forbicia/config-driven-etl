import io
import json
import logging
import unittest
from datetime import datetime, timedelta
from unittest import mock

from config_driven_etl.logging import (
    LOGGER,
    PrettyFormatter,
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


def pretty_line(event: str, created: float = 0.0, **fields: object) -> str:
    """One record rendered by PrettyFormatter, without its time."""
    record = logging.LogRecord(
        "config_driven_etl", logging.INFO, __file__, 0, event, None, None
    )
    record.created = created
    record.fields = fields
    line = PrettyFormatter().format(record)
    time, rest = line.split("  ", 1)
    assert len(time) == len("00:00:00"), line
    return rest


class PrettyFormatterTest(unittest.TestCase):
    def test_time_is_local_hours_minutes_seconds(self) -> None:
        created = 1_795_000_000.5
        record = logging.LogRecord(
            "config_driven_etl", logging.INFO, __file__, 0, "x", None, None
        )
        record.created = created

        line = PrettyFormatter().format(record)

        expected = datetime.fromtimestamp(created).strftime("%H:%M:%S")
        self.assertTrue(line.startswith(f"{expected}  "), line)

    def test_extract_has_no_rows_in_or_rejected(self) -> None:
        line = pretty_line(
            "stage_completed",
            stage="extract",
            dataset="d",
            rows_in=None,
            rows_out=3,
            rows_rejected=0,
            duration_s=2.745,
        )

        self.assertEqual(line, "extract    d  out 3  2.7s")

    def test_transform_lists_its_rejections(self) -> None:
        line = pretty_line(
            "stage_completed",
            stage="transform",
            dataset="d",
            rows_in=10,
            rows_out=7,
            rows_rejected=3,
            rejections={"id: null": 2, "id: duplicate": 1},
            rejected_file="processed/d/d.rejected.parquet",
            duration_s=0.04,
        )

        self.assertEqual(
            line,
            "transform  d  in 10  out 7  rejected 3  "
            "(id: null 2, id: duplicate 1)  0.0s",
        )

    def test_transform_lists_its_dropped_columns(self) -> None:
        line = pretty_line(
            "stage_completed",
            stage="transform",
            dataset="d",
            rows_in=10,
            rows_out=10,
            rows_rejected=0,
            rejections={},
            dropped_columns=["Internal Note", "Extra"],
            duration_s=0.04,
        )

        self.assertEqual(
            line,
            "transform  d  in 10  out 10  rejected 0  "
            "dropped 'Internal Note', 'Extra'  0.0s",
        )

    def test_no_dropped_columns_are_not_mentioned(self) -> None:
        line = pretty_line(
            "stage_completed",
            stage="transform",
            dataset="d",
            rows_in=10,
            rows_out=10,
            rows_rejected=0,
            rejections={},
            dropped_columns=[],
            duration_s=0.04,
        )

        self.assertNotIn("dropped", line)

    def test_load_names_the_rejected_table(self) -> None:
        line = pretty_line(
            "stage_completed",
            stage="load",
            dataset="d",
            rows_in=7,
            rows_out=7,
            rows_rejected=0,
            rejected_table="d_rejected",
            rejected_rows_loaded=3,
            duration_s=9.1,
        )

        self.assertEqual(
            line,
            "load       d  in 7  out 7  rejected loaded 3 into "
            "d_rejected  9.1s",
        )

    def test_failed_stage_is_marked(self) -> None:
        line = pretty_line(
            "stage_failed",
            stage="transform",
            dataset="d",
            rows_in=4,
            rows_out=0,
            rows_rejected=4,
            rejections={"id: null": 4},
            duration_s=0.1,
        )

        self.assertTrue(line.endswith("0.1s  FAILED"), line)

    def test_errors_fit_on_one_line(self) -> None:
        line = pretty_line(
            "run_failed",
            category="load",
            exit_code=6,
            error="dataset.t: BigQuery error: 400\ndataset.t:   cause",
        )

        self.assertEqual(
            line,
            "error      load, exit 6: dataset.t: BigQuery error: 400 | "
            "dataset.t:   cause",
        )

    def test_audit_failure(self) -> None:
        line = pretty_line("audit_failed", error="denied")

        self.assertEqual(line, "error      audit: denied")

    def test_run_finished(self) -> None:
        common = {"command": "run", "dataset": "d", "run_id": "ab12"}
        cases = [
            (
                {"outcome": "success", "exit_code": 0, "duration_s": 12.36},
                "run        d  success  exit 0  12.4s  run ab12",
            ),
            (
                {
                    "outcome": "failed",
                    "exit_code": 7,
                    "category": "quality_gate",
                    "failed_stage": "transform",
                    "duration_s": 0.4,
                },
                "run        d  failed  exit 7  quality_gate in transform  "
                "0.4s  run ab12",
            ),
            (
                {
                    "outcome": "failed",
                    "exit_code": 3,
                    "category": "config",
                    "failed_stage": None,
                    "duration_s": 0.0,
                },
                "run        d  failed  exit 3  config  0.0s  run ab12",
            ),
        ]
        for fields, expected in cases:
            with self.subTest(exit_code=fields["exit_code"]):
                self.assertEqual(
                    pretty_line("run_finished", **common, **fields), expected
                )

    def test_run_finished_without_a_dataset(self) -> None:
        # The dataset file could not be read, or settings failed first.
        line = pretty_line(
            "run_finished",
            command="extract",
            dataset=None,
            run_id="ab12",
            outcome="failed",
            exit_code=3,
            category="config",
            failed_stage=None,
            duration_s=0.0,
        )

        self.assertEqual(
            line, "extract    -  failed  exit 3  config  0.0s  run ab12"
        )

    def test_unknown_events_list_their_fields(self) -> None:
        line = pretty_line("something_new", a=1, b="two")

        self.assertEqual(line, "something_new  a=1 b=two")

    def test_configure_pretty_writes_text_lines(self) -> None:
        stream = io.StringIO()
        handlers = LOGGER.handlers[:]
        self.addCleanup(setattr, LOGGER, "handlers", handlers)
        with mock.patch("sys.stderr", stream):
            configure_logging("pretty")
            log_event("audit_failed", error="x")

        self.assertTrue(stream.getvalue().endswith("  error      audit: x\n"))


if __name__ == "__main__":
    unittest.main()
