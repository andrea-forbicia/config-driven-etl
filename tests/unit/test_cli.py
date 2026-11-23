import json
import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from google.api_core.exceptions import Forbidden, NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery
from typer.testing import CliRunner, Result

from config_driven_etl import __version__
from config_driven_etl.bq_schema import AUDIT_TABLE_SPEC
from config_driven_etl.cli import ExitCode, app
from fixture_outcomes import FIXTURES, OUTCOMES, fixture_path

# Rich forces colors on CI runners, splitting option names with ANSI codes.
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_version_prints_package_version(self) -> None:
        result = self.runner.invoke(app, ["--version"])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), __version__)

    def test_help_exits_successfully(self) -> None:
        result = self.runner.invoke(app, ["--help"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--version", ANSI_ESCAPE.sub("", result.output))


DATASETS_DIR = Path(__file__).parents[2] / "datasets"
INVALID_FIXTURES_DIR = FIXTURES / "invalid"

# Every invalid fixture with the problem `validate` must report for it.
INVALID_FIXTURES = {
    "unknown_type.yaml": "columns[0].type: Input should be",
    "missing_column.yaml": (
        "rules.deduplicate_on: 'customer_id' is not a column"
    ),
    "duplicate_target_name.yaml": (
        "columns: name 'id' is used more than once"
    ),
    "bad_partition_column.yaml": (
        "target.partition_by: 'id' must be a date or datetime column"
    ),
    "invalid_yaml.yaml": "line 4: expected ',' or ']'",
    "rejected_table_same_as_table.yaml": (
        "target.rejected_table: 'loans' is also target.table"
    ),
    "max_rejected_ratio_as_percent.yaml": (
        "rules.max_rejected_ratio: Input should be less than or equal to 1"
    ),
    "min_rows_zero.yaml": (
        "rules.min_rows: Input should be greater than or equal to 1"
    ),
}

VALID_YAML = """\
name: sample
source:
  path: data/raw/sample.csv
columns:
  - source: Id
    name: id
    type: int
target:
  table: sample
"""


class ValidateCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def write(self, name: str, text: str) -> Path:
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_valid_file(self) -> None:
        path = self.write("sample.yaml", VALID_YAML)

        result = self.runner.invoke(app, ["validate", str(path)])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            result.stdout.strip(), f"ok: {path} (sample, 1 columns)"
        )
        self.assertEqual(result.stderr, "")

    def test_invalid_file_exits_with_three(self) -> None:
        path = self.write(
            "bad.yaml", VALID_YAML.replace("type: int", "type: decimal")
        )

        result = self.runner.invoke(app, ["validate", str(path)])

        self.assertEqual(result.exit_code, 3)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith(f"{path}: columns[0].type: "))

    def test_missing_file(self) -> None:
        path = self.directory / "missing.yaml"

        result = self.runner.invoke(app, ["validate", str(path)])

        self.assertEqual(result.exit_code, 3)
        self.assertIn(f"{path}: cannot read file", result.stderr)

    def test_checks_every_file_before_failing(self) -> None:
        bad = self.write("a_bad.yaml", "name: Bad\n")
        good = self.write("b_good.yaml", VALID_YAML)

        result = self.runner.invoke(app, ["validate", str(bad), str(good)])

        self.assertEqual(result.exit_code, 3)
        self.assertIn(f"ok: {good}", result.stdout)
        self.assertIn(f"{bad}: name: ", result.stderr)

    def test_directory_validates_yaml_files_only(self) -> None:
        self.write("one.yaml", VALID_YAML)
        self.write("two.yaml", VALID_YAML)
        self.write("notes.txt", "not a config")
        self.write("other.yml", "not: [valid")

        result = self.runner.invoke(app, ["validate", str(self.directory)])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                f"ok: {self.directory / 'one.yaml'} (sample, 1 columns)",
                f"ok: {self.directory / 'two.yaml'} (sample, 1 columns)",
            ],
        )

    def test_directory_without_yaml_files(self) -> None:
        result = self.runner.invoke(app, ["validate", str(self.directory)])

        self.assertEqual(result.exit_code, 3)
        self.assertEqual(
            result.stderr.strip(),
            f"{self.directory}: no *.yaml dataset files found",
        )

    def test_no_arguments_is_a_usage_error(self) -> None:
        result = self.runner.invoke(app, ["validate"])

        self.assertEqual(result.exit_code, 2)

    def test_every_invalid_fixture_has_an_expectation(self) -> None:
        self.assertEqual(
            {path.name for path in INVALID_FIXTURES_DIR.glob("*.yaml")},
            set(INVALID_FIXTURES),
        )

    def test_invalid_fixtures_fail_with_clear_message(self) -> None:
        for name, problem in INVALID_FIXTURES.items():
            with self.subTest(fixture=name):
                path = INVALID_FIXTURES_DIR / name

                result = self.runner.invoke(app, ["validate", str(path)])

                self.assertEqual(result.exit_code, 3)
                self.assertEqual(result.stdout, "")
                # Exactly one line: no misleading follow-up errors.
                (line,) = result.stderr.splitlines()
                self.assertTrue(line.startswith(f"{path}: {problem}"))

    def test_every_fixture_dataset_has_an_outcome(self) -> None:
        datasets = {
            path.name
            for path in FIXTURES.iterdir()
            if path.is_dir() and path.name != "invalid"
        }

        self.assertEqual(datasets, set(OUTCOMES))

    def test_fixture_datasets_are_valid(self) -> None:
        paths = [str(FIXTURES / name) for name in OUTCOMES]

        result = self.runner.invoke(app, ["validate", *paths])

        self.assertEqual(result.exit_code, 0, result.stderr)
        self.assertEqual(len(result.stdout.splitlines()), 2)

    def test_committed_datasets_are_valid(self) -> None:
        result = self.runner.invoke(app, ["validate", str(DATASETS_DIR)])

        self.assertEqual(result.exit_code, 0, result.stderr)
        self.assertEqual(
            len(result.stdout.splitlines()),
            len(list(DATASETS_DIR.glob("*.yaml"))),
        )


LIBRARY = str(fixture_path("library_loans"))
LOANS = OUTCOMES["library_loans"]
SENSOR = str(fixture_path("sensor_readings"))
# Distinctive values, so a leak into the logs is easy to spot.
PROJECT = "secret-project-4f1c"
BQ_DATASET = "secret_dataset_4f1c"


class PipelineCommandsTest(unittest.TestCase):
    """extract, transform, load and run, isolated from any real .env."""

    def setUp(self) -> None:
        self.runner = CliRunner()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        # Settings reads .env from the working directory: never the repo's.
        self.addCleanup(os.chdir, Path.cwd())
        os.chdir(self.directory)
        environment = mock.patch.dict(
            os.environ,
            {
                "DATA_DIR": str(self.directory / "data"),
                "GCP_PROJECT_ID": PROJECT,
                "BQ_DATASET": BQ_DATASET,
            },
            clear=True,
        )
        environment.start()
        self.addCleanup(environment.stop)
        self.client = mock.create_autospec(bigquery.Client, instance=True)
        self.client.project = PROJECT
        self.client.__enter__.return_value = self.client
        self.tables_hold(LOANS.rows_valid, LOANS.rows_rejected)
        factory = mock.patch(
            "config_driven_etl.cli._bigquery_client", return_value=self.client
        )
        self.client_factory = factory.start()
        self.addCleanup(factory.stop)

    def tables_hold(self, *table_rows: int, audit: bool = False) -> None:
        """Tables, in load order, are missing, then hold table_rows rows.

        With audit, the audit table is looked up last and is missing too.
        """
        answers: list[object] = [NotFound("no table") for _ in table_rows]
        for rows in table_rows:
            table = mock.create_autospec(bigquery.Table, instance=True)
            table.num_rows = rows
            table.time_partitioning = None
            answers.append(table)
        if audit:
            answers.append(NotFound("no audit table"))
        self.client.get_table.side_effect = answers

    def invoke(self, *args: str) -> Result:
        return self.runner.invoke(app, list(args))

    def all_events(self, result: Result) -> list[dict[str, Any]]:
        """The JSON events a command logged, one per stderr line."""
        return [json.loads(line) for line in result.stderr.splitlines()]

    def events(self, result: Result) -> list[dict[str, Any]]:
        """The events before the run_finished line every command ends with."""
        *events, summary = self.all_events(result)
        self.assertEqual(summary["event"], "run_finished")
        return events

    def summary(self, result: Result) -> dict[str, Any]:
        return self.all_events(result)[-1]

    def test_stages_one_by_one(self) -> None:
        data = self.directory / "data"

        extract = self.invoke("extract", LIBRARY)
        self.assertEqual(extract.exit_code, 0, extract.output)
        self.assertTrue((data / "raw" / "library_loans").is_dir())

        transform = self.invoke("transform", LIBRARY)
        self.assertEqual(transform.exit_code, 0, transform.output)
        self.assertTrue(
            (
                data / "processed" / "library_loans" / "library_loans.parquet"
            ).is_file()
        )

        load = self.invoke("load", LIBRARY)
        self.assertEqual(load.exit_code, 0, load.output)
        self.client_factory.assert_called_once_with(PROJECT)

    def test_run_executes_every_stage(self) -> None:
        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(result.stdout, "")
        events = self.events(result)
        self.assertEqual(
            [
                (
                    event["event"],
                    event["stage"],
                    event["dataset"],
                    event["rows_in"],
                    event["rows_out"],
                    event["rows_rejected"],
                )
                for event in events
            ],
            [
                (
                    "stage_completed",
                    "extract",
                    "library_loans",
                    None,
                    LOANS.rows_in,
                    0,
                ),
                (
                    "stage_completed",
                    "transform",
                    "library_loans",
                    LOANS.rows_in,
                    LOANS.rows_valid,
                    LOANS.rows_rejected,
                ),
                (
                    "stage_completed",
                    "load",
                    "library_loans",
                    LOANS.rows_valid,
                    LOANS.rows_valid,
                    0,
                ),
            ],
        )
        for event in events:
            self.assertEqual(event["level"], "info")
            self.assertGreaterEqual(event["duration_s"], 0)
        self.assertEqual(events[1]["rejections"], LOANS.rejections)
        self.assertEqual(
            events[1]["dropped_columns"], list(LOANS.dropped_columns)
        )
        for event in (events[0], events[2]):
            self.assertNotIn("dropped_columns", event)
        self.assertEqual(self.client.load_table_from_file.call_count, 2)

    def test_logs_where_the_rejected_rows_are(self) -> None:
        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        _, transform, load = self.events(result)
        # Relative to DATA_DIR, with / on every OS.
        self.assertEqual(
            transform["rejected_file"],
            "processed/library_loans/library_loans.rejected.parquet",
        )
        self.assertEqual(
            (load["rejected_table"], load["rejected_rows_loaded"]),
            ("library_loans_rejected", LOANS.rows_rejected),
        )

    def test_without_rejected_table_load_logs_no_rejected_fields(
        self,
    ) -> None:
        self.tables_hold(OUTCOMES["sensor_readings"].rows_valid)

        result = self.invoke("run", SENSOR)

        self.assertEqual(result.exit_code, 0, result.output)
        _, transform, load = self.events(result)
        self.assertEqual(
            transform["rejected_file"],
            "processed/sensor_readings/sensor_readings.rejected.parquet",
        )
        self.assertNotIn("rejected_table", load)
        self.assertNotIn("rejected_rows_loaded", load)

    def test_logs_never_contain_settings(self) -> None:
        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        data_dir = os.environ["DATA_DIR"]
        for value in (
            PROJECT,
            BQ_DATASET,
            data_dir,
            Path(data_dir).as_posix(),
        ):
            self.assertNotIn(value, result.stderr)

    def test_transform_before_extract_fails(self) -> None:
        result = self.invoke("transform", LIBRARY)

        self.assertEqual(result.exit_code, 4)
        (event,) = self.events(result)
        self.assertEqual(event["level"], "error")
        self.assertEqual(event["event"], "run_failed")
        self.assertEqual(event["dataset_file"], LIBRARY)
        self.assertEqual(event["category"], "extract")
        self.assertEqual(event["exit_code"], 4)
        self.assertIn("cannot read CSV", event["error"])

    def test_invalid_dataset_file_fails(self) -> None:
        result = self.invoke("run", str(self.directory / "missing.yaml"))

        self.assertEqual(result.exit_code, 3)
        (event,) = self.events(result)
        self.assertEqual(event["category"], "config")
        self.assertIn("cannot read file", event["error"])

    def test_run_without_settings_fails_before_extracting(self) -> None:
        del os.environ["BQ_DATASET"]

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 3)
        (event,) = self.events(result)
        self.assertEqual(event["category"], "config")
        self.assertEqual(event["error"], "missing setting: BQ_DATASET")
        summary = self.summary(result)
        self.assertEqual(
            (summary["dataset"], summary["stages"], summary["failed_stage"]),
            ("library_loans", [], None),
        )
        self.assertFalse((self.directory / "data").exists())
        self.client_factory.assert_not_called()

    def test_data_that_does_not_match_the_config_exits_with_five(self) -> None:
        source = self.directory / "source"
        source.mkdir()
        (source / "x.csv").write_text("a,extra\n1,2\n", encoding="utf-8")
        dataset = source / "x.yaml"
        dataset.write_text(
            "name: x\nsource:\n  path: x.csv\ncolumns:\n"
            "  - {source: a, name: a, type: int}\n"
            "rules:\n  drop_unmapped_columns: false\n"
            "target:\n  table: x\n",
            encoding="utf-8",
        )
        self.assertEqual(self.invoke("extract", str(dataset)).exit_code, 0)

        result = self.invoke("transform", str(dataset))

        self.assertEqual(result.exit_code, 5)
        (event,) = self.events(result)
        self.assertEqual(event["category"], "transform")
        self.assertIn("unmapped source column: 'extra'", event["error"])

    def gated_library(self, limit: float) -> str:
        """The library_loans fixture with rules.max_rejected_ratio set."""
        fixture = Path(LIBRARY)
        text = fixture.read_text(encoding="utf-8")
        csv = (fixture.parent / "library_loans.csv").as_posix()
        text = text.replace(
            "path: library_loans.csv", f"path: '{csv}'"
        ).replace(
            "drop_unmapped_columns: true",
            f"drop_unmapped_columns: true\n  max_rejected_ratio: {limit}",
        )
        dataset = self.directory / "gated.yaml"
        dataset.write_text(text, encoding="utf-8")
        return str(dataset)

    def test_quality_gate_failure_exits_with_seven(self) -> None:
        dataset = self.gated_library(0.5)
        self.assertEqual(self.invoke("extract", dataset).exit_code, 0)

        for command in ["transform", "run"]:
            with self.subTest(command=command):
                result = self.invoke(command, dataset)

                self.assertEqual(result.exit_code, 7)
                *_, stage, failure = self.events(result)
                self.assertEqual(
                    (stage["event"], stage["stage"]),
                    ("stage_failed", "transform"),
                )
                self.assertEqual(stage["rows_rejected"], LOANS.rows_rejected)
                self.assertEqual(stage["rejections"], dict(LOANS.rejections))
                self.assertEqual(
                    stage["dropped_columns"], list(LOANS.dropped_columns)
                )
                self.assertEqual(
                    stage["rejected_file"],
                    "processed/library_loans/library_loans.rejected.parquet",
                )
                self.assertEqual(failure["category"], "quality_gate")
                self.assertIn(
                    "above rules.max_rejected_ratio 0.5", failure["error"]
                )
        self.client_factory.assert_not_called()

    def test_load_after_a_failed_quality_gate_loads_nothing(self) -> None:
        # A previous run left valid rows that a load could pick up.
        self.assertEqual(self.invoke("extract", LIBRARY).exit_code, 0)
        self.assertEqual(self.invoke("transform", LIBRARY).exit_code, 0)
        dataset = self.gated_library(0.5)
        self.assertEqual(self.invoke("transform", dataset).exit_code, 7)

        result = self.invoke("load", dataset)

        self.assertEqual(result.exit_code, 6)
        self.assertIn(
            "not found; run transform first", self.events(result)[-1]["error"]
        )
        self.client.load_table_from_file.assert_not_called()

    def test_bigquery_errors_never_log_the_project(self) -> None:
        # Google quotes the project in its messages, e.g. in request URLs.
        denied = Forbidden(f"POST .../projects/{PROJECT}/jobs: denied")
        self.enable_audit().result.side_effect = denied
        self.client.get_table.side_effect = denied

        for command in ["run", "extract"]:
            with self.subTest(command=command):
                result = self.invoke(command, LIBRARY)

                self.assertIn(result.exit_code, (6, 8))
                self.assertIn("projects/<project>/jobs", result.stderr)
                self.assertNotIn(PROJECT, result.stderr)

    def test_bigquery_failure_exits_with_six(self) -> None:
        self.client.get_table.side_effect = Forbidden("access denied")

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 6)
        event = self.events(result)[-1]
        self.assertEqual(event["category"], "load")
        self.assertIn("403 access denied", event["error"])
        summary = self.summary(result)
        self.assertEqual(
            (summary["category"], summary["failed_stage"]), ("load", "load")
        )
        self.assertEqual(
            [stage["stage"] for stage in summary["stages"]],
            ["extract", "transform"],
        )

    def test_missing_credentials_exit_with_six(self) -> None:
        self.client_factory.side_effect = DefaultCredentialsError("no ADC")

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 6)
        event = self.events(result)[-1]
        self.assertEqual(event["category"], "load")
        self.assertEqual(event["error"], "no ADC")

    def test_unexpected_errors_exit_with_one(self) -> None:
        self.client_factory.side_effect = RuntimeError("bug")

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, RuntimeError)
        summary = self.summary(result)
        self.assertEqual(
            (summary["level"], summary["outcome"], summary["exit_code"]),
            ("error", "failed", 1),
        )
        self.assertEqual(
            [stage["stage"] for stage in summary["stages"]],
            ["extract", "transform"],
        )

    def test_successful_run_ends_with_its_summary(self) -> None:
        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        stages = self.events(result)
        summary = self.summary(result)
        self.assertEqual(summary["level"], "info")
        self.assertEqual(
            {
                key: summary[key]
                for key in [
                    "command",
                    "version",
                    "dataset",
                    "outcome",
                    "exit_code",
                ]
            },
            {
                "command": "run",
                "version": __version__,
                "dataset": "library_loans",
                "outcome": "success",
                "exit_code": 0,
            },
        )
        self.assertEqual(
            (summary["category"], summary["failed_stage"]), (None, None)
        )
        self.assertRegex(summary["run_id"], r"^[0-9a-f]{32}$")
        self.assertEqual(
            {stage["run_id"] for stage in stages}, {summary["run_id"]}
        )
        started_at = datetime.fromisoformat(summary["started_at"])
        self.assertEqual(started_at.utcoffset(), timedelta(0))
        self.assertGreaterEqual(summary["duration_s"], 0)
        keys = ["stage", "rows_in", "rows_out", "rows_rejected", "duration_s"]
        self.assertEqual(
            [{key: stage[key] for key in keys} for stage in summary["stages"]],
            [{key: stage[key] for key in keys} for stage in stages],
        )
        _, transform, load = summary["stages"]
        self.assertEqual(
            {item["reason"]: item["rows"] for item in transform["rejections"]},
            LOANS.rejections,
        )
        self.assertEqual(
            transform["dropped_columns"], list(LOANS.dropped_columns)
        )
        self.assertEqual(load["rejections"], [])
        self.assertEqual(load["dropped_columns"], [])

    def enable_audit(self, *table_rows: int) -> mock.MagicMock:
        """Set BQ_AUDIT_TABLE; return the job that appends the row."""
        os.environ["BQ_AUDIT_TABLE"] = "etl_runs"
        self.tables_hold(*table_rows, audit=True)
        job = mock.create_autospec(bigquery.LoadJob, instance=True)
        job.output_rows = 1
        self.client.load_table_from_json.return_value = job
        return job

    def audit_rows(self) -> list[dict[str, Any]]:
        calls = self.client.load_table_from_json.call_args_list
        for (_, table_id), _ in calls:
            self.assertEqual(table_id, f"{PROJECT}.{BQ_DATASET}.etl_runs")
        return [row for (rows, _), _ in calls for row in rows]

    def assert_row_is_the_summary(
        self, row: dict[str, Any], result: Result
    ) -> None:
        summary = self.summary(result)
        for key in ["time", "level", "event"]:
            del summary[key]
        self.assertEqual(row, summary)

    def test_audit_appends_the_summary_of_a_run(self) -> None:
        self.enable_audit(LOANS.rows_valid, LOANS.rows_rejected)

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        (row,) = self.audit_rows()
        self.assert_row_is_the_summary(row, result)
        self.assertEqual((row["outcome"], row["exit_code"]), ("success", 0))
        _, kwargs = self.client.load_table_from_json.call_args
        self.assertEqual(
            kwargs["job_config"].schema, list(AUDIT_TABLE_SPEC.schema)
        )
        created = self.client.create_table.call_args_list[-1].args[0]
        self.assertEqual(created.schema, list(AUDIT_TABLE_SPEC.schema))

    def test_unexpected_error_is_audited_and_keeps_its_traceback(
        self,
    ) -> None:
        self.enable_audit()
        # Every table is missing: the load stops before any row count.
        self.client.get_table.side_effect = NotFound("no table")
        self.client.load_table_from_file.side_effect = RuntimeError("bug")

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, RuntimeError)
        (row,) = self.audit_rows()
        self.assertEqual(
            (row["exit_code"], row["category"], row["failed_stage"]),
            (1, "internal", "load"),
        )

    def test_audit_bug_still_logs_the_summary(self) -> None:
        self.enable_audit().result.side_effect = RuntimeError("audit bug")

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, RuntimeError)
        summary = self.summary(result)
        self.assertEqual(
            (summary["event"], summary["exit_code"], summary["category"]),
            ("run_finished", 1, "internal"),
        )

    def test_missing_credentials_for_the_audit_exit_with_eight(self) -> None:
        self.enable_audit()
        self.client_factory.side_effect = DefaultCredentialsError("no ADC")

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 8)
        (failure,) = self.events(result)[1:]
        self.assertEqual(
            (failure["event"], failure["error"]), ("audit_failed", "no ADC")
        )
        self.assertEqual(self.summary(result)["category"], "audit")

    def test_audit_appends_a_failed_run(self) -> None:
        self.enable_audit()

        result = self.invoke("run", self.gated_library(0.5))

        self.assertEqual(result.exit_code, 7)
        (row,) = self.audit_rows()
        self.assert_row_is_the_summary(row, result)
        self.assertEqual(
            (row["exit_code"], row["failed_stage"]), (7, "transform")
        )

    def test_audit_appends_a_stage_command(self) -> None:
        self.enable_audit()

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        (row,) = self.audit_rows()
        self.assertEqual(row["command"], "extract")
        self.client.load_table_from_file.assert_not_called()

    def test_failed_audit_of_a_successful_run_exits_with_eight(self) -> None:
        self.enable_audit().result.side_effect = Forbidden("no access")

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 8)
        (failure,) = self.events(result)[1:]
        self.assertEqual(failure["event"], "audit_failed")
        self.assertIn("403 no access", failure["error"])
        summary = self.summary(result)
        self.assertEqual(
            (summary["outcome"], summary["exit_code"], summary["category"]),
            ("failed", 8, "audit"),
        )
        self.assertEqual(summary["run_id"], failure["run_id"])

    def test_failed_audit_of_a_failed_run_keeps_its_exit_code(self) -> None:
        self.enable_audit().result.side_effect = Forbidden("no access")

        result = self.invoke("run", self.gated_library(0.5))

        self.assertEqual(result.exit_code, 7)
        self.assertEqual(
            [event["event"] for event in self.events(result)][-2:],
            ["run_failed", "audit_failed"],
        )
        self.assertEqual(self.summary(result)["exit_code"], 7)

    def test_invalid_audit_setting_is_a_config_error(self) -> None:
        self.enable_audit()
        os.environ["BQ_AUDIT_TABLE"] = "Bad-Name"

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 3)
        (failure,) = self.events(result)
        self.assertTrue(failure["error"].startswith("BQ_AUDIT_TABLE: "))
        self.assertIsNone(self.summary(result)["dataset"])
        self.client.load_table_from_json.assert_not_called()
        self.assertFalse((self.directory / "data").exists())

    def test_pretty_log_format_writes_one_text_line_per_event(self) -> None:
        os.environ["LOG_FORMAT"] = "pretty"

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        lines = result.stderr.splitlines()
        self.assertEqual(
            [line.split()[1] for line in lines],
            ["extract", "transform", "load", "run"],
        )
        self.assertFalse(any(line.startswith("{") for line in lines))
        self.assertIn("library_loans  success  exit 0", lines[-1])
        self.assertRegex(lines[-1], r"run [0-9a-f]{32}$")
        for value in (PROJECT, BQ_DATASET, os.environ["DATA_DIR"]):
            self.assertNotIn(value, result.stderr)

    def test_invalid_setting_is_reported_in_the_asked_format(self) -> None:
        os.environ.update(LOG_FORMAT="pretty", BQ_AUDIT_TABLE="Bad-Name")

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 3)
        error, summary = result.stderr.splitlines()
        self.assertIn("error      config, exit 3: BQ_AUDIT_TABLE: ", error)
        self.assertIn("extract    -  failed  exit 3  config", summary)

    def test_invalid_log_format_is_a_config_error_in_json(self) -> None:
        os.environ["LOG_FORMAT"] = "fancy"

        result = self.invoke("extract", LIBRARY)

        self.assertEqual(result.exit_code, 3)
        (failure,) = self.events(result)
        self.assertTrue(failure["error"].startswith("LOG_FORMAT: "))

    def test_every_pipeline_command_is_summarized(self) -> None:
        self.assertEqual(self.invoke("extract", LIBRARY).exit_code, 0)
        self.assertEqual(self.invoke("transform", LIBRARY).exit_code, 0)
        for command in ["extract", "transform", "load"]:
            with self.subTest(command=command):
                result = self.invoke(command, LIBRARY)

                self.assertEqual(result.exit_code, 0, result.output)
                summary = self.summary(result)
                self.assertEqual(summary["command"], command)
                self.assertEqual(
                    [stage["stage"] for stage in summary["stages"]],
                    [command],
                )

    def test_failed_quality_gate_is_summarized_with_its_counts(
        self,
    ) -> None:
        result = self.invoke("run", self.gated_library(0.5))

        self.assertEqual(result.exit_code, 7)
        summary = self.summary(result)
        self.assertEqual(
            (summary["level"], summary["outcome"], summary["exit_code"]),
            ("error", "failed", 7),
        )
        _, transform = summary["stages"]
        self.assertEqual(
            (transform["stage"], transform["rows_rejected"]),
            ("transform", LOANS.rows_rejected),
        )
        self.assertEqual(
            (summary["category"], summary["failed_stage"]),
            ("quality_gate", "transform"),
        )
        # Every line of the command, failed ones included, shares run_id.
        self.assertEqual(
            {event["run_id"] for event in self.events(result)},
            {summary["run_id"]},
        )

    def test_unreadable_dataset_file_is_summarized_without_a_dataset(
        self,
    ) -> None:
        result = self.invoke("load", str(self.directory / "missing.yaml"))

        self.assertEqual(result.exit_code, 3)
        summary = self.summary(result)
        self.assertEqual(
            (summary["dataset"], summary["stages"], summary["exit_code"]),
            (None, [], 3),
        )

    def test_each_command_has_its_own_run_id(self) -> None:
        first = self.summary(self.invoke("extract", LIBRARY))
        second = self.summary(self.invoke("extract", LIBRARY))

        self.assertNotEqual(first["run_id"], second["run_id"])

    def test_usage_errors_exit_with_two(self) -> None:
        self.assertEqual(self.invoke("run").exit_code, 2)


class ExitCodeHelpTest(unittest.TestCase):
    def test_help_documents_every_exit_code(self) -> None:
        result = CliRunner().invoke(app, ["--help"], terminal_width=200)

        text = " ".join(ANSI_ESCAPE.sub("", result.output).split())
        for code in ExitCode:
            with self.subTest(code=code.name):
                self.assertIn(f"{code.value} ", text)
        self.assertIn("6 Parquet output or BigQuery load failed", text)
