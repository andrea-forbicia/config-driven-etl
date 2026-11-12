import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from typer.testing import CliRunner, Result

from config_driven_etl import __version__
from config_driven_etl.cli import app

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
INVALID_FIXTURES_DIR = Path(__file__).parents[1] / "fixtures" / "invalid"

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

    def test_invalid_file_exits_with_one(self) -> None:
        path = self.write(
            "bad.yaml", VALID_YAML.replace("type: int", "type: decimal")
        )

        result = self.runner.invoke(app, ["validate", str(path)])

        self.assertEqual(result.exit_code, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith(f"{path}: columns[0].type: "))

    def test_missing_file(self) -> None:
        path = self.directory / "missing.yaml"

        result = self.runner.invoke(app, ["validate", str(path)])

        self.assertEqual(result.exit_code, 1)
        self.assertIn(f"{path}: cannot read file", result.stderr)

    def test_checks_every_file_before_failing(self) -> None:
        bad = self.write("a_bad.yaml", "name: Bad\n")
        good = self.write("b_good.yaml", VALID_YAML)

        result = self.runner.invoke(app, ["validate", str(bad), str(good)])

        self.assertEqual(result.exit_code, 1)
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

        self.assertEqual(result.exit_code, 1)
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

                self.assertEqual(result.exit_code, 1)
                self.assertEqual(result.stdout, "")
                # Exactly one line: no misleading follow-up errors.
                (line,) = result.stderr.splitlines()
                self.assertTrue(line.startswith(f"{path}: {problem}"))

    def test_fixture_datasets_are_valid(self) -> None:
        fixtures = Path(__file__).parents[1] / "fixtures"
        paths = [
            str(fixtures / "sensor_readings"),
            str(fixtures / "library_loans"),
        ]

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


FIXTURES_DIR = Path(__file__).parents[1] / "fixtures"
LIBRARY = str(FIXTURES_DIR / "library_loans" / "library_loans.yaml")
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
        table = mock.create_autospec(bigquery.Table, instance=True)
        table.num_rows = 4
        table.time_partitioning = None
        self.client.get_table.side_effect = [NotFound("no table"), table]
        factory = mock.patch(
            "config_driven_etl.cli._bigquery_client", return_value=self.client
        )
        self.client_factory = factory.start()
        self.addCleanup(factory.stop)

    def invoke(self, *args: str) -> Result:
        return self.runner.invoke(app, list(args))

    def events(self, result: Result) -> list[dict[str, Any]]:
        """The JSON events a command logged, one per stderr line."""
        return [json.loads(line) for line in result.stderr.splitlines()]

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
                ("stage_completed", "extract", "library_loans", None, 12, 0),
                ("stage_completed", "transform", "library_loans", 12, 4, 8),
                ("stage_completed", "load", "library_loans", 4, 4, 0),
            ],
        )
        for event in events:
            self.assertEqual(event["level"], "info")
            self.assertGreaterEqual(event["duration_s"], 0)
        self.assertEqual(sum(events[1]["rejections"].values()), 8)
        self.client.load_table_from_file.assert_called_once()

    def test_logs_never_contain_settings(self) -> None:
        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 0, result.output)
        for value in (PROJECT, BQ_DATASET):
            self.assertNotIn(value, result.stderr)

    def test_transform_before_extract_fails(self) -> None:
        result = self.invoke("transform", LIBRARY)

        self.assertEqual(result.exit_code, 1)
        (event,) = self.events(result)
        self.assertEqual(event["level"], "error")
        self.assertEqual(event["event"], "run_failed")
        self.assertEqual(event["dataset_file"], LIBRARY)
        self.assertIn("cannot read CSV", event["error"])

    def test_invalid_dataset_file_fails(self) -> None:
        result = self.invoke("run", str(self.directory / "missing.yaml"))

        self.assertEqual(result.exit_code, 1)
        (event,) = self.events(result)
        self.assertIn("cannot read file", event["error"])

    def test_run_without_settings_fails_before_extracting(self) -> None:
        del os.environ["BQ_DATASET"]

        result = self.invoke("run", LIBRARY)

        self.assertEqual(result.exit_code, 1)
        (event,) = self.events(result)
        self.assertEqual(event["error"], "missing setting: BQ_DATASET")
        self.assertFalse((self.directory / "data").exists())
        self.client_factory.assert_not_called()
