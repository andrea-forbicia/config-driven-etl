import re
import tempfile
import unittest
from pathlib import Path

from typer.testing import CliRunner

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

    def test_committed_datasets_are_valid(self) -> None:
        result = self.runner.invoke(app, ["validate", str(DATASETS_DIR)])

        self.assertEqual(result.exit_code, 0, result.stderr)
        self.assertEqual(
            len(result.stdout.splitlines()),
            len(list(DATASETS_DIR.glob("*.yaml"))),
        )
