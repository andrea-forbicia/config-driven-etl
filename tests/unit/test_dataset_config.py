import tempfile
import unittest
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from config_driven_etl.dataset_config import (
    TEMPORAL_TYPES,
    Column,
    ColumnType,
    DatasetConfig,
    DatasetConfigError,
    Rules,
    Source,
    Target,
    _format_yaml_error,
    load_dataset_config,
)

EXAMPLE_PATH = Path(__file__).parents[2] / "datasets" / "example.yaml"


def minimal_config() -> dict[str, Any]:
    return {
        "name": "sample",
        "source": {"path": "data/raw/sample.csv"},
        "columns": [{"source": "Id", "name": "id", "type": "int"}],
        "target": {"table": "sample"},
    }


def full_config() -> dict[str, Any]:
    return {
        "name": "city_weather",
        "source": {
            "url": "https://example.org/weather.csv",
            "delimiter": ";",
            "encoding": "utf-8-lossy",
        },
        "columns": [
            {
                "source": "Date",
                "name": "observation_date",
                "type": "date",
                "format": "%Y-%m-%d",
                "nullable": False,
            },
            {
                "source": "Temp (C)",
                "name": "temperature_c",
                "type": "float",
                "min": -90,
                "max": 60,
            },
            {"source": "Station", "name": "station_id", "type": "string"},
        ],
        "rules": {
            "deduplicate_on": ["observation_date", "station_id"],
            "drop_unmapped_columns": False,
        },
        "target": {
            "table": "city_weather",
            "partition_by": "observation_date",
            "cluster_by": ["station_id"],
            "write_mode": "truncate",
        },
    }


class DatasetConfigValidTest(unittest.TestCase):
    def test_minimal_config_applies_defaults(self) -> None:
        config = DatasetConfig.model_validate(minimal_config())

        self.assertEqual(config.source.path, Path("data/raw/sample.csv"))
        self.assertIsNone(config.source.url)
        self.assertEqual(config.source.delimiter, ",")
        self.assertEqual(config.source.encoding, "utf-8")
        column = config.columns[0]
        self.assertIs(column.type, ColumnType.INT)
        self.assertTrue(column.nullable)
        self.assertIsNone(column.format)
        self.assertIsNone(column.min)
        self.assertIsNone(column.max)
        self.assertEqual(config.rules.deduplicate_on, ())
        self.assertTrue(config.rules.drop_unmapped_columns)
        self.assertIsNone(config.target.partition_by)
        self.assertEqual(config.target.cluster_by, ())
        self.assertEqual(config.target.write_mode, "truncate")

    def test_full_config_keeps_every_value(self) -> None:
        config = DatasetConfig.model_validate(full_config())

        self.assertEqual(
            str(config.source.url), "https://example.org/weather.csv"
        )
        self.assertEqual(config.source.delimiter, ";")
        self.assertEqual(config.source.encoding, "utf-8-lossy")
        date_column, temperature_column, _ = config.columns
        self.assertIs(date_column.type, ColumnType.DATE)
        self.assertEqual(date_column.format, "%Y-%m-%d")
        self.assertFalse(date_column.nullable)
        self.assertEqual(temperature_column.min, -90.0)
        self.assertEqual(temperature_column.max, 60.0)
        self.assertEqual(
            config.rules.deduplicate_on, ("observation_date", "station_id")
        )
        self.assertFalse(config.rules.drop_unmapped_columns)
        self.assertEqual(config.target.partition_by, "observation_date")
        self.assertEqual(config.target.cluster_by, ("station_id",))

    def test_every_column_type_is_accepted(self) -> None:
        for column_type in ColumnType:
            with self.subTest(column_type=column_type):
                data = {"source": "x", "name": "x", "type": column_type.value}
                if column_type in TEMPORAL_TYPES:
                    data["format"] = "%Y-%m-%d"

                column = Column.model_validate(data)

                self.assertIs(column.type, column_type)

    def test_source_header_is_free_text(self) -> None:
        column = Column.model_validate(
            {"source": "Temp (C) / día", "name": "temp", "type": "float"}
        )

        self.assertEqual(column.source, "Temp (C) / día")

    def test_config_is_immutable(self) -> None:
        config = DatasetConfig.model_validate(minimal_config())

        with self.assertRaises(ValidationError):
            config.name = "other"  # type: ignore[misc]


class DatasetConfigInvalidTest(unittest.TestCase):
    def assert_invalid(self, data: dict[str, Any], location: tuple) -> None:
        with self.assertRaises(ValidationError) as context:
            DatasetConfig.model_validate(data)

        locations = [error["loc"] for error in context.exception.errors()]
        self.assertIn(location, locations)

    def test_unknown_column_type(self) -> None:
        data = minimal_config()
        data["columns"][0]["type"] = "decimal"

        self.assert_invalid(data, ("columns", 0, "type"))

    def test_invalid_only_column_reports_only_its_own_error(self) -> None:
        data = minimal_config()
        data["columns"][0]["type"] = "decimal"

        with self.assertRaises(ValidationError) as context:
            DatasetConfig.model_validate(data)

        locations = [error["loc"] for error in context.exception.errors()]
        self.assertEqual(locations, [("columns", 0, "type")])

    def test_unknown_key_is_rejected(self) -> None:
        data = minimal_config()
        data["columns"][0]["nulable"] = False

        self.assert_invalid(data, ("columns", 0, "nulable"))

    def test_unknown_top_level_key_is_rejected(self) -> None:
        data = minimal_config()
        data["schedule"] = "daily"

        self.assert_invalid(data, ("schedule",))

    def test_invalid_identifiers(self) -> None:
        for value in [
            "",
            "Sample",
            "1st",
            "with space",
            "with-dash",
            "x" * 129,
        ]:
            with self.subTest(value=value):
                data = minimal_config()
                data["name"] = value

                self.assert_invalid(data, ("name",))

    def test_identifier_rule_applies_to_references(self) -> None:
        cases = [
            ("rules", {"deduplicate_on": ["Id"]}, ("deduplicate_on", 0)),
            (
                "target",
                {"table": "t", "partition_by": "My Date"},
                ("partition_by",),
            ),
            (
                "target",
                {"table": "t", "cluster_by": ["a-b"]},
                ("cluster_by", 0),
            ),
            ("target", {"table": "My-Table"}, ("table",)),
        ]
        for section, value, location in cases:
            with self.subTest(section=section, value=value):
                data = minimal_config()
                data[section] = value

                self.assert_invalid(data, (section, *location))

    def test_tab_delimiter_is_accepted(self) -> None:
        data = minimal_config()
        data["source"]["delimiter"] = "\t"

        config = DatasetConfig.model_validate(data)

        self.assertEqual(config.source.delimiter, "\t")

    def test_empty_path(self) -> None:
        data = minimal_config()
        data["source"]["path"] = ""

        self.assert_invalid(data, ("source", "path"))

    def test_non_finite_bound(self) -> None:
        for bound in ["min", "max"]:
            for value in [float("nan"), float("inf"), float("-inf")]:
                with self.subTest(bound=bound, value=value):
                    data = minimal_config()
                    data["columns"][0][bound] = value

                    self.assert_invalid(data, ("columns", 0, bound))

    def test_delimiter_must_be_one_byte(self) -> None:
        for value in ["", ";;", "é", "\x00"]:
            with self.subTest(value=value):
                data = minimal_config()
                data["source"]["delimiter"] = value

                self.assert_invalid(data, ("source", "delimiter"))

    def test_unsupported_encoding(self) -> None:
        data = minimal_config()
        data["source"]["encoding"] = "latin-1"

        self.assert_invalid(data, ("source", "encoding"))

    def test_invalid_url(self) -> None:
        data = minimal_config()
        data["source"] = {"url": "not a url"}

        self.assert_invalid(data, ("source", "url"))

    def test_non_numeric_bound(self) -> None:
        data = minimal_config()
        data["columns"][0]["max"] = "high"

        self.assert_invalid(data, ("columns", 0, "max"))

    def test_more_than_four_cluster_columns(self) -> None:
        data = minimal_config()
        data["target"]["cluster_by"] = ["a", "b", "c", "d", "e"]

        self.assert_invalid(data, ("target", "cluster_by"))

    def test_unsupported_write_mode(self) -> None:
        data = minimal_config()
        data["target"]["write_mode"] = "append"

        self.assert_invalid(data, ("target", "write_mode"))

    def test_empty_columns(self) -> None:
        data = minimal_config()
        data["columns"] = []

        self.assert_invalid(data, ("columns",))

    def test_empty_source_header(self) -> None:
        data = minimal_config()
        data["columns"][0]["source"] = ""

        self.assert_invalid(data, ("columns", 0, "source"))

    def test_missing_required_sections(self) -> None:
        for key in ["name", "source", "columns", "target"]:
            with self.subTest(key=key):
                data = minimal_config()
                del data[key]

                self.assert_invalid(data, (key,))


def error_message(context: Any) -> str:
    """Return the single model-level message, without pydantic's prefix."""
    (error,) = context.exception.errors()
    return error["msg"].removeprefix("Value error, ")


class SourceLocationTest(unittest.TestCase):
    def test_url_or_path_alone_is_valid(self) -> None:
        for data in [
            {"url": "https://example.org/data.csv"},
            {"path": "data/raw/data.csv"},
        ]:
            with self.subTest(data=data):
                Source.model_validate(data)

    def test_both_or_neither_is_invalid(self) -> None:
        for data in [
            {},
            {"url": "https://example.org/data.csv", "path": "data.csv"},
        ]:
            with self.subTest(data=data):
                with self.assertRaises(ValidationError) as context:
                    Source.model_validate(data)

                self.assertEqual(
                    error_message(context),
                    "set exactly one of 'url' or 'path'",
                )


class ColumnTypeOptionsTest(unittest.TestCase):
    def column_error(self, data: dict[str, Any]) -> str:
        with self.assertRaises(ValidationError) as context:
            Column.model_validate({"source": "x", "name": "x", **data})

        return error_message(context)

    def test_bounds_on_numeric_columns(self) -> None:
        for column_type in ["int", "float"]:
            with self.subTest(column_type=column_type):
                column = Column.model_validate(
                    {
                        "source": "x",
                        "name": "x",
                        "type": column_type,
                        "min": 0,
                        "max": 0,
                    }
                )

                self.assertEqual((column.min, column.max), (0.0, 0.0))

    def test_bounds_on_non_numeric_columns(self) -> None:
        cases = [
            ({"type": "string", "min": 0}, "string"),
            ({"type": "bool", "max": 1}, "bool"),
            ({"type": "date", "format": "%Y-%m-%d", "min": 0}, "date"),
        ]
        for data, column_type in cases:
            with self.subTest(data=data):
                self.assertEqual(
                    self.column_error(data),
                    "'min' and 'max' are only allowed on int and float "
                    f"columns, not {column_type}",
                )

    def test_min_greater_than_max(self) -> None:
        self.assertEqual(
            self.column_error({"type": "int", "min": 10, "max": 1}),
            "'min' (10.0) must not exceed 'max' (1.0)",
        )

    def test_format_required_on_temporal_columns(self) -> None:
        for column_type in ["date", "datetime"]:
            with self.subTest(column_type=column_type):
                self.assertEqual(
                    self.column_error({"type": column_type}),
                    f"'format' is required on {column_type} columns",
                )

    def test_format_on_non_temporal_columns(self) -> None:
        for column_type in ["string", "int", "float", "bool"]:
            with self.subTest(column_type=column_type):
                self.assertEqual(
                    self.column_error({"type": column_type, "format": "%Y"}),
                    "'format' is only allowed on date and datetime "
                    f"columns, not {column_type}",
                )

    def test_valid_temporal_formats(self) -> None:
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%-d/%-m/%y",
            "%d %b %Y",
            "%Y%m%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %I:%M %p",
            "%Y-%m-%dT%H:%M:%S%#z",
            "%Y-%m-%d %H:%M:%S%6f",
            "%Y-%m-%d %%Z",
        ]
        for column_type in ["date", "datetime"]:
            for format_ in formats:
                with self.subTest(type=column_type, format=format_):
                    column = Column.model_validate(
                        {
                            "source": "x",
                            "name": "x",
                            "type": column_type,
                            "format": format_,
                        }
                    )

                    self.assertEqual(column.format, format_)

    def test_format_with_unknown_or_incomplete_directive(self) -> None:
        for format_ in ["%Q", "%Y-%m-%d %", "%"]:
            for column_type in ["date", "datetime"]:
                with self.subTest(type=column_type, format=format_):
                    self.assertEqual(
                        self.column_error(
                            {"type": column_type, "format": format_}
                        ),
                        f"'format' {format_!r} has an unknown or "
                        "incomplete directive",
                    )

    def test_format_rejected_by_the_parser(self) -> None:
        for column_type in ["date", "datetime"]:
            with self.subTest(type=column_type):
                self.assertEqual(
                    self.column_error(
                        {"type": column_type, "format": "%Y-%m-%d %H"}
                    ),
                    f"'format' '%Y-%m-%d %H' cannot be used to read a "
                    f"{column_type}: Invalid format string: Please either "
                    "specify both hour and minute, or neither.",
                )

    def test_format_that_cannot_be_read_back(self) -> None:
        cases = [("date", "%s"), ("datetime", "%Y-%m-%d %z")]
        for column_type, format_ in cases:
            with self.subTest(type=column_type, format=format_):
                self.assertEqual(
                    self.column_error(
                        {"type": column_type, "format": format_}
                    ),
                    f"'format' {format_!r} cannot read back a "
                    f"{column_type} written with it",
                )

    def test_format_with_percent_f(self) -> None:
        for column_type in ["date", "datetime"]:
            with self.subTest(type=column_type):
                self.assertEqual(
                    self.column_error(
                        {"type": column_type, "format": "%H:%M:%S.%f"}
                    ),
                    "'format' '%H:%M:%S.%f' uses %f, which misreads "
                    "fractions; use %.f or %6f",
                )

    def test_format_with_time_zone_name(self) -> None:
        for column_type in ["date", "datetime"]:
            with self.subTest(type=column_type):
                self.assertEqual(
                    self.column_error(
                        {"type": column_type, "format": "%Y-%m-%d %Z"}
                    ),
                    "'format' '%Y-%m-%d %Z' uses %Z, whose time zone name "
                    "is ignored; use %z",
                )

    def test_format_without_a_full_date(self) -> None:
        for format_ in ["", "abc", "%Y", "%Y-%m", "%m-%d", "%H:%M:%S"]:
            for column_type in ["date", "datetime"]:
                with self.subTest(type=column_type, format=format_):
                    self.assertEqual(
                        self.column_error(
                            {"type": column_type, "format": format_}
                        ),
                        f"'format' {format_!r} must include the year, "
                        "month and day",
                    )

    def test_reports_every_problem(self) -> None:
        message = self.column_error(
            {"type": "string", "format": "%Y", "min": 2, "max": 1}
        )

        self.assertEqual(len(message.splitlines()), 3)


class DatasetConfigReferencesTest(unittest.TestCase):
    def config_error(self, data: dict[str, Any]) -> str:
        with self.assertRaises(ValidationError) as context:
            DatasetConfig.model_validate(data)

        return error_message(context)

    def test_duplicate_column_name(self) -> None:
        data = minimal_config()
        data["columns"].append(
            {"source": "Other", "name": "id", "type": "int"}
        )

        self.assertEqual(
            self.config_error(data),
            "columns: name 'id' is used more than once",
        )

    def test_duplicate_source_header(self) -> None:
        data = minimal_config()
        data["columns"].append({"source": "Id", "name": "id2", "type": "int"})

        self.assertEqual(
            self.config_error(data),
            "columns: source 'Id' is mapped more than once",
        )

    def test_duplicate_reported_once(self) -> None:
        data = minimal_config()
        data["columns"] += [
            {"source": f"Id{i}", "name": "id", "type": "int"} for i in range(2)
        ]

        self.assertEqual(
            self.config_error(data),
            "columns: name 'id' is used more than once",
        )

    def test_unknown_partition_column(self) -> None:
        data = minimal_config()
        data["target"]["partition_by"] = "missing"

        self.assertEqual(
            self.config_error(data),
            "target.partition_by: 'missing' is not a column",
        )

    def test_partition_column_must_be_temporal(self) -> None:
        data = minimal_config()
        data["target"]["partition_by"] = "id"

        self.assertEqual(
            self.config_error(data),
            "target.partition_by: 'id' must be a date or datetime column, "
            "not int",
        )

    def test_datetime_partition_column(self) -> None:
        data = minimal_config()
        data["columns"].append(
            {
                "source": "When",
                "name": "observed_at",
                "type": "datetime",
                "format": "%Y-%m-%dT%H:%M:%S",
            }
        )
        data["target"]["partition_by"] = "observed_at"

        config = DatasetConfig.model_validate(data)

        self.assertEqual(config.target.partition_by, "observed_at")

    def test_unknown_or_repeated_list_references(self) -> None:
        cases = [
            ("target", "cluster_by", ["missing"], "'missing' is not a column"),
            (
                "target",
                "cluster_by",
                ["id", "id"],
                "'id' is listed more than once",
            ),
            (
                "rules",
                "deduplicate_on",
                ["missing"],
                "'missing' is not a column",
            ),
            (
                "rules",
                "deduplicate_on",
                ["id", "id"],
                "'id' is listed more than once",
            ),
        ]
        for section, key, value, text in cases:
            with self.subTest(key=key, value=value):
                data = minimal_config()
                data.setdefault(section, {})[key] = value

                self.assertEqual(
                    self.config_error(data), f"{section}.{key}: {text}"
                )

    def test_float_cluster_column(self) -> None:
        data = minimal_config()
        data["columns"].append(
            {"source": "Temp", "name": "temperature", "type": "float"}
        )
        data["target"]["cluster_by"] = ["temperature"]

        self.assertEqual(
            self.config_error(data),
            "target.cluster_by: 'temperature' must not be a float column",
        )

    def test_float_column_allowed_in_deduplicate_on(self) -> None:
        data = minimal_config()
        data["columns"].append(
            {"source": "Temp", "name": "temperature", "type": "float"}
        )
        data["rules"] = {"deduplicate_on": ["temperature"]}

        config = DatasetConfig.model_validate(data)

        self.assertEqual(config.rules.deduplicate_on, ("temperature",))

    def test_reports_every_problem_in_order(self) -> None:
        data = minimal_config()
        data["columns"].append({"source": "Id", "name": "id", "type": "int"})
        data["target"].update(partition_by="missing", cluster_by=["nope"])
        data["rules"] = {"deduplicate_on": ["id", "id"]}

        self.assertEqual(
            self.config_error(data).splitlines(),
            [
                "columns: name 'id' is used more than once",
                "columns: source 'Id' is mapped more than once",
                "target.partition_by: 'missing' is not a column",
                "target.cluster_by: 'nope' is not a column",
                "rules.deduplicate_on: 'id' is listed more than once",
            ],
        )


VALID_YAML = """\
name: city_weather
source:
  url: https://example.org/weather.csv
columns:
  - source: "Date"
    name: observation_date
    type: date
    format: "%Y-%m-%d"
  - source: "Temp (C)"
    name: temperature_c
    type: float
    min: -90
    max: 60
target:
  table: city_weather
  partition_by: observation_date
"""


class LoadDatasetConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def write(self, text: str) -> Path:
        path = self.directory / "dataset.yaml"
        path.write_text(text, encoding="utf-8")
        return path

    def load_problems(self, path: Path) -> tuple[str, ...]:
        with self.assertRaises(DatasetConfigError) as context:
            load_dataset_config(path)

        self.assertEqual(context.exception.path, path)
        return context.exception.problems

    def test_valid_file(self) -> None:
        config = load_dataset_config(self.write(VALID_YAML))

        self.assertEqual(config.name, "city_weather")
        self.assertEqual(config.columns[1].max, 60.0)
        self.assertEqual(config.target.partition_by, "observation_date")

    def test_relative_source_path_starts_from_config_directory(self) -> None:
        text = VALID_YAML.replace(
            "url: https://example.org/weather.csv", "path: ../local/w.csv"
        )

        config = load_dataset_config(self.write(text))

        self.assertEqual(config.source.path, self.directory / "../local/w.csv")

    def test_absolute_source_path_is_kept(self) -> None:
        absolute = (self.directory / "w.csv").resolve()
        text = VALID_YAML.replace(
            "url: https://example.org/weather.csv", f"path: '{absolute}'"
        )

        config = load_dataset_config(self.write(text))

        self.assertEqual(config.source.path, absolute)

    def test_model_without_loader_keeps_relative_path(self) -> None:
        source = Source.model_validate({"path": "local/w.csv"})

        self.assertEqual(source.path, Path("local/w.csv"))

    def test_missing_file(self) -> None:
        problems = self.load_problems(self.directory / "missing.yaml")

        self.assertEqual(len(problems), 1)
        self.assertTrue(problems[0].startswith("cannot read file: "))

    def test_file_not_utf8(self) -> None:
        path = self.directory / "latin1.yaml"
        path.write_bytes("name: caf\xe9\n".encode("latin-1"))

        self.assertEqual(
            self.load_problems(path), ("file is not valid UTF-8",)
        )

    def test_invalid_yaml_syntax(self) -> None:
        problems = self.load_problems(self.write("name: [unclosed\n"))

        self.assertEqual(len(problems), 1)
        self.assertRegex(problems[0], r"^line \d+: ")

    def test_duplicate_key(self) -> None:
        text = VALID_YAML.replace(
            "    type: float\n", "    type: float\n    type: int\n"
        )

        self.assertEqual(
            self.load_problems(self.write(text)),
            ("line 12: duplicate key 'type'",),
        )

    def test_control_character(self) -> None:
        problems = self.load_problems(self.write("name: a\x07b\n"))

        self.assertEqual(len(problems), 1)
        self.assertIn("unacceptable character", problems[0])

    def test_merge_key_is_allowed_and_can_be_overridden(self) -> None:
        text = (
            "defaults: &defaults {type: int, nullable: false}\n"
            + VALID_YAML.replace(
                "    type: float\n",
                "    <<: *defaults\n    type: float\n",
            )
        )
        problems = self.load_problems(self.write(text))

        # Only the unknown top-level key fails: the merge itself is valid.
        self.assertEqual(
            problems, ("defaults: Extra inputs are not permitted",)
        )

    def test_unhashable_key(self) -> None:
        problems = self.load_problems(self.write("? [a, b]\n: 1\n"))

        self.assertEqual(len(problems), 1)
        self.assertIn("found unhashable key", problems[0])

    def test_yaml_error_without_position(self) -> None:
        error = yaml.MarkedYAMLError(problem="broken")

        self.assertEqual(_format_yaml_error(error), "broken")

    def test_python_tags_are_rejected(self) -> None:
        problems = self.load_problems(
            self.write("name: !!python/object/apply:os.getcwd []\n")
        )

        self.assertEqual(len(problems), 1)
        self.assertIn("could not determine a constructor", problems[0])

    def test_same_key_in_sibling_mappings_is_allowed(self) -> None:
        config = load_dataset_config(self.write(VALID_YAML))

        self.assertEqual(len(config.columns), 2)

    def test_empty_file(self) -> None:
        for text in ["", "# only a comment\n"]:
            with self.subTest(text=text):
                self.assertEqual(
                    self.load_problems(self.write(text)), ("file is empty",)
                )

    def test_top_level_not_a_mapping(self) -> None:
        self.assertEqual(
            self.load_problems(self.write("- a\n- b\n")),
            ("top level must be a mapping, not list",),
        )

    def test_field_error_has_dotted_path(self) -> None:
        text = VALID_YAML.replace("type: float", "type: decimal")

        problems = self.load_problems(self.write(text))

        self.assertEqual(len(problems), 1)
        self.assertTrue(problems[0].startswith("columns[1].type: "))

    def test_column_errors_are_prefixed_with_column_path(self) -> None:
        text = VALID_YAML.replace('    format: "%Y-%m-%d"\n', "")

        self.assertEqual(
            self.load_problems(self.write(text)),
            ("columns[0]: 'format' is required on date columns",),
        )

    def test_source_error_is_prefixed_with_source(self) -> None:
        text = VALID_YAML.replace(
            "  url: https://example.org/weather.csv\n",
            "  url: https://example.org/weather.csv\n  path: weather.csv\n",
        )

        self.assertEqual(
            self.load_problems(self.write(text)),
            ("source: set exactly one of 'url' or 'path'",),
        )

    def test_config_errors_are_split_one_per_line(self) -> None:
        text = VALID_YAML.replace(
            "  partition_by: observation_date\n",
            "  partition_by: missing\n  cluster_by: [temperature_c]\n",
        )

        self.assertEqual(
            self.load_problems(self.write(text)),
            (
                "target.partition_by: 'missing' is not a column",
                "target.cluster_by: 'temperature_c' must not be a float "
                "column",
            ),
        )

    def test_every_field_error_is_reported(self) -> None:
        text = VALID_YAML.replace("name: city_weather", "name: City").replace(
            "type: float", "type: decimal"
        )

        problems = self.load_problems(self.write(text))

        locations = [problem.split(":")[0] for problem in problems]
        self.assertIn("name", locations)
        self.assertIn("columns[1].type", locations)

    def test_message_prefixes_every_line_with_the_file(self) -> None:
        text = VALID_YAML.replace(
            "  partition_by: observation_date\n",
            "  partition_by: missing\n  cluster_by: [temperature_c]\n",
        )
        path = self.write(text)

        with self.assertRaises(DatasetConfigError) as context:
            load_dataset_config(path)

        lines = str(context.exception).splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertTrue(line.startswith(f"{path}: target."))


class ExampleDatasetConfigTest(unittest.TestCase):
    def test_example_is_valid(self) -> None:
        config = load_dataset_config(EXAMPLE_PATH)

        self.assertEqual(
            {column.type for column in config.columns}, set(ColumnType)
        )

    def test_example_documents_every_option(self) -> None:
        text = EXAMPLE_PATH.read_text(encoding="utf-8")

        for model in [DatasetConfig, Source, Column, Rules, Target]:
            for field in model.model_fields:
                with self.subTest(model=model.__name__, field=field):
                    self.assertIn(f"{field}:", text)


class CommittedDatasetConfigsTest(unittest.TestCase):
    def test_every_dataset_file_is_valid(self) -> None:
        paths = sorted(EXAMPLE_PATH.parent.glob("*.yaml"))
        self.assertTrue(
            {"example", "usgs_earthquakes", "wa_ev_population"}
            <= {path.stem for path in paths}
        )

        for path in paths:
            with self.subTest(path=path.name):
                load_dataset_config(path)
