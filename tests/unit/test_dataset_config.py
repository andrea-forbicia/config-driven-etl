import unittest
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from config_driven_etl.dataset_config import (
    Column,
    ColumnType,
    DatasetConfig,
)


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
                column = Column.model_validate(
                    {"source": "x", "name": "x", "type": column_type.value}
                )
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
