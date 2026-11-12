import unittest
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

import polars as pl

from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.transform import (
    REJECTIONS,
    TransformError,
    cast_columns,
    check_nullable,
    select_columns,
)

TEMPORAL_FORMATS = {"date": "%Y-%m-%d", "datetime": "%Y-%m-%d %H:%M:%S"}


def typed_config(
    types: dict[str, str],
    formats: dict[str, str] | None = None,
    required: Iterable[str] = (),
) -> DatasetConfig:
    """Config whose source headers equal the target names.

    Temporal columns get the format from formats, or a default ISO one.
    Columns in required are not nullable.
    """
    formats = formats or {}
    columns = []
    for name, type_ in types.items():
        column: dict[str, Any] = {
            "source": name,
            "name": name,
            "type": type_,
            "nullable": name not in required,
        }
        if type_ in TEMPORAL_FORMATS:
            column["format"] = formats.get(name, TEMPORAL_FORMATS[type_])
        columns.append(column)
    return DatasetConfig.model_validate(
        {
            "name": "sample",
            "source": {"url": "https://example.org/sample.csv"},
            "columns": columns,
            "target": {"table": "sample"},
        }
    )


def make_config(
    columns: list[tuple[str, str]], drop_unmapped_columns: bool = True
) -> DatasetConfig:
    data: dict[str, Any] = {
        "name": "sample",
        "source": {"url": "https://example.org/sample.csv"},
        "columns": [
            {"source": source, "name": name, "type": "string"}
            for source, name in columns
        ],
        "rules": {"drop_unmapped_columns": drop_unmapped_columns},
        "target": {"table": "sample"},
    }
    return DatasetConfig.model_validate(data)


def raw_frame(data: dict[str, list[str | None]]) -> pl.LazyFrame:
    return pl.LazyFrame(data, schema=dict.fromkeys(data, pl.String))


class SelectColumnsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lf = raw_frame(
            {
                "Extra": ["x1", "x2"],
                "Second": ["b1", None],
                "First": ["a1", "a2"],
            }
        )

    def test_renames_mapped_columns_in_config_order(self) -> None:
        config = make_config([("First", "first"), ("Second", "second")])

        result = select_columns(self.lf, config).collect()

        self.assertEqual(result.columns, ["first", "second"])
        self.assertEqual(result["first"].to_list(), ["a1", "a2"])
        self.assertEqual(result["second"].to_list(), ["b1", None])

    def test_drops_unmapped_columns_by_default(self) -> None:
        config = make_config([("First", "first")])

        result = select_columns(self.lf, config).collect()

        self.assertEqual(result.columns, ["first"])

    def test_keeps_unmapped_columns_after_mapped_ones(self) -> None:
        config = make_config([("First", "first")], drop_unmapped_columns=False)

        result = select_columns(self.lf, config).collect()

        self.assertEqual(result.columns, ["first", "Extra", "Second"])
        self.assertEqual(result["Extra"].to_list(), ["x1", "x2"])

    def test_keeps_values_as_strings(self) -> None:
        config = make_config([("First", "first")])

        result = select_columns(self.lf, config).collect()

        self.assertEqual(result.schema["first"], pl.String)

    def test_fails_on_missing_mapped_columns(self) -> None:
        config = make_config(
            [("First", "first"), ("Gone", "gone"), ("Lost", "lost")]
        )

        with self.assertRaises(TransformError) as context:
            select_columns(self.lf, config)

        self.assertEqual(
            str(context.exception), "missing source column: 'Gone', 'Lost'"
        )

    def test_source_names_are_case_sensitive(self) -> None:
        config = make_config([("first", "first")])

        with self.assertRaises(TransformError):
            select_columns(self.lf, config)

    def test_fails_when_kept_column_clashes_with_target_name(self) -> None:
        lf = raw_frame({"First": ["a1"], "first": ["dup"]})
        config = make_config([("First", "first")], drop_unmapped_columns=False)

        with self.assertRaises(TransformError) as context:
            select_columns(lf, config)

        self.assertIn("'first'", str(context.exception))

    def test_dropped_column_may_share_a_target_name(self) -> None:
        lf = raw_frame({"First": ["a1"], "first": ["dup"]})
        config = make_config([("First", "first")])

        result = select_columns(lf, config).collect()

        self.assertEqual(result["first"].to_list(), ["a1"])

    def test_swapped_names_are_renamed_simultaneously(self) -> None:
        lf = raw_frame({"a": ["1"], "b": ["2"]})
        config = make_config([("a", "b"), ("b", "a")])

        result = select_columns(lf, config).collect()

        self.assertEqual(result.columns, ["b", "a"])
        self.assertEqual(result.row(0), ("1", "2"))

    def test_empty_input_keeps_columns(self) -> None:
        lf = raw_frame({"First": [], "Extra": []})
        config = make_config([("First", "first")])

        result = select_columns(lf, config).collect()

        self.assertEqual(result.columns, ["first"])
        self.assertEqual(result.height, 0)

    def test_does_not_collect_the_input(self) -> None:
        config = make_config([("First", "first")])

        result = select_columns(self.lf, config)

        self.assertIsInstance(result, pl.LazyFrame)


def cast(
    types: dict[str, str], data: dict[str, list[str | None]]
) -> pl.DataFrame:
    return cast_columns(raw_frame(data), typed_config(types)).collect()


class CastColumnsTest(unittest.TestCase):
    def test_casts_valid_values(self) -> None:
        result = cast(
            {"s": "string", "i": "int", "f": "float", "b": "bool"},
            {"s": ["a"], "i": ["-42"], "f": ["2.5"], "b": ["true"]},
        )

        self.assertEqual(
            result.schema,
            pl.Schema(
                {
                    "s": pl.String,
                    "i": pl.Int64,
                    "f": pl.Float64,
                    "b": pl.Boolean,
                    REJECTIONS: pl.List(pl.String),
                }
            ),
        )
        self.assertEqual(result.row(0), ("a", -42, 2.5, True, []))

    def test_int_values(self) -> None:
        cases = {
            "7": 7,
            "+7": 7,
            "-0": 0,
            " 7 ": 7,
            "9223372036854775807": 9223372036854775807,
        }
        result = cast({"i": "int"}, {"i": list(cases)})

        self.assertEqual(result["i"].to_list(), list(cases.values()))
        self.assertEqual(result[REJECTIONS].to_list(), [[]] * len(cases))

    def test_float_values(self) -> None:
        cases = {"1": 1.0, "-2.5": -2.5, ".5": 0.5, "1e3": 1000.0, "5.": 5.0}
        result = cast({"f": "float"}, {"f": list(cases)})

        self.assertEqual(result["f"].to_list(), list(cases.values()))
        self.assertEqual(result[REJECTIONS].to_list(), [[]] * len(cases))

    def test_bool_values_ignore_case(self) -> None:
        result = cast(
            {"b": "bool"}, {"b": ["true", "FALSE", " True ", "false"]}
        )

        self.assertEqual(result["b"].to_list(), [True, False, True, False])

    def test_malformed_values_become_null_with_a_reason(self) -> None:
        cases = {
            "int": ["abc", "1.0", "1e3", "1_000", "9223372036854775808"],
            "float": ["abc", "1,5", "nan", "inf", "-inf"],
            "bool": ["maybe", "yes", "1", "t"],
        }
        for type_, values in cases.items():
            for value in values:
                with self.subTest(type=type_, value=value):
                    result = cast({"x": type_}, {"x": [value]})

                    self.assertIsNone(result["x"][0])
                    self.assertEqual(
                        result[REJECTIONS][0].to_list(),
                        [f"x: invalid {type_}"],
                    )

    def test_null_and_blank_values_are_null_without_reason(self) -> None:
        for type_ in ("int", "float", "bool", "date", "datetime"):
            with self.subTest(type=type_):
                result = cast({"x": type_}, {"x": [None, "", "   "]})

                self.assertEqual(result["x"].to_list(), [None] * 3)
                self.assertEqual(result[REJECTIONS].to_list(), [[]] * 3)

    def test_string_values_are_kept_as_written(self) -> None:
        values = ["  padded ", "", None]
        result = cast({"s": "string"}, {"s": values})

        self.assertEqual(result["s"].to_list(), values)

    def test_unmapped_columns_stay_strings(self) -> None:
        lf = raw_frame({"i": ["1"], "extra": [" 5 "]})

        result = cast_columns(lf, typed_config({"i": "int"})).collect()

        self.assertEqual(result["extra"].to_list(), [" 5 "])

    def test_date_values_use_the_configured_format(self) -> None:
        cases = {
            "03/02/2026": date(2026, 2, 3),
            " 03/02/2026 ": date(2026, 2, 3),
            "29/02/2024": date(2024, 2, 29),
            "01/01/1000": date(1000, 1, 1),
        }
        result = cast_columns(
            raw_frame({"d": list(cases)}),
            typed_config({"d": "date"}, {"d": "%d/%m/%Y"}),
        ).collect()

        self.assertEqual(result.schema["d"], pl.Date)
        self.assertEqual(result["d"].to_list(), list(cases.values()))
        self.assertEqual(result[REJECTIONS].to_list(), [[]] * len(cases))

    def test_datetime_values_use_the_configured_format(self) -> None:
        cases = {
            "2026-01-01T00:00:00": datetime(2026, 1, 1),
            "2026-12-31T23:59:59": datetime(2026, 12, 31, 23, 59, 59),
        }
        result = cast_columns(
            raw_frame({"t": list(cases)}),
            typed_config({"t": "datetime"}, {"t": "%Y-%m-%dT%H:%M:%S"}),
        ).collect()

        self.assertEqual(result.schema["t"], pl.Datetime("us"))
        self.assertEqual(result["t"].to_list(), list(cases.values()))
        self.assertEqual(result[REJECTIONS].to_list(), [[]] * len(cases))

    def test_datetime_offsets_become_naive_utc(self) -> None:
        values: list[str | None] = [
            "2026-01-01T10:00:00+02:00",
            "2026-01-01T10:00:00-0500",
        ]
        result = cast_columns(
            raw_frame({"t": values}),
            typed_config({"t": "datetime"}, {"t": "%Y-%m-%dT%H:%M:%S%z"}),
        ).collect()

        self.assertEqual(result.schema["t"], pl.Datetime("us"))
        self.assertEqual(
            result["t"].to_list(),
            [datetime(2026, 1, 1, 8), datetime(2026, 1, 1, 15)],
        )

    def test_datetime_keeps_microseconds(self) -> None:
        result = cast_columns(
            raw_frame({"t": ["2026-01-01 00:00:00.123456"]}),
            typed_config({"t": "datetime"}, {"t": "%Y-%m-%d %H:%M:%S%.f"}),
        ).collect()

        self.assertEqual(
            result["t"].to_list(), [datetime(2026, 1, 1, 0, 0, 0, 123456)]
        )

    def test_malformed_temporal_values_become_null_with_a_reason(
        self,
    ) -> None:
        cases = {
            ("date", "%d/%m/%Y"): [
                "2026-02-03",
                "31/02/2026",
                "29/02/2026",
                "03/13/2026",
                "03/02/26",
                "03/02/999",
                "soon",
            ],
            ("datetime", "%Y-%m-%dT%H:%M:%S"): [
                "2026-01-01",
                "2026-01-01 00:00:00",
                "2026-01-01T25:00:00",
                "26-01-01T00:00:00",
                "2026-01-01T00:00:00Z",
            ],
        }
        for (type_, format_), values in cases.items():
            config = typed_config({"x": type_}, {"x": format_})
            for value in values:
                with self.subTest(type=type_, value=value):
                    result = cast_columns(
                        raw_frame({"x": [value]}), config
                    ).collect()

                    self.assertIsNone(result["x"][0])
                    self.assertEqual(
                        result[REJECTIONS][0].to_list(),
                        [f"x: invalid {type_}"],
                    )

    def test_collects_one_reason_per_failed_column(self) -> None:
        result = cast(
            {"i": "int", "f": "float", "b": "bool"},
            {"i": ["x", "1"], "f": ["y", "2"], "b": ["true", "z"]},
        )

        self.assertEqual(
            result[REJECTIONS].to_list(),
            [["i: invalid int", "f: invalid float"], ["b: invalid bool"]],
        )

    def test_keeps_existing_reasons(self) -> None:
        lf = raw_frame({"i": ["x", "1"]}).with_columns(
            pl.Series(REJECTIONS, [["earlier"], []])
        )

        result = cast_columns(lf, typed_config({"i": "int"})).collect()

        self.assertEqual(
            result[REJECTIONS].to_list(),
            [["earlier", "i: invalid int"], []],
        )

    def test_null_existing_reasons_are_extended(self) -> None:
        lf = raw_frame({"i": ["x"]}).with_columns(
            pl.Series(REJECTIONS, [None], dtype=pl.List(pl.String))
        )

        result = cast_columns(lf, typed_config({"i": "int"})).collect()

        self.assertEqual(result[REJECTIONS].to_list(), [["i: invalid int"]])

    def test_without_castable_columns_adds_empty_reasons(self) -> None:
        result = cast({"s": "string"}, {"s": ["a"]})

        self.assertEqual(result[REJECTIONS].to_list(), [[]])

    def test_empty_input(self) -> None:
        result = cast(
            {"i": "int", "b": "bool", "d": "date", "t": "datetime"},
            {"i": [], "b": [], "d": [], "t": []},
        )

        self.assertEqual(result.height, 0)
        self.assertEqual(result.schema["i"], pl.Int64)
        self.assertEqual(result.schema["b"], pl.Boolean)
        self.assertEqual(result.schema["d"], pl.Date)
        self.assertEqual(result.schema["t"], pl.Datetime("us"))
        self.assertEqual(result.schema[REJECTIONS], pl.List(pl.String))

    def test_does_not_collect_the_input(self) -> None:
        lf = raw_frame({"i": ["1"]})

        result = cast_columns(lf, typed_config({"i": "int"}))

        self.assertIsInstance(result, pl.LazyFrame)


def cast_and_check(
    types: dict[str, str],
    data: dict[str, list[str | None]],
    required: Iterable[str],
) -> pl.DataFrame:
    config = typed_config(types, required=required)
    return check_nullable(
        cast_columns(raw_frame(data), config), config
    ).collect()


class CheckNullableTest(unittest.TestCase):
    def test_rejects_nulls_in_required_columns(self) -> None:
        for type_ in ("string", "int", "float", "bool", "date", "datetime"):
            with self.subTest(type=type_):
                result = cast_and_check(
                    {"x": type_}, {"x": [None]}, required=["x"]
                )

                self.assertEqual(result[REJECTIONS].to_list(), [["x: null"]])

    def test_blank_values_count_as_null_after_casting(self) -> None:
        result = cast_and_check({"i": "int"}, {"i": ["  "]}, required=["i"])

        self.assertEqual(result[REJECTIONS].to_list(), [["i: null"]])

    def test_empty_strings_are_values(self) -> None:
        result = cast_and_check(
            {"s": "string"}, {"s": ["", "  "]}, required=["s"]
        )

        self.assertEqual(result[REJECTIONS].to_list(), [[], []])

    def test_nullable_columns_accept_nulls(self) -> None:
        result = cast_and_check(
            {"a": "int", "b": "string"},
            {"a": [None], "b": ["x"]},
            required=["b"],
        )

        self.assertEqual(result[REJECTIONS].to_list(), [[]])

    def test_invalid_value_is_rejected_once(self) -> None:
        result = cast_and_check({"i": "int"}, {"i": ["abc"]}, required=["i"])

        self.assertEqual(result[REJECTIONS].to_list(), [["i: invalid int"]])

    def test_invalid_value_in_another_column_does_not_hide_null(
        self,
    ) -> None:
        result = cast_and_check(
            {"a": "int", "b": "int"},
            {"a": ["abc"], "b": [None]},
            required=["a", "b"],
        )

        self.assertEqual(
            result[REJECTIONS].to_list(), [["a: invalid int", "b: null"]]
        )

    def test_one_reason_per_null_column(self) -> None:
        result = cast_and_check(
            {"a": "string", "b": "date"},
            {"a": [None, "x"], "b": [None, "2026-01-01"]},
            required=["a", "b"],
        )

        self.assertEqual(
            result[REJECTIONS].to_list(), [["a: null", "b: null"], []]
        )

    def test_does_not_change_values(self) -> None:
        result = cast_and_check(
            {"i": "int"}, {"i": ["1", None]}, required=["i"]
        )

        self.assertEqual(result["i"].to_list(), [1, None])

    def test_works_without_earlier_rejections(self) -> None:
        lf = raw_frame({"s": ["x", None]})
        config = typed_config({"s": "string"}, required=["s"])

        result = check_nullable(lf, config).collect()

        self.assertEqual(result[REJECTIONS].to_list(), [[], ["s: null"]])

    def test_empty_input(self) -> None:
        result = cast_and_check({"i": "int"}, {"i": []}, required=["i"])

        self.assertEqual(result.height, 0)
        self.assertEqual(result.schema[REJECTIONS], pl.List(pl.String))

    def test_does_not_collect_the_input(self) -> None:
        config = typed_config({"s": "string"}, required=["s"])

        result = check_nullable(raw_frame({"s": ["x"]}), config)

        self.assertIsInstance(result, pl.LazyFrame)


if __name__ == "__main__":
    unittest.main()
