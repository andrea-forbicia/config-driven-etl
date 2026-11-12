import unittest
from typing import Any

import polars as pl

from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.transform import (
    REJECTIONS,
    TransformError,
    cast_columns,
    select_columns,
)


def typed_config(types: dict[str, str]) -> DatasetConfig:
    """Config whose source headers equal the target names."""
    columns = [
        {"source": name, "name": name, "type": type_}
        | ({"format": "%Y-%m-%d"} if type_ in ("date", "datetime") else {})
        for name, type_ in types.items()
    ]
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
        for type_ in ("int", "float", "bool"):
            with self.subTest(type=type_):
                result = cast({"x": type_}, {"x": [None, "", "   "]})

                self.assertEqual(result["x"].to_list(), [None] * 3)
                self.assertEqual(result[REJECTIONS].to_list(), [[]] * 3)

    def test_string_values_are_kept_as_written(self) -> None:
        values = ["  padded ", "", None]
        result = cast({"s": "string"}, {"s": values})

        self.assertEqual(result["s"].to_list(), values)

    def test_temporal_and_unmapped_columns_stay_strings(self) -> None:
        lf = raw_frame({"d": ["2026-01-01"], "extra": ["5"]})

        result = cast_columns(lf, typed_config({"d": "date"})).collect()

        self.assertEqual(result["d"].to_list(), ["2026-01-01"])
        self.assertEqual(result["extra"].to_list(), ["5"])
        self.assertEqual(result[REJECTIONS].to_list(), [[]])

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
        result = cast({"i": "int", "b": "bool"}, {"i": [], "b": []})

        self.assertEqual(result.height, 0)
        self.assertEqual(result.schema["i"], pl.Int64)
        self.assertEqual(result.schema["b"], pl.Boolean)
        self.assertEqual(result.schema[REJECTIONS], pl.List(pl.String))

    def test_does_not_collect_the_input(self) -> None:
        lf = raw_frame({"i": ["1"]})

        result = cast_columns(lf, typed_config({"i": "int"}))

        self.assertIsInstance(result, pl.LazyFrame)


if __name__ == "__main__":
    unittest.main()
