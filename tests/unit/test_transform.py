import unittest
from typing import Any

import polars as pl

from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.transform import TransformError, select_columns


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


if __name__ == "__main__":
    unittest.main()
