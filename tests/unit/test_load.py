import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest import mock

import polars as pl
import pyarrow.parquet as pq

from config_driven_etl.dataset_config import (
    DatasetConfig,
    load_dataset_config,
)
from config_driven_etl.extract import scan_raw_csv
from config_driven_etl.load import (
    LoadError,
    processed_parquet_path,
    verify_parquet,
    write_parquet,
)
from config_driven_etl.transform import transform

FIXTURES = Path(__file__).parents[1] / "fixtures"


def typed_frame() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "s": ["a", None],
            "i": [1, 2],
            "f": [1.5, None],
            "b": [True, False],
            "d": [date(2026, 1, 2), None],
            "t": [datetime(2026, 1, 2, 3, 4, 5, 6), None],
        },
        schema={
            "s": pl.String,
            "i": pl.Int64,
            "f": pl.Float64,
            "b": pl.Boolean,
            "d": pl.Date,
            "t": pl.Datetime("us"),
        },
    )


class ProcessedParquetPathTest(unittest.TestCase):
    def test_path_under_processed_directory(self) -> None:
        self.assertEqual(
            processed_parquet_path(Path("data"), "sample"),
            Path("data") / "processed" / "sample" / "sample.parquet",
        )


class WriteParquetTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dir = Path(directory.name)
        self.destination = self.dir / "processed" / "sample" / "x.parquet"

    def test_writes_every_row_and_type(self) -> None:
        write_parquet(typed_frame(), self.destination)

        written = pl.read_parquet(self.destination)
        self.assertEqual(written.schema, typed_frame().collect_schema())
        self.assertTrue(written.equals(typed_frame().collect()))

    def test_uses_snappy_compression(self) -> None:
        write_parquet(typed_frame(), self.destination)

        metadata = pq.ParquetFile(self.destination).metadata
        codecs = {
            metadata.row_group(group).column(column).compression
            for group in range(metadata.num_row_groups)
            for column in range(metadata.num_columns)
        }
        self.assertEqual(codecs, {"SNAPPY"})

    def test_replaces_the_previous_output(self) -> None:
        write_parquet(typed_frame(), self.destination)

        write_parquet(pl.LazyFrame({"other": [7]}), self.destination)

        self.assertEqual(pl.read_parquet(self.destination).rows(), [(7,)])

    def test_failed_write_keeps_the_previous_output(self) -> None:
        write_parquet(typed_frame(), self.destination)
        failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64))

        with self.assertRaises(pl.exceptions.PolarsError):
            write_parquet(failing, self.destination)

        self.assertEqual(pl.read_parquet(self.destination).height, 2)
        self.assertEqual(
            list(self.destination.parent.iterdir()), [self.destination]
        )

    def test_failed_first_write_leaves_no_file(self) -> None:
        failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64))

        with self.assertRaises(pl.exceptions.PolarsError):
            write_parquet(failing, self.destination)

        self.assertEqual(list(self.destination.parent.iterdir()), [])

    def test_empty_frame_keeps_its_schema(self) -> None:
        write_parquet(typed_frame().head(0), self.destination)

        written = pl.read_parquet(self.destination)
        self.assertEqual(written.height, 0)
        self.assertEqual(written.schema, typed_frame().collect_schema())


def typed_config() -> DatasetConfig:
    """Config whose output schema is the schema of typed_frame."""
    types = {
        "s": "string",
        "i": "int",
        "f": "float",
        "b": "bool",
        "d": "date",
        "t": "datetime",
    }
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


class VerifyParquetTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "x.parquet"

    def problems(self, lf: pl.LazyFrame, expected_rows: int = 2) -> list[str]:
        write_parquet(lf, self.path)
        with self.assertRaises(LoadError) as context:
            verify_parquet(self.path, typed_config(), expected_rows)
        prefix = f"{self.path}: "
        lines = str(context.exception).splitlines()
        self.assertTrue(all(line.startswith(prefix) for line in lines))
        return [line.removeprefix(prefix) for line in lines]

    def test_accepts_matching_file(self) -> None:
        write_parquet(typed_frame(), self.path)

        verify_parquet(self.path, typed_config(), expected_rows=2)

    def test_accepts_empty_file(self) -> None:
        write_parquet(typed_frame().head(0), self.path)

        verify_parquet(self.path, typed_config(), expected_rows=0)

    def test_row_count_mismatch(self) -> None:
        self.assertEqual(
            self.problems(typed_frame(), expected_rows=3),
            ["2 rows written, expected 3"],
        )

    def test_wrong_dtype(self) -> None:
        lf = typed_frame().with_columns(pl.col("i").cast(pl.Int32))

        self.assertEqual(
            self.problems(lf), ["column 'i' is Int32, expected Int64"]
        )

    def test_missing_column(self) -> None:
        self.assertEqual(
            self.problems(typed_frame().drop("b")), ["missing column 'b'"]
        )

    def test_columns_out_of_order(self) -> None:
        lf = typed_frame().select("i", "s", "f", "b", "d", "t")

        self.assertEqual(
            self.problems(lf), ["columns are not in config order"]
        )

    def test_unexpected_column(self) -> None:
        lf = typed_frame().with_columns(pl.lit("x").alias("Extra"))

        self.assertEqual(self.problems(lf), ["unexpected column 'Extra'"])

    def test_reports_every_problem(self) -> None:
        lf = typed_frame().drop("b").with_columns(pl.col("i").cast(pl.Int32))

        self.assertEqual(
            self.problems(lf, expected_rows=5),
            [
                "column 'i' is Int32, expected Int64",
                "missing column 'b'",
                "2 rows written, expected 5",
            ],
        )

    def test_missing_file(self) -> None:
        with self.assertRaises(LoadError) as context:
            verify_parquet(self.path, typed_config(), 0)

        self.assertIn("cannot read Parquet", str(context.exception))

    def test_error_without_message_names_its_type(self) -> None:
        error = pl.exceptions.ComputeError("")
        with (
            mock.patch.object(pl, "read_parquet_schema", side_effect=error),
            self.assertRaises(LoadError) as context,
        ):
            verify_parquet(self.path, typed_config(), 0)

        self.assertEqual(
            str(context.exception),
            f"{self.path}: cannot read Parquet: ComputeError",
        )

    def test_corrupt_file(self) -> None:
        self.path.write_bytes(b"not a parquet file")

        with self.assertRaises(LoadError) as context:
            verify_parquet(self.path, typed_config(), 0)

        self.assertIn("cannot read Parquet", str(context.exception))


class FixtureParquetTest(unittest.TestCase):
    """Exit criterion: each fixture writes Parquet matching its config."""

    def test_fixtures_write_verified_parquet(self) -> None:
        for name in ("sensor_readings", "library_loans"):
            with self.subTest(dataset=name):
                config = load_dataset_config(FIXTURES / name / f"{name}.yaml")
                source = config.source
                assert source.path is not None
                lf = scan_raw_csv(source.path, source)
                valid = transform(lf, config).valid.collect()
                with tempfile.TemporaryDirectory() as directory:
                    path = processed_parquet_path(Path(directory), name)

                    write_parquet(valid.lazy(), path)

                    verify_parquet(path, config, valid.height)
                    self.assertEqual(valid.height, 4)


if __name__ == "__main__":
    unittest.main()
