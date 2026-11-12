import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from config_driven_etl.load import processed_parquet_path, write_parquet


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


if __name__ == "__main__":
    unittest.main()
