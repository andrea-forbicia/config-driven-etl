import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import polars as pl
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from config_driven_etl.dataset_config import (
    DatasetConfig,
    load_dataset_config,
)
from config_driven_etl.extract import ExtractError, raw_csv_path
from config_driven_etl.load import LoadError, processed_parquet_path
from config_driven_etl.pipeline import (
    StageResult,
    extract_dataset,
    load_dataset,
    transform_dataset,
)

FIXTURES = Path(__file__).parents[1] / "fixtures"


def fixture_config(name: str) -> DatasetConfig:
    return load_dataset_config(FIXTURES / name / f"{name}.yaml")


def fake_client(table_rows: int) -> mock.MagicMock:
    """A client whose table is missing, then holds table_rows rows."""
    client = mock.create_autospec(bigquery.Client, instance=True)
    client.project = "project"
    table = mock.create_autospec(bigquery.Table, instance=True)
    table.num_rows = table_rows
    table.time_partitioning = None
    client.get_table.side_effect = [NotFound("no table yet"), table]
    return client


class PipelineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.data_dir = Path(directory.name)
        self.session = mock.create_autospec(requests.Session, instance=True)


class ExtractDatasetTest(PipelineTestCase):
    def test_copies_a_local_source_and_counts_rows(self) -> None:
        config = fixture_config("library_loans")

        result = extract_dataset(config, self.data_dir, self.session)

        self.assertEqual(result, StageResult(rows_in=None, rows_out=12))
        self.assertTrue(raw_csv_path(self.data_dir, config.name).is_file())
        self.session.get.assert_not_called()

    def test_downloads_a_url_source(self) -> None:
        config = fixture_config("library_loans").model_copy(
            update={
                "source": fixture_config("library_loans").source.model_copy(
                    update={"url": "https://example.org/x.csv", "path": None}
                )
            }
        )
        csv = FIXTURES / "library_loans" / "library_loans.csv"

        def download(url: str, destination: Path, session: object) -> None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(csv, destination)

        with mock.patch(
            "config_driven_etl.pipeline.download_csv", side_effect=download
        ) as download_csv:
            result = extract_dataset(config, self.data_dir, self.session)

        download_csv.assert_called_once_with(
            "https://example.org/x.csv",
            raw_csv_path(self.data_dir, config.name),
            self.session,
        )
        self.assertEqual(result.rows_out, 12)

    def test_unparsable_body_is_an_extract_error(self) -> None:
        directory = self.data_dir / "source"
        directory.mkdir()
        (directory / "bad.csv").write_bytes(b"a,b\n1,2\n1,2,3,4\n")
        (directory / "bad.yaml").write_text(
            "name: bad\nsource:\n  path: bad.csv\ncolumns:\n"
            "  - {source: a, name: a, type: string}\n"
            "target:\n  table: bad\n",
            encoding="utf-8",
        )
        config = load_dataset_config(directory / "bad.yaml")

        with self.assertRaises(ExtractError) as context:
            extract_dataset(config, self.data_dir, self.session)

        self.assertIn("cannot read CSV", str(context.exception))


class TransformDatasetTest(PipelineTestCase):
    def test_writes_verified_parquet_and_counts_rejections(self) -> None:
        config = fixture_config("sensor_readings")
        extract_dataset(config, self.data_dir, self.session)

        result = transform_dataset(config, self.data_dir)

        self.assertEqual(
            (result.rows_in, result.rows_out, result.rows_rejected),
            (11, 4, 7),
        )
        assert result.rejections is not None
        self.assertEqual(sum(result.rejections.values()), 7)
        self.assertEqual(result.rejections["reading_id: duplicate"], 1)
        path = processed_parquet_path(self.data_dir, config.name)
        self.assertEqual(pl.read_parquet(path).height, 4)

    def test_missing_raw_file_is_an_extract_error(self) -> None:
        with self.assertRaises(ExtractError):
            transform_dataset(fixture_config("sensor_readings"), self.data_dir)


class LoadDatasetTest(PipelineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = fixture_config("library_loans")
        extract_dataset(self.config, self.data_dir, self.session)
        transform_dataset(self.config, self.data_dir)

    def test_loads_the_parquet_into_the_target_table(self) -> None:
        client = fake_client(table_rows=4)

        result = load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertEqual(result, StageResult(rows_in=4, rows_out=4))
        args, _ = client.load_table_from_file.call_args
        self.assertEqual(args[1], "project.dataset.library_loans")

    def test_row_count_mismatch_fails(self) -> None:
        client = fake_client(table_rows=3)

        with self.assertRaisesRegex(LoadError, "3 rows loaded, expected 4"):
            load_dataset(self.config, self.data_dir, client, "dataset")


if __name__ == "__main__":
    unittest.main()
