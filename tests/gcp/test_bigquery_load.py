"""Loads into a real BigQuery sandbox; run with pytest -m gcp."""

import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import google.auth
import polars as pl
import pytest
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from config_driven_etl.bq_schema import table_spec
from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.extract import scan_raw_csv
from config_driven_etl.load import (
    bigquery_table_id,
    ensure_table,
    load_parquet,
    parquet_row_count,
    processed_parquet_path,
    verify_parquet,
    verify_table_rows,
    write_parquet,
)
from config_driven_etl.settings import Settings
from config_driven_etl.transform import output_schema, transform
from fixture_outcomes import OUTCOMES, fixture_config


def sandbox() -> tuple[bigquery.Client, str] | None:
    """A client and dataset id, or None without settings or credentials."""
    settings = Settings()
    if settings.gcp_project_id is None or settings.bq_dataset is None:
        return None
    try:
        credentials, _ = google.auth.default()
    except DefaultCredentialsError:
        return None
    client = bigquery.Client(
        project=settings.gcp_project_id, credentials=credentials
    )
    return client, settings.bq_dataset


def recent_partitions(
    frame: pl.DataFrame, config: DatasetConfig
) -> pl.DataFrame:
    """Shift the partition column so its latest value is yesterday.

    The sandbox drops partitions older than 60 days at load time, and the
    fixtures are dated early 2026; relative gaps between values are kept.
    """
    column = config.target.partition_by
    if column is None or frame.is_empty():
        return frame
    latest = frame[column].max()
    assert isinstance(latest, date)
    yesterday = date.today() - timedelta(days=1)
    shift = yesterday - (
        latest.date() if isinstance(latest, datetime) else latest
    )
    return frame.with_columns(pl.col(column) + shift)


@pytest.mark.gcp
class BigQueryLoadTest(unittest.TestCase):
    def setUp(self) -> None:
        found = sandbox()
        if found is None:
            self.skipTest("GCP_PROJECT_ID, BQ_DATASET or credentials missing")
        self.client, self.dataset = found
        self.addCleanup(self.client.close)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.data_dir = Path(directory.name)

    def write_fixture(self, name: str) -> tuple[Path, str]:
        """Transform a fixture into Parquet; return it and a new table id."""
        config = fixture_config(name)
        source = config.source
        assert source.path is not None
        valid = transform(scan_raw_csv(source.path, source), config)
        frame = recent_partitions(valid.valid.collect(), config)
        path = processed_parquet_path(self.data_dir, name)
        write_parquet(frame, path)
        verify_parquet(path, output_schema(config), frame.height)
        # A unique name keeps concurrent runs and leftovers apart.
        table = f"{config.target.table}_test_{uuid.uuid4().hex[:12]}"
        table_id = bigquery_table_id(self.client.project, self.dataset, table)
        self.addCleanup(self.client.delete_table, table_id, not_found_ok=True)
        return path, table_id

    def test_fixtures_load_and_reload_to_the_same_state(self) -> None:
        for name in OUTCOMES:
            with self.subTest(dataset=name):
                config = fixture_config(name)
                path, table_id = self.write_fixture(name)
                rows = parquet_row_count(path)
                expected = pl.read_parquet(path)
                spec = table_spec(config)

                for _ in range(2):
                    ensure_table(self.client, table_id, spec)
                    load_parquet(self.client, path, table_id, spec)
                    verify_table_rows(self.client, table_id, rows)

                    # list_rows reads table data without a billed query.
                    loaded = pl.from_arrow(
                        self.client.list_rows(table_id).to_arrow(
                            create_bqstorage_client=False
                        )
                    )
                    assert isinstance(loaded, pl.DataFrame)
                    key = expected.columns[0]
                    self.assertTrue(
                        loaded.sort(key).equals(expected.sort(key)),
                        f"{loaded}\n!=\n{expected}",
                    )


if __name__ == "__main__":
    unittest.main()
