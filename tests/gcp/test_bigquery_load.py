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

from config_driven_etl.bq_schema import (
    AUDIT_TABLE_SPEC,
    TableSpec,
    rejected_table_spec,
    table_spec,
)
from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.extract import scan_raw_csv
from config_driven_etl.load import (
    append_rows,
    bigquery_table_id,
    ensure_table,
    load_parquet,
    parquet_row_count,
    processed_parquet_path,
    rejected_parquet_path,
    verify_parquet,
    verify_table_rows,
    write_parquet,
)
from config_driven_etl.pipeline import (
    RunRecorder,
    StageResult,
    audit_row,
)
from config_driven_etl.settings import Settings
from config_driven_etl.transform import (
    output_schema,
    rejected_schema,
    transform,
)
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
        return path, self.temporary_table(config.target.table)

    def write_rejected(self, name: str, empty: bool = False) -> Path:
        """Transform a fixture and write its rejected rows as Parquet."""
        config = fixture_config(name)
        source = config.source
        assert source.path is not None
        result = transform(scan_raw_csv(source.path, source), config)
        frame = result.rejected.collect()
        if empty:
            frame = frame.head(0)
        path = rejected_parquet_path(self.data_dir, name)
        write_parquet(frame, path)
        verify_parquet(path, rejected_schema(config), frame.height)
        return path

    def temporary_table(self, base: str) -> str:
        """A new table id, deleted after the test."""
        # A unique name keeps concurrent runs and leftovers apart.
        table = f"{base}_test_{uuid.uuid4().hex[:12]}"
        table_id = bigquery_table_id(self.client.project, self.dataset, table)
        self.addCleanup(self.client.delete_table, table_id, not_found_ok=True)
        return table_id

    def read_table(self, table_id: str) -> pl.DataFrame:
        # list_rows reads table data without a billed query.
        loaded = pl.from_arrow(
            self.client.list_rows(table_id).to_arrow(
                create_bqstorage_client=False
            )
        )
        assert isinstance(loaded, pl.DataFrame)
        return loaded

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

                    loaded = self.read_table(table_id)
                    key = expected.columns[0]
                    self.assertTrue(
                        loaded.sort(key).equals(expected.sort(key)),
                        f"{loaded}\n!=\n{expected}",
                    )

    def test_rejected_rows_load_and_reload_to_the_same_state(self) -> None:
        for name in OUTCOMES:
            with self.subTest(dataset=name):
                spec = rejected_table_spec(fixture_config(name))
                path = self.write_rejected(name)
                table_id = self.temporary_table(f"{name}_rejected")
                rows = parquet_row_count(path)
                expected = pl.read_parquet(path).sort("row_number")
                self.assertGreater(rows, 0)

                for _ in range(2):
                    ensure_table(self.client, table_id, spec)
                    load_parquet(self.client, path, table_id, spec)
                    verify_table_rows(self.client, table_id, rows)

                    loaded = self.read_table(table_id).sort("row_number")
                    self.assertTrue(
                        loaded.equals(expected), f"{loaded}\n!=\n{expected}"
                    )

    def test_empty_rejected_file_empties_the_table(self) -> None:
        name = "library_loans"
        spec = rejected_table_spec(fixture_config(name))
        table_id = self.temporary_table(f"{name}_rejected")
        ensure_table(self.client, table_id, spec)
        path = self.write_rejected(name)
        load_parquet(self.client, path, table_id, spec)

        path = self.write_rejected(name, empty=True)
        load_parquet(self.client, path, table_id, spec)

        verify_table_rows(self.client, table_id, 0)

    def test_audit_rows_are_appended(self) -> None:
        table_id = self.temporary_table("etl_runs")
        rows = []
        for exit_code, category in [(0, None), (7, "quality_gate")]:
            recorder = RunRecorder("run", "0.0.0")
            recorder.dataset = "library_loans"
            recorder.start("transform")
            result = StageResult(
                rows_in=14,
                rows_out=4,
                rows_rejected=10,
                rejections={"loan_id: duplicate": 1, "fine_eur: below min": 1},
            )
            recorder.record("transform", result, 0.5, failed=exit_code != 0)
            rows.append(audit_row(recorder.finish(exit_code, category)))

        # Each append is a separate command, as in real runs.
        for row in rows:
            ensure_table(self.client, table_id, AUDIT_TABLE_SPEC)
            append_rows(self.client, table_id, AUDIT_TABLE_SPEC, [row])

        verify_table_rows(self.client, table_id, len(rows))
        table = self.read_table(table_id)
        self.assertEqual(table["started_at"].null_count(), 0)
        # TIMESTAMP values need tzdata to become Python objects on Windows.
        loaded = {
            row["run_id"]: row for row in table.drop("started_at").to_dicts()
        }
        for row in rows:
            with self.subTest(exit_code=row["exit_code"]):
                stored = loaded[row["run_id"]]
                self.assertEqual(
                    (stored["exit_code"], stored["category"]),
                    (row["exit_code"], row["category"]),
                )
                self.assertEqual(stored["stages"][0]["rows_rejected"], 10)
                self.assertEqual(
                    {
                        item["reason"]: item["rows"]
                        for item in stored["stages"][0]["rejections"]
                    },
                    {"loan_id: duplicate": 1, "fine_eur: below min": 1},
                )

    def test_older_audit_table_gains_the_new_fields(self) -> None:
        table_id = self.temporary_table("etl_runs")
        # The audit schema of v1.5.0, before stages.dropped_columns.
        older = TableSpec(
            schema=tuple(
                bigquery.SchemaField(
                    field.name,
                    field.field_type,
                    mode=field.mode,
                    fields=[
                        sub
                        for sub in field.fields
                        if sub.name != "dropped_columns"
                    ],
                )
                for field in AUDIT_TABLE_SPEC.schema
            ),
            partition_by=None,
            cluster_by=(),
        )
        ensure_table(self.client, table_id, older)
        recorder = RunRecorder("run", "0.0.0")
        recorder.start("transform")
        result = StageResult(
            rows_in=1, rows_out=1, dropped_columns=("Internal Note",)
        )
        recorder.record("transform", result, 0.5)
        row = audit_row(recorder.finish(0, None))

        append_rows(self.client, table_id, AUDIT_TABLE_SPEC, [row])

        (stored,) = self.read_table(table_id).drop("started_at").to_dicts()
        self.assertEqual(
            stored["stages"][0]["dropped_columns"], ["Internal Note"]
        )


if __name__ == "__main__":
    unittest.main()
