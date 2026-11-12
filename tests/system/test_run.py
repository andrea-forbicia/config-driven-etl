"""The run command end to end on fixture datasets, BigQuery faked."""

import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from typing import IO, Any
from unittest import mock

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from typer.testing import CliRunner, Result

from config_driven_etl.cli import app
from config_driven_etl.dataset_config import load_dataset_config
from config_driven_etl.transform import output_schema

FIXTURES = Path(__file__).parents[1] / "fixtures"


class FakeBigQuery:
    """In-memory stand-in for the few Client calls the pipeline makes.

    Loaded Parquet files are read back, so row counts and data are real.
    """

    project = "project"

    def __init__(self) -> None:
        self.tables: dict[str, bigquery.Table] = {}
        self.data: dict[str, pl.DataFrame] = {}
        self.job_configs: list[bigquery.LoadJobConfig] = []

    def __enter__(self) -> "FakeBigQuery":
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def get_table(self, table_id: str) -> bigquery.Table:
        if table_id not in self.tables:
            raise NotFound(f"Table {table_id} not found")
        table = self.tables[table_id]
        table._properties["numRows"] = str(self.data[table_id].height)
        return table

    def create_table(
        self, table: bigquery.Table, exists_ok: bool = False
    ) -> bigquery.Table:
        table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
        self.tables.setdefault(table_id, table)
        self.data.setdefault(table_id, pl.DataFrame())
        return table

    def load_table_from_file(
        self,
        file: IO[bytes],
        table_id: str,
        job_config: bigquery.LoadJobConfig,
    ) -> mock.MagicMock:
        self.job_configs.append(job_config)
        # WRITE_TRUNCATE: the file replaces whatever the table held.
        self.data[table_id] = pl.read_parquet(io.BytesIO(file.read()))
        return mock.create_autospec(bigquery.LoadJob, instance=True)


class RunCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        # Settings reads .env from the working directory: never the repo's.
        self.addCleanup(os.chdir, Path.cwd())
        os.chdir(self.directory)
        environment = mock.patch.dict(
            os.environ,
            {
                "DATA_DIR": str(self.directory / "data"),
                "GCP_PROJECT_ID": "project",
                "BQ_DATASET": "dataset",
            },
            clear=True,
        )
        environment.start()
        self.addCleanup(environment.stop)
        self.bigquery = FakeBigQuery()
        factory = mock.patch(
            "config_driven_etl.cli._bigquery_client",
            return_value=self.bigquery,
        )
        factory.start()
        self.addCleanup(factory.stop)

    def run_dataset(self, name: str) -> Result:
        dataset = FIXTURES / name / f"{name}.yaml"
        result = CliRunner().invoke(app, ["run", str(dataset)])
        self.assertEqual(result.exit_code, 0, result.output)
        return result

    def parquet(self, name: str) -> pl.DataFrame:
        path = self.directory / "data" / "processed" / name / f"{name}.parquet"
        return pl.read_parquet(path)

    def events(self, result: Result) -> list[dict[str, Any]]:
        return [json.loads(line) for line in result.stderr.splitlines()]

    def assert_parquet_matches_config(self, name: str, rows: int) -> None:
        config = load_dataset_config(FIXTURES / name / f"{name}.yaml")
        frame = self.parquet(name)
        self.assertEqual(frame.schema, output_schema(config))
        self.assertEqual(frame.height, rows)
        loaded = self.bigquery.data[f"project.dataset.{config.target.table}"]
        self.assertTrue(loaded.equals(frame))

    def test_sensor_readings(self) -> None:
        result = self.run_dataset("sensor_readings")

        self.assert_parquet_matches_config("sensor_readings", rows=4)
        self.assertEqual(
            self.parquet("sensor_readings").rows(),
            [
                ("r001", "s1", datetime(2026, 1, 1, 0), 21.5, 40, True),
                ("r002", "s1", datetime(2026, 1, 1, 1), 22.0, 41, False),
                ("r003", "s2", datetime(2026, 1, 1, 0), None, 55, True),
                ("r010", "s5", datetime(2026, 1, 2, 2), -50.0, 0, False),
            ],
        )
        transform = self.events(result)[1]
        self.assertEqual(
            (transform["rows_in"], transform["rows_out"]), (11, 4)
        )
        self.assertEqual(transform["rows_rejected"], 7)
        (job_config,) = self.bigquery.job_configs
        partitioning = job_config.time_partitioning
        assert partitioning is not None
        self.assertEqual(partitioning.field, "measured_at")
        self.assertEqual(job_config.clustering_fields, ["sensor_id"])

    def test_library_loans(self) -> None:
        result = self.run_dataset("library_loans")

        self.assert_parquet_matches_config("library_loans", rows=4)
        frame = self.parquet("library_loans")
        self.assertEqual(frame["loan_id"].to_list(), [1001, 1002, 1005, 1010])
        self.assertEqual(frame["branch_code"].to_list()[0], "007")
        self.assertEqual(frame["title"].to_list()[1], "Guerra; pace")
        self.assertEqual(frame["title"].to_list()[2], "Caf� society")
        self.assertIsNone(frame["title"].to_list()[3])
        self.assertEqual(frame["loan_date"].to_list()[0], date(2026, 2, 3))
        transform = self.events(result)[1]
        self.assertEqual(
            (transform["rows_in"], transform["rows_out"]), (12, 4)
        )
        self.assertEqual(transform["rejections"]["loan_date: invalid date"], 2)
        (job_config,) = self.bigquery.job_configs
        self.assertIsNone(job_config.time_partitioning)

    def test_second_run_leaves_the_same_state(self) -> None:
        self.run_dataset("library_loans")
        first = self.parquet("library_loans")
        first_loaded = self.bigquery.data["project.dataset.library_loans"]

        self.run_dataset("library_loans")

        self.assertTrue(self.parquet("library_loans").equals(first))
        self.assertTrue(
            self.bigquery.data["project.dataset.library_loans"].equals(
                first_loaded
            )
        )
        self.assertEqual(len(self.bigquery.tables), 1)

    def test_logs_one_line_per_stage(self) -> None:
        result = self.run_dataset("sensor_readings")

        self.assertEqual(
            [event["stage"] for event in self.events(result)],
            ["extract", "transform", "load"],
        )


if __name__ == "__main__":
    unittest.main()
