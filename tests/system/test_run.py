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
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from typer.testing import CliRunner, Result

from config_driven_etl.bq_schema import bigquery_rejected_schema
from config_driven_etl.cli import app
from config_driven_etl.transform import output_schema, rejected_schema
from fixture_outcomes import OUTCOMES, fixture_config, fixture_path


class FakeBigQuery:
    """In-memory stand-in for the few Client calls the pipeline makes.

    Loaded Parquet files are read back, so row counts and data are real.
    """

    project = "project"

    def __init__(self) -> None:
        self.tables: dict[str, bigquery.Table] = {}
        self.data: dict[str, pl.DataFrame] = {}
        self.job_configs: list[bigquery.LoadJobConfig] = []
        # Rows appended with load_table_from_json, per table.
        self.appended: dict[str, list[dict[str, Any]]] = {}

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

    def load_table_from_json(
        self,
        rows: list[dict[str, Any]],
        table_id: str,
        job_config: bigquery.LoadJobConfig,
    ) -> mock.MagicMock:
        assert job_config.schema is not None
        names = [field.name for field in job_config.schema]
        for row in rows:
            # What BigQuery would receive: JSON with the schema's fields.
            assert set(json.loads(json.dumps(row))) == set(names), row
        self.appended.setdefault(table_id, []).extend(rows)
        job = mock.create_autospec(bigquery.LoadJob, instance=True)
        job.output_rows = len(rows)
        return job


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
        dataset = fixture_path(name)
        result = CliRunner().invoke(app, ["run", str(dataset)])
        self.assertEqual(result.exit_code, 0, result.output)
        return result

    def gated_dataset(
        self, name: str, csv: Path | None = None, **rules: object
    ) -> Path:
        """A copy of a fixture's YAML with rules set, and maybe its CSV."""
        config = fixture_config(name)
        updated = config.rules.model_copy(update=rules)
        # The loaded source path is absolute, so the copy works anywhere.
        gated = config.model_copy(update={"rules": updated})
        if csv is not None:
            source = config.source.model_copy(update={"path": csv})
            gated = gated.model_copy(update={"source": source})
        path = self.directory / f"{name}_gated.yaml"
        path.write_text(
            yaml.safe_dump(gated.model_dump(mode="json", exclude_none=True)),
            encoding="utf-8",
        )
        return path

    def summary(self, result: Result) -> dict[str, Any]:
        """The run_finished line, without the log envelope."""
        line = json.loads(result.stderr.splitlines()[-1])
        for key in ["time", "level", "event"]:
            del line[key]
        return line

    def parquet(self, name: str) -> pl.DataFrame:
        path = self.directory / "data" / "processed" / name / f"{name}.parquet"
        return pl.read_parquet(path)

    def rejected(self, name: str) -> pl.DataFrame:
        directory = self.directory / "data" / "processed" / name
        return pl.read_parquet(directory / f"{name}.rejected.parquet")

    def rejected_row(self, name: str, row_number: int) -> dict[str, Any]:
        rows = self.rejected(name).filter(pl.col("row_number") == row_number)
        (row,) = rows.rows(named=True)
        return row

    def events(self, result: Result) -> list[dict[str, Any]]:
        """The events before the run_finished line every command ends with."""
        lines = result.stderr.splitlines()
        *events, summary = [json.loads(line) for line in lines]
        self.assertEqual(summary["event"], "run_finished")
        return events

    def assert_matches_outcome(self, name: str, result: Result) -> None:
        """Parquet, loaded table and transform event match the fixture."""
        config = fixture_config(name)
        outcome = OUTCOMES[name]
        frame = self.parquet(name)
        self.assertEqual(frame.schema, output_schema(config))
        self.assertEqual(frame.height, outcome.rows_valid)
        rejected = self.rejected(name)
        self.assertEqual(rejected.schema, rejected_schema(config))
        self.assertEqual(rejected.height, outcome.rows_rejected)
        transform = self.events(result)[1]
        self.assertEqual(
            (
                transform["rows_in"],
                transform["rows_out"],
                transform["rows_rejected"],
            ),
            (outcome.rows_in, outcome.rows_valid, outcome.rows_rejected),
        )
        self.assertEqual(transform["rejections"], outcome.rejections)
        loaded = self.bigquery.data[f"project.dataset.{config.target.table}"]
        self.assertTrue(loaded.equals(frame))

    def test_sensor_readings(self) -> None:
        result = self.run_dataset("sensor_readings")

        self.assert_matches_outcome("sensor_readings", result)
        self.assertEqual(
            self.parquet("sensor_readings").rows(),
            [
                ("r001", "s1", datetime(2026, 1, 1, 0), 21.5, 40, True),
                ("r002", "s1", datetime(2026, 1, 1, 1), 22.0, 41, False),
                ("r003", "s2", datetime(2026, 1, 1, 0), None, 55, True),
                ("r010", "s5", datetime(2026, 1, 2, 2), -50.0, 0, False),
                ("r011", "s5", datetime(2026, 1, 2, 3), None, 61, True),
            ],
        )
        self.assertEqual(
            self.rejected_row("sensor_readings", 7),
            {
                "row_number": 7,
                "reasons": ["humidity_pct: invalid int"],
                "raw": {
                    "reading_id": "r007",
                    "sensor_id": "s3",
                    "measured_at": "2026-01-01T04:00:00",
                    "temperature_c": "18.4",
                    "humidity_pct": "abc",
                    "is_online": "true",
                },
            },
        )
        (job_config,) = self.bigquery.job_configs
        partitioning = job_config.time_partitioning
        assert partitioning is not None
        self.assertEqual(partitioning.field, "measured_at")
        self.assertEqual(job_config.clustering_fields, ["sensor_id"])

    def test_library_loans(self) -> None:
        result = self.run_dataset("library_loans")

        self.assert_matches_outcome("library_loans", result)
        frame = self.parquet("library_loans")
        self.assertEqual(frame["loan_id"].to_list(), [1001, 1002, 1005, 1010])
        self.assertEqual(frame["branch_code"].to_list()[0], "007")
        self.assertEqual(frame["title"].to_list()[1], "Guerra; pace")
        self.assertEqual(frame["title"].to_list()[2], "Caf� society")
        self.assertIsNone(frame["title"].to_list()[3])
        self.assertEqual(frame["loan_date"].to_list()[0], date(2026, 2, 3))
        self.assertEqual(
            self.rejected_row("library_loans", 4)["raw"]["title"],
            "Dune\nPart Two",
        )
        # Row 4 spans two lines, so this record is on line 8 of the file.
        self.assertEqual(
            self.rejected_row("library_loans", 6),
            {
                "row_number": 6,
                "reasons": ["fine_eur: below min"],
                "raw": {
                    "loan_id": "1006",
                    "branch_code": "031",
                    "title": "Emma",
                    "loan_date": "09/02/2026",
                    "due_date": "23/02/2026",
                    "fine_eur": "-1.00",
                    "renewals": "0",
                },
            },
        )
        loaded = self.bigquery.data["project.dataset.library_loans_rejected"]
        self.assertTrue(loaded.equals(self.rejected("library_loans")))
        valid_job, rejected_job = self.bigquery.job_configs
        self.assertIsNone(valid_job.time_partitioning)
        self.assertEqual(
            rejected_job.schema,
            bigquery_rejected_schema(fixture_config("library_loans")),
        )

    def test_second_run_leaves_the_same_state(self) -> None:
        self.run_dataset("library_loans")
        first = self.parquet("library_loans")
        first_rejected = self.rejected("library_loans")
        first_loaded = dict(self.bigquery.data)

        self.run_dataset("library_loans")

        self.assertTrue(self.parquet("library_loans").equals(first))
        self.assertTrue(self.rejected("library_loans").equals(first_rejected))
        self.assertEqual(
            sorted(self.bigquery.data),
            [
                "project.dataset.library_loans",
                "project.dataset.library_loans_rejected",
            ],
        )
        for table_id, frame in self.bigquery.data.items():
            with self.subTest(table=table_id):
                self.assertTrue(frame.equals(first_loaded[table_id]))

    def test_logged_rejected_file_is_the_rejected_parquet(self) -> None:
        result = self.run_dataset("library_loans")

        transform = self.events(result)[1]
        path = self.directory / "data" / transform["rejected_file"]
        self.assertTrue(
            pl.read_parquet(path).equals(self.rejected("library_loans"))
        )

    def test_quality_gate_leaves_the_tables_untouched(self) -> None:
        self.run_dataset("library_loans")
        loaded = dict(self.bigquery.data)
        jobs = len(self.bigquery.job_configs)
        outcome = OUTCOMES["library_loans"]
        limit = outcome.rows_rejected / outcome.rows_in / 2
        dataset = self.gated_dataset("library_loans", max_rejected_ratio=limit)

        result = CliRunner().invoke(app, ["run", str(dataset)])

        self.assertEqual(result.exit_code, 7, result.output)
        # Same data could also mean a second identical load: count jobs.
        self.assertEqual(len(self.bigquery.job_configs), jobs)
        self.assertEqual(sorted(self.bigquery.data), sorted(loaded))
        for table_id, frame in self.bigquery.data.items():
            with self.subTest(table=table_id):
                self.assertTrue(frame.equals(loaded[table_id]))
        directory = self.directory / "data" / "processed" / "library_loans"
        self.assertEqual(
            [path.name for path in directory.iterdir()],
            ["library_loans.rejected.parquet"],
        )
        self.assertEqual(
            self.rejected("library_loans").height, outcome.rows_rejected
        )

    def test_header_only_source_leaves_the_tables_untouched(self) -> None:
        self.run_dataset("library_loans")
        loaded = dict(self.bigquery.data)
        jobs = len(self.bigquery.job_configs)
        source = fixture_config("library_loans").source.path
        assert source is not None
        header = source.read_bytes().splitlines(keepends=True)[0]
        csv = self.directory / "header_only.csv"
        csv.write_bytes(header)
        dataset = self.gated_dataset(
            "library_loans", csv, max_rejected_ratio=0.01, min_rows=1
        )

        result = CliRunner().invoke(app, ["run", str(dataset)])

        # Without min_rows this would truncate both tables to zero rows.
        self.assertEqual(result.exit_code, 7, result.output)
        self.assertEqual(len(self.bigquery.job_configs), jobs)
        for table_id, frame in self.bigquery.data.items():
            with self.subTest(table=table_id):
                self.assertTrue(frame.equals(loaded[table_id]))
        summary = self.summary(result)
        self.assertEqual(summary["category"], "quality_gate")
        (failure,) = [
            json.loads(line)
            for line in result.stderr.splitlines()
            if '"run_failed"' in line
        ]
        self.assertIn("0 valid rows, below rules.min_rows 1", failure["error"])
        self.assertNotIn("max_rejected_ratio", failure["error"])

    def test_audit_table_gets_one_row_per_run(self) -> None:
        os.environ["BQ_AUDIT_TABLE"] = "etl_runs"
        gated = self.gated_dataset("library_loans", max_rejected_ratio=0.0)

        runs = [
            self.run_dataset("sensor_readings"),
            CliRunner().invoke(app, ["run", str(gated)]),
            self.run_dataset("sensor_readings"),
        ]

        self.assertEqual([run.exit_code for run in runs], [0, 7, 0])
        rows = self.bigquery.appended["project.dataset.etl_runs"]
        self.assertEqual(rows, [self.summary(run) for run in runs])
        self.assertEqual(
            [(row["dataset"], row["outcome"]) for row in rows],
            [
                ("sensor_readings", "success"),
                ("library_loans", "failed"),
                ("sensor_readings", "success"),
            ],
        )
        self.assertEqual(len({row["run_id"] for row in rows}), 3)
        failed = rows[1]
        self.assertEqual(
            (failed["category"], failed["failed_stage"]),
            ("quality_gate", "transform"),
        )

    def test_logs_one_line_per_stage(self) -> None:
        result = self.run_dataset("sensor_readings")

        events = self.events(result)
        self.assertEqual(
            [event["stage"] for event in events],
            ["extract", "transform", "load"],
        )
        # One run_id ties the stage lines to the summary.
        self.assertEqual(len({event["run_id"] for event in events}), 1)


if __name__ == "__main__":
    unittest.main()
