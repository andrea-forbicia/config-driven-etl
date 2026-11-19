import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import polars as pl
import requests
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from config_driven_etl.bq_schema import bigquery_rejected_schema
from config_driven_etl.dataset_config import (
    DatasetConfig,
    load_dataset_config,
)
from config_driven_etl.extract import ExtractError, raw_csv_path
from config_driven_etl.load import (
    LoadError,
    processed_parquet_path,
    rejected_parquet_path,
)
from config_driven_etl.pipeline import (
    StageResult,
    extract_dataset,
    load_dataset,
    transform_dataset,
)
from config_driven_etl.transform import rejected_schema
from fixture_outcomes import FIXTURES, OUTCOMES, fixture_config

LOANS = OUTCOMES["library_loans"]


VALID_TABLE = "project.dataset.library_loans"
REJECTED_TABLE = "project.dataset.library_loans_rejected"
NOT_LOADED = (
    f"rejected rows not loaded; {VALID_TABLE} already holds this run's "
    "valid rows"
)


def fake_client(*table_rows: int) -> mock.MagicMock:
    """A client whose tables, in load order, are missing, then hold rows.

    ensure_table finds every table missing, then verify_table_rows finds
    table_rows rows in each.
    """
    client = mock.create_autospec(bigquery.Client, instance=True)
    client.project = "project"
    answers: list[object] = [NotFound("no table yet") for _ in table_rows]
    for rows in table_rows:
        table = mock.create_autospec(bigquery.Table, instance=True)
        table.num_rows = rows
        table.time_partitioning = None
        answers.append(table)
    client.get_table.side_effect = answers
    return client


def without_rejected_table(config: DatasetConfig) -> DatasetConfig:
    target = config.target.model_copy(update={"rejected_table": None})
    return config.model_copy(update={"target": target})


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

        self.assertEqual(
            result, StageResult(rows_in=None, rows_out=LOANS.rows_in)
        )
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
        self.assertEqual(result.rows_out, LOANS.rows_in)

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
    def test_fixtures_match_their_expected_outcome(self) -> None:
        for name, outcome in OUTCOMES.items():
            with self.subTest(dataset=name):
                config = fixture_config(name)
                extract_dataset(config, self.data_dir, self.session)

                result = transform_dataset(config, self.data_dir)

                self.assertEqual(
                    (result.rows_in, result.rows_out, result.rows_rejected),
                    (
                        outcome.rows_in,
                        outcome.rows_valid,
                        outcome.rows_rejected,
                    ),
                )
                self.assertEqual(result.rejections, outcome.rejections)
                path = processed_parquet_path(self.data_dir, config.name)
                self.assertEqual(
                    pl.read_parquet(path).height, outcome.rows_valid
                )
                rejected = pl.read_parquet(
                    rejected_parquet_path(self.data_dir, config.name)
                )
                self.assertEqual(rejected.height, outcome.rows_rejected)
                self.assertEqual(rejected.schema, rejected_schema(config))

    def test_no_rejected_rows_writes_an_empty_rejected_file(self) -> None:
        config = fixture_config("library_loans")
        source = config.source
        assert source.path is not None
        # Bytes, so the copy keeps the fixture's encoding.
        header, first, *_ = source.path.read_bytes().split(b"\n")
        clean = self.data_dir / "clean.csv"
        clean.write_bytes(header + b"\n" + first + b"\n")
        config = config.model_copy(
            update={"source": source.model_copy(update={"path": clean})}
        )
        extract_dataset(config, self.data_dir, self.session)

        result = transform_dataset(config, self.data_dir)

        rejected = pl.read_parquet(
            rejected_parquet_path(self.data_dir, config.name)
        )
        self.assertEqual((result.rows_out, rejected.height), (1, 0))
        self.assertEqual(rejected.schema, rejected_schema(config))

    def test_output_off_its_config_keeps_the_previous_output(self) -> None:
        config = fixture_config("library_loans")
        extract_dataset(config, self.data_dir, self.session)
        transform_dataset(config, self.data_dir)
        path = processed_parquet_path(self.data_dir, config.name)
        previous = pl.read_parquet(path)
        expected = pl.Schema({**previous.schema, "extra": pl.String})

        with (
            mock.patch(
                "config_driven_etl.pipeline.output_schema",
                return_value=expected,
            ),
            self.assertRaises(LoadError) as context,
        ):
            transform_dataset(config, self.data_dir)

        self.assertIn(
            f"{path}: missing column 'extra'", str(context.exception)
        )
        self.assertTrue(pl.read_parquet(path).equals(previous))
        self.assertEqual(
            sorted(path.parent.iterdir()),
            [path, rejected_parquet_path(self.data_dir, config.name)],
        )

    def test_missing_raw_file_is_an_extract_error(self) -> None:
        with self.assertRaises(ExtractError):
            transform_dataset(fixture_config("sensor_readings"), self.data_dir)


class LoadDatasetTest(PipelineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = fixture_config("library_loans")
        extract_dataset(self.config, self.data_dir, self.session)
        transform_dataset(self.config, self.data_dir)

    def loaded_tables(self, client: mock.MagicMock) -> list[str]:
        return [
            args[1] for args, _ in client.load_table_from_file.call_args_list
        ]

    def test_loads_the_parquet_into_the_target_table(self) -> None:
        config = without_rejected_table(self.config)
        client = fake_client(LOANS.rows_valid)

        result = load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            result,
            StageResult(rows_in=LOANS.rows_valid, rows_out=LOANS.rows_valid),
        )
        self.assertEqual(self.loaded_tables(client), [VALID_TABLE])

    def test_without_rejected_table_the_rejected_file_is_unused(
        self,
    ) -> None:
        rejected_parquet_path(self.data_dir, self.config.name).unlink()
        client = fake_client(LOANS.rows_valid)

        load_dataset(
            without_rejected_table(self.config),
            self.data_dir,
            client,
            "dataset",
        )

        self.assertEqual(self.loaded_tables(client), [VALID_TABLE])

    def test_loads_the_rejected_rows_after_the_valid_ones(self) -> None:
        config = self.config
        client = fake_client(LOANS.rows_valid, LOANS.rows_rejected)

        result = load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            result,
            StageResult(
                rows_in=LOANS.rows_valid,
                rows_out=LOANS.rows_valid,
                rejected_rows_loaded=LOANS.rows_rejected,
            ),
        )
        self.assertEqual(
            self.loaded_tables(client), [VALID_TABLE, REJECTED_TABLE]
        )
        _, kwargs = client.load_table_from_file.call_args
        self.assertEqual(
            kwargs["job_config"].schema, bigquery_rejected_schema(config)
        )
        self.assertEqual(
            [args[0] for args, _ in client.get_table.call_args_list],
            [VALID_TABLE, REJECTED_TABLE, VALID_TABLE, REJECTED_TABLE],
        )

    def test_failed_rejected_load_says_the_valid_rows_are_loaded(
        self,
    ) -> None:
        client = fake_client(LOANS.rows_valid, LOANS.rows_rejected)
        valid_job = mock.create_autospec(bigquery.LoadJob, instance=True)
        rejected_job = mock.create_autospec(bigquery.LoadJob, instance=True)
        rejected_job.result.side_effect = BadRequest("bad rejected rows")
        client.load_table_from_file.side_effect = [valid_job, rejected_job]

        with self.assertRaises(LoadError) as context:
            load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines(),
            [
                f"{REJECTED_TABLE}: BigQuery error: 400 bad rejected rows",
                NOT_LOADED,
            ],
        )
        valid_job.result.assert_called_once_with()

    def test_rejected_row_count_mismatch_fails(self) -> None:
        loaded = LOANS.rows_rejected - 1
        client = fake_client(LOANS.rows_valid, loaded)

        with self.assertRaises(LoadError) as context:
            load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines(),
            [
                f"{REJECTED_TABLE}: {loaded} rows loaded, "
                f"expected {LOANS.rows_rejected}",
                NOT_LOADED,
            ],
        )

    def test_rejected_table_layout_is_checked_before_any_load(self) -> None:
        client = fake_client(LOANS.rows_valid, LOANS.rows_rejected)
        partitioned = mock.create_autospec(bigquery.Table, instance=True)
        partitioned.time_partitioning = bigquery.TimePartitioning(field="x")
        partitioned.range_partitioning = None
        partitioned.clustering_fields = None
        client.get_table.side_effect = [NotFound("no table yet"), partitioned]

        with self.assertRaises(LoadError) as context:
            load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines(),
            [
                f"{REJECTED_TABLE}: partitioned by x, config says nothing",
                f"{REJECTED_TABLE}: delete the table to recreate it",
            ],
        )
        client.load_table_from_file.assert_not_called()

    def test_missing_rejected_file_fails_before_any_load(self) -> None:
        path = rejected_parquet_path(self.data_dir, self.config.name)
        path.unlink()
        client = fake_client(LOANS.rows_valid, LOANS.rows_rejected)

        with self.assertRaises(LoadError) as context:
            load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertIn(f"{path}: cannot read Parquet", str(context.exception))
        client.get_table.assert_not_called()
        client.load_table_from_file.assert_not_called()

    def test_row_count_mismatch_fails(self) -> None:
        loaded = LOANS.rows_valid - 1
        client = fake_client(loaded)

        with self.assertRaisesRegex(
            LoadError, f"{loaded} rows loaded, expected {LOANS.rows_valid}"
        ):
            load_dataset(
                without_rejected_table(self.config),
                self.data_dir,
                client,
                "dataset",
            )


if __name__ == "__main__":
    unittest.main()
