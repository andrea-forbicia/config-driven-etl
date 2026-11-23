import shutil
import tempfile
import unittest
from datetime import UTC
from pathlib import Path
from unittest import mock

import polars as pl
import requests
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from config_driven_etl.bq_schema import (
    bigquery_rejected_schema,
    rejected_table_spec,
    table_spec,
)
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
    QualityGateError,
    RunRecorder,
    SchemaDriftError,
    StageRecord,
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
# Messages name tables without their project.
VALID_LABEL = "dataset.library_loans"
REJECTED_LABEL = "dataset.library_loans_rejected"
NOT_LOADED = (
    f"rejected rows not loaded; {VALID_LABEL} already holds this run's "
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


def existing_client(
    config: DatasetConfig,
    schema: list[bigquery.SchemaField],
    rejected_schema: list[bigquery.SchemaField] | None = None,
) -> mock.MagicMock:
    """A client whose target table exists with schema.

    With config.target.rejected_table, the rejected table exists with
    rejected_schema, or is missing when it is None. Both tables then hold
    the fixture's rows.
    """
    spec = table_spec(config)
    table = bigquery.Table(VALID_TABLE, schema=schema)
    table.clustering_fields = list(spec.cluster_by) or None
    answers: list[object] = [table]
    counts = [LOANS.rows_valid]
    if config.target.rejected_table is not None:
        answers.append(
            NotFound("no rejected table")
            if rejected_schema is None
            else bigquery.Table(REJECTED_TABLE, schema=rejected_schema)
        )
        counts.append(LOANS.rows_rejected)
    for rows in counts:
        loaded = mock.create_autospec(bigquery.Table, instance=True)
        loaded.num_rows = rows
        answers.append(loaded)
    client = mock.create_autospec(bigquery.Client, instance=True)
    client.project = "project"
    client.get_table.side_effect = answers
    return client


def with_rules(config: DatasetConfig, **rules: object) -> DatasetConfig:
    updated = config.rules.model_copy(update=rules)
    return config.model_copy(update={"rules": updated})


def with_max_rejected_ratio(
    config: DatasetConfig, limit: float
) -> DatasetConfig:
    return with_rules(config, max_rejected_ratio=limit)


def without_rejected_table(config: DatasetConfig) -> DatasetConfig:
    return with_target(config, rejected_table=None)


def with_target(config: DatasetConfig, **target: object) -> DatasetConfig:
    updated = config.target.model_copy(update=target)
    return config.model_copy(update={"target": updated})


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
                self.assertEqual(
                    result.dropped_columns, outcome.dropped_columns
                )
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


class QualityGateTest(PipelineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = fixture_config("library_loans")
        extract_dataset(self.config, self.data_dir, self.session)
        self.valid_path = processed_parquet_path(
            self.data_dir, self.config.name
        )
        self.rejected_path = rejected_parquet_path(
            self.data_dir, self.config.name
        )
        self.ratio = LOANS.rows_rejected / LOANS.rows_in

    def with_source(self, csv: bytes) -> DatasetConfig:
        path = self.data_dir / "source.csv"
        path.write_bytes(csv)
        source = self.config.source.model_copy(update={"path": path})
        config = self.config.model_copy(update={"source": source})
        extract_dataset(config, self.data_dir, self.session)
        return config

    def fixture_lines(self) -> list[bytes]:
        source = self.config.source.path
        assert source is not None
        # Bytes, so the copy keeps the fixture's encoding.
        return source.read_bytes().split(b"\n")

    def test_ratio_equal_to_the_limit_passes(self) -> None:
        config = with_max_rejected_ratio(self.config, self.ratio)

        result = transform_dataset(config, self.data_dir)

        self.assertEqual(result.rows_rejected, LOANS.rows_rejected)
        self.assertTrue(self.valid_path.is_file())

    def test_ratio_above_the_limit_fails_and_writes_no_valid_rows(
        self,
    ) -> None:
        # The previous run's valid output must not survive the failure.
        transform_dataset(self.config, self.data_dir)
        config = with_max_rejected_ratio(self.config, 0.5)

        with self.assertRaises(QualityGateError) as context:
            transform_dataset(config, self.data_dir)

        self.assertEqual(
            str(context.exception),
            f"library_loans: {LOANS.rows_rejected} of {LOANS.rows_in} rows "
            f"rejected (ratio {self.ratio:.4g}), above "
            "rules.max_rejected_ratio 0.5; valid rows not written, "
            "rejected rows written",
        )
        self.assertEqual(
            sorted(self.valid_path.parent.iterdir()), [self.rejected_path]
        )
        rejected = pl.read_parquet(self.rejected_path)
        self.assertEqual(rejected.height, LOANS.rows_rejected)
        self.assertEqual(rejected.schema, rejected_schema(config))

    def test_zero_limit_fails_on_any_rejected_row(self) -> None:
        config = with_max_rejected_ratio(self.config, 0.0)

        with self.assertRaises(QualityGateError) as context:
            transform_dataset(config, self.data_dir)

        self.assertEqual(
            context.exception.result.rows_rejected, LOANS.rows_rejected
        )
        self.assertEqual(context.exception.result.rejections, LOANS.rejections)
        self.assertFalse(self.valid_path.exists())

    def test_zero_limit_passes_without_rejected_rows(self) -> None:
        header, first, *_ = self.fixture_lines()
        config = with_max_rejected_ratio(
            self.with_source(header + b"\n" + first + b"\n"), 0.0
        )

        result = transform_dataset(config, self.data_dir)

        self.assertEqual((result.rows_out, result.rows_rejected), (1, 0))

    def test_empty_source_passes_any_limit(self) -> None:
        header, *_ = self.fixture_lines()
        config = with_max_rejected_ratio(self.with_source(header + b"\n"), 0.0)

        result = transform_dataset(config, self.data_dir)

        self.assertEqual((result.rows_in, result.rows_rejected), (0, 0))
        self.assertEqual(pl.read_parquet(self.valid_path).height, 0)

    def test_valid_rows_equal_to_min_rows_pass(self) -> None:
        config = with_rules(self.config, min_rows=LOANS.rows_valid)

        result = transform_dataset(config, self.data_dir)

        self.assertEqual(result.rows_out, LOANS.rows_valid)
        self.assertTrue(self.valid_path.is_file())

    def test_fewer_valid_rows_than_min_rows_fail(self) -> None:
        transform_dataset(self.config, self.data_dir)
        config = with_rules(self.config, min_rows=LOANS.rows_valid + 1)

        with self.assertRaises(QualityGateError) as context:
            transform_dataset(config, self.data_dir)

        self.assertEqual(
            str(context.exception),
            f"library_loans: {LOANS.rows_valid} valid rows, below "
            f"rules.min_rows {LOANS.rows_valid + 1}; valid rows not "
            "written, rejected rows written",
        )
        self.assertEqual(
            sorted(self.valid_path.parent.iterdir()), [self.rejected_path]
        )

    def test_header_only_source_fails_min_rows(self) -> None:
        header, *_ = self.fixture_lines()
        # The ratio alone lets an empty source pass.
        config = with_rules(
            self.with_source(header + b"\n"),
            max_rejected_ratio=0.0,
            min_rows=1,
        )

        with self.assertRaises(QualityGateError) as context:
            transform_dataset(config, self.data_dir)

        self.assertIn(
            "0 valid rows, below rules.min_rows 1", str(context.exception)
        )
        self.assertNotIn("max_rejected_ratio", str(context.exception))
        self.assertFalse(self.valid_path.exists())

    def test_both_gates_failing_give_both_reasons(self) -> None:
        config = with_rules(
            self.config,
            max_rejected_ratio=0.5,
            min_rows=LOANS.rows_valid + 1,
        )

        with self.assertRaises(QualityGateError) as context:
            transform_dataset(config, self.data_dir)

        self.assertEqual(
            str(context.exception),
            f"library_loans: {LOANS.rows_rejected} of {LOANS.rows_in} rows "
            f"rejected (ratio {self.ratio:.4g}), above "
            f"rules.max_rejected_ratio 0.5; {LOANS.rows_valid} valid rows, "
            f"below rules.min_rows {LOANS.rows_valid + 1}; valid rows not "
            "written, rejected rows written",
        )

    def test_previous_output_that_cannot_be_removed_is_a_load_error(
        self,
    ) -> None:
        transform_dataset(self.config, self.data_dir)
        config = with_max_rejected_ratio(self.config, 0.5)
        unlink = Path.unlink

        def fail_on_valid_output(path: Path, missing_ok: bool = False) -> None:
            if path == self.valid_path:
                raise PermissionError("file in use")
            unlink(path, missing_ok=missing_ok)

        with (
            mock.patch.object(
                Path, "unlink", autospec=True, side_effect=fail_on_valid_output
            ),
            self.assertRaises(LoadError) as context,
        ):
            transform_dataset(config, self.data_dir)

        message = str(context.exception)
        self.assertIn("above rules.max_rejected_ratio 0.5\n", message)
        self.assertIn(
            f"{self.valid_path}: cannot remove the previous valid rows, a "
            "later load would use them: file in use",
            message,
        )

    def test_failed_rejected_write_leaves_no_valid_rows(self) -> None:
        transform_dataset(self.config, self.data_dir)
        config = with_max_rejected_ratio(self.config, 0.5)

        with (
            mock.patch(
                "config_driven_etl.pipeline.write_parquet_files",
                side_effect=LoadError("x: cannot write Parquet: disk full"),
            ),
            self.assertRaises(LoadError) as context,
        ):
            transform_dataset(config, self.data_dir)

        self.assertRegex(
            str(context.exception),
            r"above rules\.max_rejected_ratio 0\.5\n"
            r"x: cannot write Parquet: disk full$",
        )
        self.assertFalse(self.valid_path.exists())


class RunRecorderTest(unittest.TestCase):
    def test_summary_keeps_the_stages_in_order(self) -> None:
        recorder = RunRecorder("run", "1.2.3")
        recorder.dataset = "sample"
        extract = StageResult(rows_in=None, rows_out=3)
        transform = StageResult(rows_in=3, rows_out=2, rows_rejected=1)
        recorder.start("extract")
        recorder.record("extract", extract, 0.5)
        recorder.start("transform")
        recorder.record("transform", transform, 0.25)

        summary = recorder.finish(0, None)

        self.assertEqual(
            summary.stages,
            (
                StageRecord("extract", extract, 0.5),
                StageRecord("transform", transform, 0.25),
            ),
        )
        self.assertEqual(
            (
                summary.run_id,
                summary.command,
                summary.version,
                summary.dataset,
                summary.exit_code,
                summary.outcome,
                summary.category,
                summary.failed_stage,
            ),
            (
                recorder.run_id,
                "run",
                "1.2.3",
                "sample",
                0,
                "success",
                None,
                None,
            ),
        )
        self.assertEqual(summary.started_at.tzinfo, UTC)
        self.assertGreaterEqual(summary.duration_s, 0)

    def test_any_non_zero_exit_code_is_a_failure(self) -> None:
        for exit_code in [1, 3, 7]:
            with self.subTest(exit_code=exit_code):
                summary = RunRecorder("load", "1.2.3").finish(exit_code, "x")

                self.assertEqual(summary.outcome, "failed")
                self.assertIsNone(summary.dataset)
                self.assertEqual(summary.stages, ())
                self.assertIsNone(summary.failed_stage)

    def test_failure_names_the_stage_that_was_running(self) -> None:
        recorder = RunRecorder("run", "1.2.3")
        recorder.start("extract")
        recorder.record("extract", StageResult(None, 3), 0.5)
        recorder.start("load")

        summary = recorder.finish(6, "load")

        self.assertEqual(summary.failed_stage, "load")
        self.assertEqual(summary.category, "load")
        self.assertEqual([s.stage for s in summary.stages], ["extract"])

    def test_a_failed_stage_with_counts_is_recorded_and_named(self) -> None:
        recorder = RunRecorder("transform", "1.2.3")
        recorder.start("transform")
        result = StageResult(rows_in=4, rows_out=1, rows_rejected=3)
        recorder.record("transform", result, 0.5, failed=True)

        summary = recorder.finish(7, "quality_gate")

        self.assertEqual(summary.failed_stage, "transform")
        self.assertEqual(
            summary.stages, (StageRecord("transform", result, 0.5),)
        )

    def test_a_finished_stage_is_not_the_failed_one(self) -> None:
        # e.g. run fails on a setting after extract and before load.
        recorder = RunRecorder("run", "1.2.3")
        recorder.start("extract")
        recorder.record("extract", StageResult(None, 3), 0.5)

        self.assertIsNone(recorder.finish(3, "config").failed_stage)

    def test_every_recorder_has_its_own_run_id(self) -> None:
        ids = {RunRecorder("run", "1.2.3").run_id for _ in range(3)}

        self.assertEqual(len(ids), 3)


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

    def test_unchanged_schema_loads_with_no_schema_change(self) -> None:
        config = without_rejected_table(self.config)
        schema = list(table_spec(config).schema)
        client = existing_client(config, schema)

        result = load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(result.schema_changes, ())
        self.assertEqual(self.loaded_tables(client), [VALID_TABLE])

    def test_additive_change_loads_and_is_listed(self) -> None:
        config = without_rejected_table(self.config)
        *older, last = table_spec(config).schema
        client = existing_client(config, older)

        result = load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            result.schema_changes, (f"{last.name}: added as NULLABLE",)
        )
        self.assertEqual(self.loaded_tables(client), [VALID_TABLE])

    def test_additive_change_fails_when_none_is_allowed(self) -> None:
        config = with_target(self.config, schema_change="none")
        *older, last = table_spec(config).schema
        row_number, reasons, raw = rejected_table_spec(config).schema
        older_raw = raw.to_api_repr()
        older_raw["fields"] = older_raw["fields"][:-1]
        client = existing_client(
            config,
            older,
            [
                row_number,
                reasons,
                bigquery.SchemaField.from_api_repr(older_raw),
            ],
        )

        with self.assertRaises(SchemaDriftError) as context:
            load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines(),
            [
                f"{VALID_LABEL}: additive schema change, "
                "target.schema_change is none",
                f"{VALID_LABEL}: {last.name}: added as NULLABLE",
                f"{REJECTED_LABEL}: additive schema change, "
                "target.schema_change is none",
                f"{REJECTED_LABEL}: raw.{last.name}: added as NULLABLE",
                "rerun with --allow-schema-change to replace the schemas",
            ],
        )
        client.load_table_from_file.assert_not_called()

    def test_breaking_change_fails_before_any_load(self) -> None:
        config = self.config
        schema = [
            *table_spec(config).schema,
            bigquery.SchemaField("old", "STRING"),
        ]
        client = existing_client(config, schema)

        with self.assertRaises(SchemaDriftError) as context:
            load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines()[:2],
            [
                f"{VALID_LABEL}: breaking schema change",
                f"{VALID_LABEL}: old: removed",
            ],
        )
        client.load_table_from_file.assert_not_called()

    def test_breaking_change_in_the_rejected_table_fails(self) -> None:
        config = self.config
        rejected = [
            *rejected_table_spec(config).schema,
            bigquery.SchemaField("old", "STRING"),
        ]
        client = existing_client(
            config, list(table_spec(config).schema), rejected
        )

        with self.assertRaises(SchemaDriftError) as context:
            load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception).splitlines(),
            [
                f"{REJECTED_LABEL}: breaking schema change",
                f"{REJECTED_LABEL}: old: removed",
                "rerun with --allow-schema-change to replace the schemas",
            ],
        )
        client.load_table_from_file.assert_not_called()

    def test_rejected_table_changes_are_listed_apart(self) -> None:
        config = self.config
        rejected = [
            *rejected_table_spec(config).schema,
            bigquery.SchemaField("old", "STRING"),
        ]
        client = existing_client(
            config, list(table_spec(config).schema), rejected
        )

        result = load_dataset(
            config,
            self.data_dir,
            client,
            "dataset",
            allow_schema_change=True,
        )

        self.assertEqual(
            (result.schema_changes, result.rejected_schema_changes),
            ((), ("old: removed",)),
        )
        self.assertEqual(
            self.loaded_tables(client), [VALID_TABLE, REJECTED_TABLE]
        )

    def test_new_rejected_table_has_no_schema_changes(self) -> None:
        config = self.config
        client = existing_client(config, list(table_spec(config).schema))

        result = load_dataset(config, self.data_dir, client, "dataset")

        self.assertEqual(result.schema_changes, ())
        self.assertIsNone(result.rejected_schema_changes)

    def test_allow_schema_change_loads_a_breaking_change(self) -> None:
        config = without_rejected_table(
            with_target(self.config, schema_change="none")
        )
        schema = [
            *table_spec(config).schema,
            bigquery.SchemaField("old", "STRING"),
        ]
        client = existing_client(config, schema)

        result = load_dataset(
            config,
            self.data_dir,
            client,
            "dataset",
            allow_schema_change=True,
        )

        self.assertEqual(result.schema_changes, ("old: removed",))
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
                f"{REJECTED_LABEL}: BigQuery error: 400 bad rejected rows",
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
                f"{REJECTED_LABEL}: {loaded} rows loaded, "
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
                f"{REJECTED_LABEL}: partitioned by x, config says nothing",
                f"{REJECTED_LABEL}: delete the table to recreate it",
            ],
        )
        client.load_table_from_file.assert_not_called()

    def test_missing_valid_file_fails_before_any_load(self) -> None:
        # What a failed quality gate leaves behind.
        path = processed_parquet_path(self.data_dir, self.config.name)
        path.unlink()
        client = fake_client(LOANS.rows_valid, LOANS.rows_rejected)

        with self.assertRaises(LoadError) as context:
            load_dataset(self.config, self.data_dir, client, "dataset")

        self.assertEqual(
            str(context.exception),
            f"{path}: not found; run transform first (a failed quality "
            "gate removes it)",
        )
        client.get_table.assert_not_called()
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
