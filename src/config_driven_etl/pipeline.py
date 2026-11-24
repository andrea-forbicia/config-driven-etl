"""The extract, transform and load stages of one dataset run."""

import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl
import requests
from google.cloud import bigquery

from config_driven_etl.bq_schema import (
    TableSpec,
    compare_schema,
    rejected_table_spec,
    table_spec,
)
from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.errors import error_reason
from config_driven_etl.extract import (
    ExtractError,
    copy_csv,
    download_csv,
    raw_csv_path,
    scan_raw_csv,
)
from config_driven_etl.load import (
    LoadError,
    ParquetOutput,
    bigquery_table_id,
    ensure_table,
    load_parquet,
    parquet_row_count,
    processed_parquet_path,
    rejected_parquet_path,
    table_label,
    verify_table_rows,
    write_parquet_files,
)
from config_driven_etl.transform import (
    output_schema,
    rejected_schema,
    transform,
)


@dataclass(frozen=True)
class StageResult:
    """Row counts of one stage; None where a count does not apply."""

    rows_in: int | None
    rows_out: int
    rows_rejected: int = 0
    rejections: dict[str, int] | None = None
    # Rows loaded into target.rejected_table; None without one.
    rejected_rows_loaded: int | None = None
    # Unmapped source columns the transform dropped; None in other stages.
    dropped_columns: tuple[str, ...] | None = None
    # How the load changed the schema of an existing target table; None
    # in other stages and when the load created the table.
    schema_changes: tuple[str, ...] | None = None
    # The same for target.rejected_table; None also without one.
    rejected_schema_changes: tuple[str, ...] | None = None


Stage = Literal["extract", "transform", "load"]
Command = Literal["extract", "transform", "load", "run"]


@dataclass(frozen=True)
class StageRecord:
    """A stage that produced row counts; a failed quality gate does too."""

    stage: Stage
    result: StageResult
    duration_s: float


@dataclass(frozen=True)
class RunSummary:
    """What one pipeline command did, from start to exit code."""

    run_id: str
    command: Command
    version: str
    started_at: datetime
    # None when the dataset file could not be read.
    dataset: str | None
    stages: tuple[StageRecord, ...]
    exit_code: int
    # Error category, e.g. "load"; None on success.
    category: str | None
    # The stage running when the command failed; None on success or
    # when no stage had started.
    failed_stage: Stage | None
    duration_s: float

    @property
    def outcome(self) -> Literal["success", "failed"]:
        return "success" if self.exit_code == 0 else "failed"


class RunRecorder:
    """Collects the stages of one command into a RunSummary."""

    def __init__(self, command: Command, version: str) -> None:
        self.run_id = uuid.uuid4().hex
        self.dataset: str | None = None
        self._command: Command = command
        self._version = version
        self._started_at = datetime.now(UTC)
        # Wall-clock time can jump; durations use a monotonic clock.
        self._started = time.perf_counter()
        self._stages: list[StageRecord] = []
        self._running: Stage | None = None

    def start(self, stage: Stage) -> None:
        """Mark a stage as running, so a failure can name it."""
        self._running = stage

    def record(
        self,
        stage: Stage,
        result: StageResult,
        duration_s: float,
        *,
        failed: bool = False,
    ) -> None:
        """Add a stage that produced row counts, and end it unless failed."""
        self._stages.append(StageRecord(stage, result, duration_s))
        if not failed:
            self._running = None

    def finish(self, exit_code: int, category: str | None) -> RunSummary:
        """The summary of the run, ending with exit_code."""
        return RunSummary(
            run_id=self.run_id,
            command=self._command,
            version=self._version,
            started_at=self._started_at,
            dataset=self.dataset,
            stages=tuple(self._stages),
            exit_code=exit_code,
            category=category,
            failed_stage=None if exit_code == 0 else self._running,
            duration_s=round(time.perf_counter() - self._started, 3),
        )


def audit_row(summary: RunSummary) -> dict[str, object]:
    """The summary as a JSON-ready row of bq_schema.AUDIT_TABLE_SPEC.

    The run_finished log line logs the same fields, so the two cannot
    drift apart.
    """
    return {
        "run_id": summary.run_id,
        "command": summary.command,
        "version": summary.version,
        "dataset": summary.dataset,
        "started_at": summary.started_at.isoformat(timespec="milliseconds"),
        "duration_s": summary.duration_s,
        "outcome": summary.outcome,
        "exit_code": summary.exit_code,
        "category": summary.category,
        "failed_stage": summary.failed_stage,
        "stages": [
            {
                "stage": record.stage,
                "rows_in": record.result.rows_in,
                "rows_out": record.result.rows_out,
                "rows_rejected": record.result.rows_rejected,
                "duration_s": record.duration_s,
                # BigQuery has no map type: one record per reason.
                "rejections": [
                    {"reason": reason, "rows": rows}
                    for reason, rows in (
                        record.result.rejections or {}
                    ).items()
                ],
                "dropped_columns": list(record.result.dropped_columns or ()),
            }
            for record in summary.stages
        ],
    }


class SchemaDriftError(Exception):
    """A load would change a table's schema more than the config allows."""


class QualityGateError(Exception):
    """Data that was read but fails a dataset-wide quality threshold."""

    def __init__(self, message: str, result: StageResult) -> None:
        super().__init__(message)
        # The counts explain the failure, so they are logged with it.
        self.result = result


def extract_dataset(
    config: DatasetConfig, data_dir: Path, session: requests.Session
) -> StageResult:
    """Download or copy the raw CSV into data_dir and count its rows.

    Counting reads the whole file, so a CSV whose body cannot be parsed
    fails here, in the extract stage.
    """
    destination = raw_csv_path(data_dir, config.name)
    source = config.source
    # The config guarantees exactly one of path and url.
    if source.path is not None:
        copy_csv(source.path, destination)
    else:
        download_csv(str(source.url), destination, session)
    lf = scan_raw_csv(destination, source)
    try:
        rows = lf.select(pl.len()).collect().item()
    except pl.exceptions.PolarsError as error:
        raise ExtractError(
            f"{destination}: cannot read CSV: {error_reason(error)}"
        ) from error
    return StageResult(rows_in=None, rows_out=rows)


def transform_dataset(config: DatasetConfig, data_dir: Path) -> StageResult:
    """Transform the raw CSV and write valid and rejected rows as Parquet.

    Both files are verified before either replaces the previous run's.
    Above rules.max_rejected_ratio or below rules.min_rows, the previous
    valid output is removed so no later load can use it, only the
    rejected rows are written, and QualityGateError is raised; if the
    removal or the write fails, a LoadError that also gives the gate's
    reason is raised instead.
    """
    raw = raw_csv_path(data_dir, config.name)
    result = transform(scan_raw_csv(raw, config.source), config)
    try:
        # Collected together so the CSV is read once.
        valid, rejected, counts = pl.collect_all(
            [result.valid, result.rejected, result.rejection_counts]
        )
    except pl.exceptions.PolarsError as error:
        # Every transform expression is non-strict and rejects bad values
        # instead of failing, so a Polars error here comes from the CSV.
        raise ExtractError(
            f"{raw}: cannot read CSV: {error_reason(error)}"
        ) from error
    stage = StageResult(
        rows_in=valid.height + rejected.height,
        rows_out=valid.height,
        rows_rejected=rejected.height,
        rejections=dict(counts.iter_rows()),
        dropped_columns=result.dropped_columns,
    )
    valid_path = processed_parquet_path(data_dir, config.name)
    rejected_output = ParquetOutput(
        rejected_parquet_path(data_dir, config.name),
        rejected,
        rejected_schema(config),
    )
    gate_failure = _gate_failure(config, stage)
    if gate_failure is None:
        write_parquet_files(
            [
                ParquetOutput(valid_path, valid, output_schema(config)),
                rejected_output,
            ]
        )
        return stage
    # Removed before anything is written, so whatever fails next leaves
    # no valid rows for a later load.
    try:
        valid_path.unlink(missing_ok=True)
    except OSError as error:
        raise LoadError(
            f"{gate_failure}\n{valid_path}: cannot remove the previous "
            f"valid rows, a later load would use them: {error_reason(error)}"
        ) from error
    try:
        write_parquet_files([rejected_output])
    except LoadError as error:
        raise LoadError(f"{gate_failure}\n{error}") from error
    raise QualityGateError(
        f"{gate_failure}; valid rows not written, rejected rows written",
        stage,
    )


def _gate_failure(config: DatasetConfig, stage: StageResult) -> str | None:
    """Every quality gate the stage fails, in one message, or None."""
    reasons = [
        reason
        for reason in (
            _rejected_ratio_failure(config, stage),
            _min_rows_failure(config, stage),
        )
        if reason is not None
    ]
    if not reasons:
        return None
    return f"{config.name}: {'; '.join(reasons)}"


def _rejected_ratio_failure(
    config: DatasetConfig, stage: StageResult
) -> str | None:
    limit = config.rules.max_rejected_ratio
    # No rows means no rejected rows: an empty source is left to
    # min_rows.
    if limit is None or not stage.rows_in:
        return None
    ratio = stage.rows_rejected / stage.rows_in
    if ratio <= limit:
        return None
    return (
        f"{stage.rows_rejected} of {stage.rows_in} rows rejected "
        f"(ratio {ratio:.4g}), above rules.max_rejected_ratio {limit:g}"
    )


def _min_rows_failure(config: DatasetConfig, stage: StageResult) -> str | None:
    minimum = config.rules.min_rows
    # Valid rows, the ones a load would put in the table.
    if minimum is None or stage.rows_out >= minimum:
        return None
    return f"{stage.rows_out} valid rows, below rules.min_rows {minimum}"


def require_supported_write_mode(config: DatasetConfig) -> None:
    """Raise LoadError if this version cannot load target.write_mode."""
    # A truncate would erase the partitions the config asks to keep.
    if config.target.write_mode == "partition":
        raise LoadError(
            "target.write_mode: partition is not supported by this version yet"
        )


def load_dataset(
    config: DatasetConfig,
    data_dir: Path,
    client: bigquery.Client,
    dataset: str,
    *,
    allow_schema_change: bool = False,
) -> StageResult:
    """Load the processed Parquet into BigQuery and check the row count.

    With target.rejected_table, the rejected rows are loaded next, into
    their own table. Both files are read, both table layouts checked and
    the schemas of both existing tables compared with the config first,
    so those failures leave both tables unchanged; a missing table is
    created, empty, before the comparison. A schema change above
    target.schema_change, in either table, raises SchemaDriftError,
    unless allow_schema_change accepts any change for this load.
    """
    require_supported_write_mode(config)
    path = processed_parquet_path(data_dir, config.name)
    if not path.is_file():
        raise LoadError(
            f"{path}: not found; run transform first (a failed quality "
            "gate removes it)"
        )
    valid = _TableLoad(
        path=path,
        rows=parquet_row_count(path),
        table_id=bigquery_table_id(
            client.project, dataset, config.target.table
        ),
        spec=table_spec(config),
    )
    rejected = None
    if config.target.rejected_table is not None:
        path = rejected_parquet_path(data_dir, config.name)
        rejected = _TableLoad(
            path=path,
            rows=parquet_row_count(path),
            table_id=bigquery_table_id(
                client.project, dataset, config.target.rejected_table
            ),
            spec=rejected_table_spec(config),
        )

    loads = [valid] if rejected is None else [valid, rejected]
    existing = [
        ensure_table(client, load.table_id, load.spec) for load in loads
    ]
    changes = _schema_changes(
        list(zip(loads, existing, strict=True)),
        config.target.schema_change,
        allow_schema_change,
    )
    _load_and_verify(client, valid)
    if rejected is not None:
        try:
            _load_and_verify(client, rejected)
        except LoadError as error:
            raise LoadError(
                f"{error}\nrejected rows not loaded; "
                f"{table_label(valid.table_id)} already holds this run's "
                "valid rows"
            ) from error
    return StageResult(
        rows_in=valid.rows,
        rows_out=valid.rows,
        rejected_rows_loaded=None if rejected is None else rejected.rows,
        schema_changes=changes[0],
        rejected_schema_changes=None if rejected is None else changes[1],
    )


@dataclass(frozen=True)
class _TableLoad:
    """A Parquet file, its row count and the table it is loaded into."""

    path: Path
    rows: int
    table_id: str
    spec: TableSpec


def _schema_changes(
    tables: Sequence[tuple[_TableLoad, Sequence[bigquery.SchemaField] | None]],
    allowed: Literal["none", "additive"],
    allow_any: bool,
) -> list[tuple[str, ...] | None]:
    """How each load changes its table's schema; None for a new table.

    Raises SchemaDriftError listing every table whose change is not
    allowed, so one run reports them all.
    """
    changes: list[tuple[str, ...] | None] = []
    problems: list[str] = []
    for load, existing in tables:
        if existing is None:
            changes.append(None)
            continue
        diff = compare_schema(existing, load.spec.schema)
        described = tuple(
            f"{item.path}: {item.message}" if item.path else item.message
            for item in diff.differences
        )
        changes.append(described)
        if (
            allow_any
            or diff.kind == "none"
            or (diff.kind == "additive" and allowed == "additive")
        ):
            continue
        label = table_label(load.table_id)
        # No config value allows a breaking change, so the setting is
        # named only when changing it would help.
        reason = (
            "breaking schema change"
            if diff.kind == "breaking"
            else f"additive schema change, target.schema_change is {allowed}"
        )
        problems.append(f"{label}: {reason}")
        problems += [f"{label}: {change}" for change in described]
    if problems:
        raise SchemaDriftError(
            "\n".join(
                [
                    *problems,
                    "rerun with --allow-schema-change to replace the schemas",
                ]
            )
        )
    return changes


def _load_and_verify(client: bigquery.Client, load: _TableLoad) -> None:
    load_parquet(client, load.path, load.table_id, load.spec)
    verify_table_rows(client, load.table_id, load.rows)
