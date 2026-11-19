"""The extract, transform and load stages of one dataset run."""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import requests
from google.cloud import bigquery

from config_driven_etl.bq_schema import (
    TableSpec,
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
    write_parquet_files(
        [
            ParquetOutput(
                processed_parquet_path(data_dir, config.name),
                valid,
                output_schema(config),
            ),
            ParquetOutput(
                rejected_parquet_path(data_dir, config.name),
                rejected,
                rejected_schema(config),
            ),
        ]
    )
    return StageResult(
        rows_in=valid.height + rejected.height,
        rows_out=valid.height,
        rows_rejected=rejected.height,
        rejections=dict(counts.iter_rows()),
    )


def load_dataset(
    config: DatasetConfig,
    data_dir: Path,
    client: bigquery.Client,
    dataset: str,
) -> StageResult:
    """Load the processed Parquet into BigQuery and check the row count.

    With target.rejected_table, the rejected rows are loaded next, into
    their own table. Both files are read and both table layouts checked
    first, so those failures leave both tables unchanged.
    """
    path = processed_parquet_path(data_dir, config.name)
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

    for load in (valid, rejected):
        if load is not None:
            ensure_table(client, load.table_id, load.spec)
    _load_and_verify(client, valid)
    if rejected is not None:
        try:
            _load_and_verify(client, rejected)
        except LoadError as error:
            raise LoadError(
                f"{error}\nrejected rows not loaded; {valid.table_id} "
                "already holds this run's valid rows"
            ) from error
    return StageResult(rows_in=valid.rows, rows_out=valid.rows)


@dataclass(frozen=True)
class _TableLoad:
    """A Parquet file, its row count and the table it is loaded into."""

    path: Path
    rows: int
    table_id: str
    spec: TableSpec


def _load_and_verify(client: bigquery.Client, load: _TableLoad) -> None:
    load_parquet(client, load.path, load.table_id, load.spec)
    verify_table_rows(client, load.table_id, load.rows)
