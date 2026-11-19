"""The extract, transform and load stages of one dataset run."""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import requests
from google.cloud import bigquery

from config_driven_etl.bq_schema import table_spec
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
    """Load the processed Parquet into BigQuery and check the row count."""
    path = processed_parquet_path(data_dir, config.name)
    rows = parquet_row_count(path)
    table_id = bigquery_table_id(client.project, dataset, config.target.table)
    spec = table_spec(config)
    ensure_table(client, table_id, spec)
    load_parquet(client, path, table_id, spec)
    verify_table_rows(client, table_id, rows)
    return StageResult(rows_in=rows, rows_out=rows)
