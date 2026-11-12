"""Parquet output and BigQuery load jobs."""

import os
import tempfile
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path

import polars as pl
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery

from config_driven_etl.bq_schema import bigquery_schema
from config_driven_etl.dataset_config import DatasetConfig
from config_driven_etl.transform import output_schema


class LoadError(Exception):
    """Output that could not be written or does not match what was sent."""


def processed_parquet_path(data_dir: Path, name: str) -> Path:
    """Return where the processed Parquet file of a dataset is stored."""
    return data_dir / "processed" / name / f"{name}.parquet"


def write_parquet(lf: pl.LazyFrame, destination: Path) -> None:
    """Write lf to destination as snappy Parquet, replacing any old file.

    The frame is streamed to a temporary file in the same directory and
    renamed when complete, so a failed run never leaves a truncated file
    or removes the previous output.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    # A unique name keeps two runs of the same dataset from colliding.
    handle, name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".part",
    )
    temp = Path(name)
    try:
        # Polars opens the path itself, and Windows cannot replace an
        # open file.
        os.close(handle)
        lf.sink_parquet(temp, compression="snappy")
        temp.replace(destination)
    finally:
        temp.unlink(missing_ok=True)


def verify_parquet(
    path: Path, config: DatasetConfig, expected_rows: int
) -> None:
    """Re-read a written Parquet file and check its schema and row count.

    The columns must be exactly those of output_schema, in config order
    and with its dtypes. Raises LoadError listing every mismatch.
    """
    try:
        # Both read only the file footer, not the data.
        actual = pl.read_parquet_schema(path)
        rows = pl.scan_parquet(path).select(pl.len()).collect().item()
    except (OSError, pl.exceptions.PolarsError) as error:
        lines = str(error).splitlines()
        reason = lines[0] if lines and lines[0] else type(error).__name__
        raise LoadError(f"{path}: cannot read Parquet: {reason}") from error

    problems = _schema_problems(config, actual)
    if rows != expected_rows:
        problems.append(f"{rows} rows written, expected {expected_rows}")
    if problems:
        raise LoadError(
            "\n".join(f"{path}: {problem}" for problem in problems)
        )


def _schema_problems(
    config: DatasetConfig, actual: Mapping[str, pl.DataType]
) -> list[str]:
    expected = output_schema(config)
    problems = []
    for name, dtype in expected.items():
        if name not in actual:
            problems.append(f"missing column '{name}'")
        elif actual[name] != dtype:
            problems.append(
                f"column '{name}' is {actual[name]}, expected {dtype}"
            )
    problems += [
        f"unexpected column '{name}'"
        for name in actual
        if name not in expected
    ]
    # Only meaningful when the same columns are there.
    if not problems and list(actual) != list(expected):
        problems.append("columns are not in config order")
    return problems


def bigquery_table_id(project: str, dataset: str, table: str) -> str:
    """Return the fully qualified id of a BigQuery table."""
    return f"{project}.{dataset}.{table}"


def ensure_table(
    client: bigquery.Client, table_id: str, config: DatasetConfig
) -> None:
    """Create the target table if it does not exist yet.

    A new table gets the config's schema, day partitioning on
    target.partition_by and clustering on target.cluster_by. An existing
    table keeps its schema, which each load replaces, but BigQuery cannot
    change its partitioning or clustering, so a mismatch raises LoadError.
    """
    with _bigquery_errors(table_id):
        try:
            existing = client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=bigquery_schema(config))
            table.time_partitioning = _time_partitioning(config)
            table.clustering_fields = list(config.target.cluster_by) or None
            # Another run may create it between the two calls.
            client.create_table(table, exists_ok=True)
            return

    problems = _layout_problems(existing, config)
    if problems:
        raise LoadError(
            "\n".join(f"{table_id}: {problem}" for problem in problems)
            + f"\n{table_id}: delete the table to recreate it"
        )


def load_parquet(
    client: bigquery.Client,
    path: Path,
    table_id: str,
    config: DatasetConfig,
) -> None:
    """Replace the table's data with a Parquet file and wait for the job.

    The load job uses the schema from the config, never autodetection,
    and WRITE_TRUNCATE, so re-running the same file leaves the same table
    state. Partitioning and clustering are repeated on the job because
    BigQuery checks them against the table. Raises LoadError if the file
    cannot be read or the job fails.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=bigquery_schema(config),
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=_time_partitioning(config),
        clustering_fields=list(config.target.cluster_by) or None,
    )
    try:
        file = path.open("rb")
    except OSError as error:
        reason = error.strerror or str(error)
        raise LoadError(f"{path}: cannot read file: {reason}") from error
    with file, _bigquery_errors(table_id):
        job = client.load_table_from_file(
            file, table_id, job_config=job_config
        )
        job.result()


def _time_partitioning(
    config: DatasetConfig,
) -> bigquery.TimePartitioning | None:
    field = config.target.partition_by
    if field is None:
        return None
    return bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field=field
    )


def _layout_problems(
    table: bigquery.Table, config: DatasetConfig
) -> list[str]:
    problems = []
    partitioning = table.time_partitioning
    if table.range_partitioning is not None:
        actual = "integer range"
    elif partitioning is None:
        actual = None
    else:
        # Without a field the table is partitioned by ingestion time.
        actual = partitioning.field or "_PARTITIONTIME"
        if partitioning.type_ != bigquery.TimePartitioningType.DAY:
            actual = f"{actual} ({partitioning.type_})"
    expected = config.target.partition_by
    if actual != expected:
        problems.append(
            f"partitioned by {actual or 'nothing'}, "
            f"config says {expected or 'nothing'}"
        )
    clustering = list(table.clustering_fields or [])
    if clustering != list(config.target.cluster_by):
        problems.append(
            f"clustered by {clustering or 'nothing'}, "
            f"config says {list(config.target.cluster_by) or 'nothing'}"
        )
    return problems


@contextmanager
def _bigquery_errors(table_id: str) -> Generator[None]:
    """Turn Google API errors into LoadError with a clear message."""
    try:
        yield
    except GoogleAPIError as error:
        reason = str(error).splitlines()[0] if str(error) else ""
        lines = [f"BigQuery error: {reason or type(error).__name__}"]
        # A failed load job puts the actual causes, such as the bad column,
        # in errors rather than in the message.
        details = getattr(error, "errors", None) or []
        lines += [
            f"  {detail['message']}"
            for detail in details[:_MAX_ERROR_DETAILS]
            if isinstance(detail, dict) and detail.get("message")
        ]
        raise LoadError(
            "\n".join(f"{table_id}: {line}" for line in lines)
        ) from error


_MAX_ERROR_DETAILS = 3
