"""Parquet output and BigQuery load jobs."""

import os
import tempfile
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import requests
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery

from config_driven_etl.bq_schema import TableSpec
from config_driven_etl.errors import error_reason


class LoadError(Exception):
    """Output that could not be written or does not match what was sent."""


@dataclass(frozen=True)
class ParquetOutput:
    """A frame to write as Parquet and the schema the file must have."""

    destination: Path
    frame: pl.DataFrame
    schema: pl.Schema


def processed_parquet_path(data_dir: Path, name: str) -> Path:
    """Return where the processed Parquet file of a dataset is stored."""
    return data_dir / "processed" / name / f"{name}.parquet"


def rejected_parquet_path(data_dir: Path, name: str) -> Path:
    """Return where the rejected rows of a dataset are stored."""
    # Names have no dot, so this never equals processed_parquet_path.
    return data_dir / "processed" / name / f"{name}.rejected.parquet"


def write_parquet(df: pl.DataFrame, destination: Path) -> None:
    """Write df to destination as snappy Parquet, replacing any old file.

    The single-file case of write_parquet_files, checked against the
    schema of df. Raises LoadError if it cannot be written.
    """
    write_parquet_files([ParquetOutput(destination, df, df.schema)])


def write_parquet_files(outputs: Sequence[ParquetOutput]) -> None:
    """Write each frame as snappy Parquet, replacing the previous files.

    Every frame is written to a temporary file next to its destination
    and verified against its schema and row count; only when all of
    them pass are they renamed into place. A failed write or check
    leaves every previous output untouched and no temporary file
    behind; a failed rename leaves the earlier destinations already
    replaced. Raises LoadError naming the destination that failed.
    """
    temps: list[Path] = []
    try:
        for output in outputs:
            temp = _temp_path(output.destination)
            temps.append(temp)
            _write(output.frame, temp, output.destination)
            _check_parquet(
                temp, output.schema, output.frame.height, output.destination
            )
        # Each rename is atomic, the set is not: a failed rename, or a
        # process killed between two, leaves outputs from different
        # runs.
        for temp, output in zip(temps, outputs, strict=True):
            try:
                temp.replace(output.destination)
            except OSError as error:
                raise _write_error(output.destination, error) from error
    finally:
        for temp in temps:
            temp.unlink(missing_ok=True)


def _temp_path(destination: Path) -> Path:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        # A unique name keeps two runs of the same dataset from colliding.
        handle, name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".part",
        )
        # Polars opens the path itself, and Windows cannot replace an
        # open file.
        os.close(handle)
    except OSError as error:
        raise _write_error(destination, error) from error
    return Path(name)


def _write(df: pl.DataFrame, temp: Path, destination: Path) -> None:
    try:
        # A synchronous write: sink_parquet can still create the file
        # from a background thread after raising, past the cleanup.
        df.write_parquet(temp, compression="snappy")
    except (OSError, pl.exceptions.PolarsError) as error:
        raise _write_error(destination, error) from error


def _write_error(destination: Path, error: Exception) -> LoadError:
    return LoadError(
        f"{destination}: cannot write Parquet: {error_reason(error)}"
    )


def verify_parquet(path: Path, schema: pl.Schema, expected_rows: int) -> None:
    """Re-read a written Parquet file and check its schema and row count.

    The columns must be exactly those of schema, in its order and with
    its dtypes. Raises LoadError listing every mismatch.
    """
    _check_parquet(path, schema, expected_rows, path)


def _check_parquet(
    path: Path, schema: pl.Schema, expected_rows: int, label: Path
) -> None:
    """verify_parquet on path, reporting problems under label."""
    actual, rows = _read_footer(path, label)
    problems = _schema_problems(schema, actual)
    if rows != expected_rows:
        problems.append(f"{rows} rows written, expected {expected_rows}")
    if problems:
        raise LoadError(
            "\n".join(f"{label}: {problem}" for problem in problems)
        )


def parquet_row_count(path: Path) -> int:
    """Return the number of rows in a Parquet file, from its footer."""
    return _read_footer(path, path)[1]


def verify_table_rows(
    client: bigquery.Client, table_id: str, expected_rows: int
) -> None:
    """Check that a table holds exactly expected_rows rows.

    After a WRITE_TRUNCATE load the table holds only the loaded file, so
    its row count must equal the file's. Raises LoadError otherwise.
    """
    with _bigquery_errors(table_id):
        table = client.get_table(table_id)
    if table.num_rows == expected_rows:
        return
    message = (
        f"{table_label(table_id)}: {table.num_rows} rows loaded, "
        f"expected {expected_rows}"
    )
    # BigQuery drops rows whose partition is already past its expiration
    # without failing the job; the sandbox expires partitions after 60
    # days, so old data silently disappears.
    if table.time_partitioning is not None:
        message += (
            "; rows in partitions older than the partition expiration are "
            "dropped (60 days in the BigQuery sandbox)"
        )
    raise LoadError(message)


def _read_footer(
    path: Path, label: Path
) -> tuple[dict[str, pl.DataType], int]:
    try:
        # Both read only the file footer, not the data.
        schema = pl.read_parquet_schema(path)
        rows = pl.scan_parquet(path).select(pl.len()).collect().item()
    except (OSError, pl.exceptions.PolarsError) as error:
        reason = error_reason(error)
        raise LoadError(f"{label}: cannot read Parquet: {reason}") from error
    return schema, rows


def _schema_problems(
    expected: pl.Schema, actual: Mapping[str, pl.DataType]
) -> list[str]:
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
        problems.append("columns are not in the expected order")
    return problems


def bigquery_table_id(project: str, dataset: str, table: str) -> str:
    """Return the fully qualified id of a BigQuery table."""
    return f"{project}.{dataset}.{table}"


def table_label(table_id: str) -> str:
    """The table id without its project, for messages that get logged."""
    # The project id is a setting, and settings never reach the logs.
    # From the right: a legacy project id can contain dots.
    return ".".join(table_id.rsplit(".", 2)[-2:])


def ensure_table(
    client: bigquery.Client, table_id: str, spec: TableSpec
) -> tuple[bigquery.SchemaField, ...] | None:
    """Create the table if it does not exist yet.

    A new table gets the spec's schema, day partitioning on partition_by
    and clustering on cluster_by, and None is returned. An existing table
    keeps its schema, which each load replaces, and that schema is
    returned for the caller to compare; BigQuery cannot change its
    partitioning or clustering, so a mismatch raises LoadError.
    """
    with _bigquery_errors(table_id):
        try:
            existing = client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=list(spec.schema))
            table.time_partitioning = _time_partitioning(spec)
            table.clustering_fields = list(spec.cluster_by) or None
            # Another run may create it between the two calls.
            client.create_table(table, exists_ok=True)
            return None

    problems = _layout_problems(existing, spec)
    if problems:
        label = table_label(table_id)
        raise LoadError(
            "\n".join(f"{label}: {problem}" for problem in problems)
            + f"\n{label}: delete the table to recreate it"
        )
    return tuple(existing.schema)


def load_parquet(
    client: bigquery.Client,
    path: Path,
    table_id: str,
    spec: TableSpec,
) -> None:
    """Replace the table's data with a Parquet file and wait for the job.

    The load job uses the schema from the spec, never autodetection,
    and WRITE_TRUNCATE, so re-running the same file leaves the same table
    state. Partitioning and clustering are repeated on the job because
    BigQuery checks them against the table. Raises LoadError if the file
    cannot be read or the job fails.
    """
    # Without list inference a Parquet list is read as a nested
    # list.element record, which does not match a REPEATED field.
    parquet_options = bigquery.ParquetOptions()
    parquet_options.enable_list_inference = True
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=list(spec.schema),
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=_time_partitioning(spec),
        clustering_fields=list(spec.cluster_by) or None,
        parquet_options=parquet_options,
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


def append_rows(
    client: bigquery.Client,
    table_id: str,
    spec: TableSpec,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Append rows to the table with a load job and wait for it.

    A load job, not streaming, which the BigQuery sandbox does not
    allow; the schema comes from the spec, never autodetection. Fields
    of the spec that the table lacks are added to it; any other
    difference fails the job. Raises LoadError if the job fails or
    appends a different number of rows.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=list(spec.schema),
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )
    with _bigquery_errors(table_id):
        job = client.load_table_from_json(
            [dict(row) for row in rows], table_id, job_config=job_config
        )
        job.result()
    if job.output_rows != len(rows):
        raise LoadError(
            f"{table_label(table_id)}: {job.output_rows} rows appended, "
            f"expected {len(rows)}"
        )


def _time_partitioning(
    spec: TableSpec,
) -> bigquery.TimePartitioning | None:
    field = spec.partition_by
    if field is None:
        return None
    return bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field=field
    )


def _layout_problems(table: bigquery.Table, spec: TableSpec) -> list[str]:
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
    expected = spec.partition_by
    if actual != expected:
        problems.append(
            f"partitioned by {actual or 'nothing'}, "
            f"config says {expected or 'nothing'}"
        )
    clustering = list(table.clustering_fields or [])
    if clustering != list(spec.cluster_by):
        problems.append(
            f"clustered by {clustering or 'nothing'}, "
            f"config says {list(spec.cluster_by) or 'nothing'}"
        )
    return problems


@contextmanager
def _bigquery_errors(table_id: str) -> Generator[None]:
    """Turn Google API, auth and network errors into LoadError.

    The project id is removed from the message: Google often quotes it,
    for example in the request URL.
    """
    label = table_label(table_id)
    project = table_id.removesuffix(f".{label}")

    def reason(error: Exception) -> str:
        return error_reason(error).replace(project, "<project>")

    try:
        yield
    except GoogleAPIError as error:
        lines = [f"BigQuery error: {reason(error)}"]
        # A failed load job puts the actual causes, such as the bad column,
        # in errors rather than in the message.
        details = getattr(error, "errors", None) or []
        lines += [
            f"  {detail['message'].replace(project, '<project>')}"
            for detail in details[:_MAX_ERROR_DETAILS]
            if isinstance(detail, dict) and detail.get("message")
        ]
        raise LoadError(
            "\n".join(f"{label}: {line}" for line in lines)
        ) from error
    except (GoogleAuthError, requests.RequestException) as error:
        # Credentials and the network fail outside the API error types.
        raise LoadError(
            f"{label}: cannot reach BigQuery: {reason(error)}"
        ) from error


_MAX_ERROR_DETAILS = 3
