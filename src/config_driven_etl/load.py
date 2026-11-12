"""Parquet output and BigQuery load jobs."""

import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

import polars as pl

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
