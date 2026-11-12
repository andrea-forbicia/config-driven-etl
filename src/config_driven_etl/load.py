"""Parquet output and BigQuery load jobs."""

import os
import tempfile
from pathlib import Path

import polars as pl


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
