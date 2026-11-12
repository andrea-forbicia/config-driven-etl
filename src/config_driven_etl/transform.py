"""Generic transformation rules applied from a dataset config."""

import polars as pl

from config_driven_etl.dataset_config import DatasetConfig


class TransformError(Exception):
    """Input data that does not match its dataset config."""


def select_columns(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Rename mapped source columns to their target names.

    Mapped columns come first, in config order. Unmapped columns are
    dropped, or kept after them under their source names when
    rules.drop_unmapped_columns is false. Raises TransformError if a mapped
    source column is missing or a kept column clashes with a target name.
    """
    # Resolving the schema of a CSV scan reads only its header.
    available = lf.collect_schema().names()
    mapped = {column.source: column.name for column in config.columns}

    missing = [source for source in mapped if source not in available]
    if missing:
        listed = ", ".join(f"'{source}'" for source in missing)
        raise TransformError(f"missing source column: {listed}")

    kept: list[str] = []
    if not config.rules.drop_unmapped_columns:
        kept = [name for name in available if name not in mapped]
        clashes = [name for name in kept if name in mapped.values()]
        if clashes:
            listed = ", ".join(f"'{name}'" for name in clashes)
            raise TransformError(
                f"unmapped source column clashes with a target name: {listed}"
            )

    return lf.select(
        *(pl.col(source).alias(name) for source, name in mapped.items()),
        *kept,
    )
