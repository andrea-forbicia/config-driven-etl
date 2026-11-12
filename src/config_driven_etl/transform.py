"""Generic transformation rules applied from a dataset config."""

from collections.abc import Callable, Sequence

import polars as pl

from config_driven_etl.dataset_config import ColumnType, DatasetConfig

# List of rejection reasons per row. The '#' keeps it apart from target
# names, which are lowercase identifiers.
REJECTIONS = "#rejections"


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


def cast_columns(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Cast string, int, float and bool columns to their Polars types.

    Int, float and bool values are trimmed and blank ones become null. A
    value that cannot be cast becomes null and adds the reason
    '<name>: invalid <type>' to the row's REJECTIONS list. Date, datetime
    and unmapped columns are left as strings.
    """
    casts: dict[str, pl.Expr] = {}
    reasons: list[pl.Expr] = []
    for column in config.columns:
        # String columns are already strings and keep their spaces;
        # temporal columns need a format and are parsed separately.
        cast = _CASTS.get(column.type)
        if cast is None:
            continue
        trimmed = pl.col(column.name).str.strip_chars()
        value = pl.when(trimmed != "").then(trimmed)
        typed = cast(value)
        casts[column.name] = typed
        reasons.append(
            pl.when(value.is_not_null() & typed.is_null()).then(
                pl.lit(f"{column.name}: invalid {column.type}")
            )
        )
    # One with_columns, so the reasons still see the raw strings.
    return lf.with_columns(_rejections(lf, reasons), **casts)


def _to_int(value: pl.Expr) -> pl.Expr:
    return value.cast(pl.Int64, strict=False)


def _to_float(value: pl.Expr) -> pl.Expr:
    number = value.cast(pl.Float64, strict=False)
    # "nan" and "inf" parse as floats but are rarely meant as data, and
    # NaN would slip through min / max comparisons.
    return pl.when(number.is_finite()).then(number)


def _to_bool(value: pl.Expr) -> pl.Expr:
    lowered = value.str.to_lowercase()
    return (
        pl.when(lowered == "true")
        .then(True)
        .when(lowered == "false")
        .then(False)
    )


_CASTS: dict[ColumnType, Callable[[pl.Expr], pl.Expr]] = {
    ColumnType.INT: _to_int,
    ColumnType.FLOAT: _to_float,
    ColumnType.BOOL: _to_bool,
}


def _rejections(lf: pl.LazyFrame, reasons: Sequence[pl.Expr]) -> pl.Expr:
    """REJECTIONS of lf, created if missing, plus the non-null reasons."""
    if REJECTIONS in lf.collect_schema().names():
        # A null list would make concat_list drop the new reasons.
        reasons = [pl.col(REJECTIONS).fill_null([]), *reasons]
    if not reasons:
        return pl.lit([], dtype=pl.List(pl.String)).alias(REJECTIONS)
    return pl.concat_list(reasons).list.drop_nulls().alias(REJECTIONS)
