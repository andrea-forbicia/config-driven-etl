"""Generic transformation rules applied from a dataset config."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import polars as pl

from config_driven_etl.dataset_config import (
    Column,
    ColumnType,
    DatasetConfig,
)

# List of rejection reasons per row. The '#' keeps it apart from target
# names, which are lowercase identifiers.
REJECTIONS = "#rejections"

# Earlier dates and datetimes are rejected as malformed.
MIN_YEAR = 1000


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
    """Cast every mapped column to the Polars type of its config type.

    Date and datetime values are parsed with the column's format;
    datetimes have microsecond precision, offsets become naive UTC, and
    years before MIN_YEAR are invalid. Non-string values are trimmed
    and blank ones become null. A value that cannot be cast becomes null
    and adds the reason '<name>: invalid <type>' to the row's REJECTIONS
    list. String values and unmapped columns are left as written.
    """
    casts: dict[str, pl.Expr] = {}
    reasons: list[pl.Expr] = []
    for column in config.columns:
        cast = _CASTS.get(column.type)
        if cast is None:
            continue
        trimmed = pl.col(column.name).str.strip_chars()
        value = pl.when(trimmed != "").then(trimmed)
        typed = cast(value, column)
        casts[column.name] = typed
        reasons.append(
            pl.when(value.is_not_null() & typed.is_null()).then(
                pl.lit(_invalid_reason(column))
            )
        )
    # One with_columns, so the reasons still see the raw strings.
    return lf.with_columns(_rejections(lf, reasons), **casts)


def check_nullable(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Reject rows with a null in a column that is not nullable.

    Adds '<name>: null' to the row's REJECTIONS list, unless the value
    is null because cast_columns already rejected it as invalid, so each
    bad value is counted once. Any non-null value is present, including
    an empty string.
    """
    earlier = (
        pl.col(REJECTIONS).fill_null([])
        if REJECTIONS in lf.collect_schema().names()
        else pl.lit([], dtype=pl.List(pl.String))
    )
    reasons: list[pl.Expr] = []
    for column in config.columns:
        if column.nullable:
            continue
        already_invalid = earlier.list.contains(_invalid_reason(column))
        reasons.append(
            pl.when(pl.col(column.name).is_null() & ~already_invalid).then(
                pl.lit(f"{column.name}: null")
            )
        )
    return lf.with_columns(_rejections(lf, reasons))


def check_range(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Reject rows with a value outside a column's min / max bounds.

    Bounds are inclusive. Adds '<name>: below min' or '<name>: above max'
    to the row's REJECTIONS list. Nulls, including values cast_columns
    rejected as invalid, are not compared.
    """
    reasons: list[pl.Expr] = []
    for column in config.columns:
        value = pl.col(column.name)
        if column.min is not None:
            reasons.append(
                pl.when(value < column.min).then(
                    pl.lit(f"{column.name}: below min")
                )
            )
        if column.max is not None:
            reasons.append(
                pl.when(value > column.max).then(
                    pl.lit(f"{column.name}: above max")
                )
            )
    return lf.with_columns(_rejections(lf, reasons))


def check_duplicates(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Reject repeated keys from rules.deduplicate_on.

    Only rows not rejected yet take part, so this runs after the other
    rules: the first valid row of each key is kept and later ones get
    '<keys>: duplicate'. Keys compare typed values: strings exactly as
    written, spaces included. Null key values match each other.
    """
    keys = list(config.rules.deduplicate_on)
    if not keys:
        return lf.with_columns(_rejections(lf, []))
    # Without earlier rules every row is still valid.
    valid = (
        pl.col(REJECTIONS).fill_null([]).list.len() == 0
        if REJECTIONS in lf.collect_schema().names()
        else pl.lit(True)
    )
    types = {column.name: column.type for column in config.columns}
    # Windows group floats bitwise, so -0.0 would differ from 0.0;
    # adding 0.0 turns -0.0 into 0.0.
    key_values = [
        pl.col(key) + 0.0 if types[key] is ColumnType.FLOAT else pl.col(key)
        for key in keys
    ]
    # Rejected rows sit in their own partition, so they never claim a key.
    seen_before = pl.int_range(pl.len()).over(valid, *key_values) > 0
    reason = pl.when(valid & seen_before).then(
        pl.lit(f"{', '.join(keys)}: duplicate")
    )
    return lf.with_columns(_rejections(lf, [reason]))


@dataclass(frozen=True)
class TransformResult:
    """Clean rows, rejected rows and rejection counts, all lazy.

    The three frames share one plan; collect them together with
    pl.collect_all so the input is read once.
    """

    valid: pl.LazyFrame
    rejected: pl.LazyFrame
    rejection_counts: pl.LazyFrame


def split_rejected(lf: pl.LazyFrame) -> TransformResult:
    """Split rows by their REJECTIONS list and count rejections.

    Valid rows have no reason and lose the REJECTIONS column; rejected
    rows keep it. rejection_counts has one row per reason with the
    number of rows it rejected, most frequent first, so a row with two
    reasons counts once under each.
    """
    lf = lf.with_columns(_rejections(lf, []))
    is_valid = pl.col(REJECTIONS).list.len() == 0
    rejected = lf.filter(~is_valid)
    counts = (
        rejected.select(pl.col(REJECTIONS).explode().alias("reason"))
        .group_by("reason")
        .agg(pl.len().alias("rows"))
        .sort(["rows", "reason"], descending=[True, False])
    )
    return TransformResult(
        valid=lf.filter(is_valid).drop(REJECTIONS),
        rejected=rejected,
        rejection_counts=counts,
    )


def _invalid_reason(column: Column) -> str:
    return f"{column.name}: invalid {column.type}"


def _to_int(value: pl.Expr, column: Column) -> pl.Expr:
    return value.cast(pl.Int64, strict=False)


def _to_float(value: pl.Expr, column: Column) -> pl.Expr:
    number = value.cast(pl.Float64, strict=False)
    # "nan" and "inf" parse as floats but are rarely meant as data, and
    # NaN would slip through min / max comparisons.
    return pl.when(number.is_finite()).then(number)


def _to_bool(value: pl.Expr, column: Column) -> pl.Expr:
    lowered = value.str.to_lowercase()
    return (
        pl.when(lowered == "true")
        .then(True)
        .when(lowered == "false")
        .then(False)
    )


def _to_date(value: pl.Expr, column: Column) -> pl.Expr:
    return _plausible_year(value.str.to_date(column.format, strict=False))


def _to_datetime(value: pl.Expr, column: Column) -> pl.Expr:
    # BigQuery stores microseconds; finer input would be truncated anyway.
    parsed = value.str.to_datetime(column.format, time_unit="us", strict=False)
    # Offsets (%z) are converted to UTC by the parser; dropping the zone
    # keeps one datetime dtype whatever the format.
    return _plausible_year(parsed.dt.replace_time_zone(None))


def _plausible_year(parsed: pl.Expr) -> pl.Expr:
    # %Y also accepts two- or three-digit years, so "26-01-02" would
    # silently become year 26 instead of being rejected.
    return pl.when(parsed.dt.year() >= MIN_YEAR).then(parsed)


# String columns are already strings and keep their spaces.
_CASTS: dict[ColumnType, Callable[[pl.Expr, Column], pl.Expr]] = {
    ColumnType.INT: _to_int,
    ColumnType.FLOAT: _to_float,
    ColumnType.BOOL: _to_bool,
    ColumnType.DATE: _to_date,
    ColumnType.DATETIME: _to_datetime,
}


def _rejections(lf: pl.LazyFrame, reasons: Sequence[pl.Expr]) -> pl.Expr:
    """REJECTIONS of lf, created if missing, plus the non-null reasons."""
    if REJECTIONS in lf.collect_schema().names():
        # A null list would make concat_list drop the new reasons.
        reasons = [pl.col(REJECTIONS).fill_null([]), *reasons]
    if not reasons:
        return pl.lit([], dtype=pl.List(pl.String)).alias(REJECTIONS)
    return pl.concat_list(reasons).list.drop_nulls().alias(REJECTIONS)
