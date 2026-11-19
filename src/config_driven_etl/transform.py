"""Generic transformation rules applied from a dataset config."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace

import polars as pl
import polars.selectors as cs

from config_driven_etl.dataset_config import (
    MIN_YEAR,
    Column,
    ColumnType,
    DatasetConfig,
    full_match,
)

# List of rejection reasons per row. The '#' keeps it apart from target
# names, which are lowercase identifiers.
REJECTIONS = "#rejections"

# Position of each row in the input, the first record being 1.
ROW_NUMBER = "#row"

# Polars dtype that cast_columns produces for each column type.
POLARS_TYPES: dict[ColumnType, pl.DataType] = {
    ColumnType.STRING: pl.String(),
    ColumnType.INT: pl.Int64(),
    ColumnType.FLOAT: pl.Float64(),
    ColumnType.BOOL: pl.Boolean(),
    ColumnType.DATE: pl.Date(),
    ColumnType.DATETIME: pl.Datetime("us"),
}


class TransformError(Exception):
    """Input data that does not match its dataset config."""


def output_schema(config: DatasetConfig) -> pl.Schema:
    """Schema of the mapped columns after transform, in config order."""
    return pl.Schema(
        {column.name: POLARS_TYPES[column.type] for column in config.columns}
    )


def select_columns(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Rename mapped source columns to their target names.

    Only mapped columns are returned, in config order. Unmapped columns
    are dropped, or fail the run when rules.drop_unmapped_columns is
    false, which catches a source that gained columns. Raises
    TransformError listing missing and unmapped source columns.
    """
    # Resolving the schema of a CSV scan reads only its header.
    available = lf.collect_schema().names()
    mapped = {column.source: column.name for column in config.columns}

    problems = []
    missing = [source for source in mapped if source not in available]
    if missing:
        problems.append(f"missing source column: {_quoted(missing)}")
    unmapped = [name for name in available if name not in mapped]
    if unmapped and not config.rules.drop_unmapped_columns:
        problems.append(f"unmapped source column: {_quoted(unmapped)}")
    if problems:
        raise TransformError("\n".join(problems))

    return lf.select(
        pl.col(source).alias(name) for source, name in mapped.items()
    )


def _quoted(names: Sequence[str]) -> str:
    return ", ".join(f"'{name}'" for name in names)


def raw_column(name: str) -> str:
    """Name of the column holding the raw value of target column name."""
    return f"#raw:{name}"


def keep_raw_values(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Add ROW_NUMBER and a raw copy of every mapped column.

    The copies keep the values exactly as read, so rejected rows can be
    reported with the values that failed, before any rule changed them.
    """
    return lf.with_row_index(ROW_NUMBER, offset=1).with_columns(
        pl.col(ROW_NUMBER).cast(pl.Int64),
        *(
            pl.col(column.name).alias(raw_column(column.name))
            for column in config.columns
        ),
    )


def apply_null_values(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Turn each column's null_values sentinels into null.

    A value matches when its trimmed text equals a sentinel exactly,
    case included; any other value is left as written.
    """
    nulls = {
        column.name: pl.when(
            ~pl.col(column.name).str.strip_chars().is_in(column.null_values)
        ).then(pl.col(column.name))
        for column in config.columns
        if column.null_values
    }
    return lf.with_columns(**nulls)


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
    reasons = [
        pl.when(fails).then(pl.lit(f"{column.name}: {reason}"))
        for column in config.columns
        for reason, fails in _range_failures(column).items()
    ]
    return lf.with_columns(_rejections(lf, reasons))


def check_allowed_values(
    lf: pl.LazyFrame, config: DatasetConfig
) -> pl.LazyFrame:
    """Reject rows with a value outside a column's allowed_values.

    Values are compared after casting, so "01" matches an allowed "1"
    on an int column; string values are compared as written. Adds
    '<name>: not allowed' to the row's REJECTIONS list. Nulls, including
    values cast_columns rejected as invalid, are not compared, and
    neither are values that fail the column's min / max or pattern:
    check_range and check_pattern give them the more specific reason.
    """
    reasons: list[pl.Expr] = []
    for column in config.columns:
        if column.allowed_values is None:
            continue
        value = pl.col(column.name)
        allowed = _typed_allowed_values(column).implode()
        other_failures = list(_range_failures(column).values())
        if (pattern := _pattern_failure(column)) is not None:
            other_failures.append(pattern)
        fails_other_check = (
            pl.any_horizontal(other_failures)
            if other_failures
            else pl.lit(False)
        )
        reasons.append(
            pl.when(
                value.is_not_null()
                & ~value.is_in(allowed)
                & ~fails_other_check
            ).then(pl.lit(f"{column.name}: not allowed"))
        )
    return lf.with_columns(_rejections(lf, reasons))


def check_pattern(lf: pl.LazyFrame, config: DatasetConfig) -> pl.LazyFrame:
    """Reject rows whose value does not fully match a column's pattern.

    Values are matched as written, so "" is checked like any other
    value. Adds '<name>: no pattern match' to the row's REJECTIONS list.
    Nulls are not compared.
    """
    reasons = [
        pl.when(fails).then(pl.lit(f"{column.name}: no pattern match"))
        for column in config.columns
        if (fails := _pattern_failure(column)) is not None
    ]
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

    Valid rows have no reason and lose every internal '#' column, such
    as REJECTIONS; rejected rows keep them all. rejection_counts has
    one row per reason with the number of rows it rejected, most
    frequent first, so a row with two reasons counts once under each.
    """
    # Cached so the valid and rejected frames share one read of the input.
    lf = lf.with_columns(_rejections(lf, [])).cache()
    is_valid = pl.col(REJECTIONS).list.len() == 0
    rejected = lf.filter(~is_valid)
    counts = (
        rejected.select(pl.col(REJECTIONS).explode().alias("reason"))
        .group_by("reason")
        .agg(pl.len().alias("rows"))
        .sort(["rows", "reason"], descending=[True, False])
    )
    return TransformResult(
        valid=lf.filter(is_valid).drop(cs.starts_with("#")),
        rejected=rejected,
        rejection_counts=counts,
    )


def transform(lf: pl.LazyFrame, config: DatasetConfig) -> TransformResult:
    """Turn raw string columns into typed, clean rows.

    Applies every rule of the config and splits the result into valid
    rows, rejected rows and rejection counts. Rejected rows hold
    ROW_NUMBER, the raw string of every mapped column under its target
    name, and REJECTIONS. Raises TransformError if the input columns do
    not match the config.
    """
    lf = keep_raw_values(select_columns(lf, config), config)
    # Duplicates come last: only rows that passed every other rule may
    # claim a key.
    steps = (
        apply_null_values,
        cast_columns,
        check_nullable,
        check_range,
        check_allowed_values,
        check_pattern,
        check_duplicates,
    )
    for step in steps:
        lf = step(lf, config)
    result = split_rejected(lf)
    raw = [
        pl.col(raw_column(column.name)).alias(column.name)
        for column in config.columns
    ]
    return replace(
        result, rejected=result.rejected.select(ROW_NUMBER, *raw, REJECTIONS)
    )


def _range_failures(column: Column) -> dict[str, pl.Expr]:
    """check_range's conditions for column, by reason; null on nulls."""
    value = pl.col(column.name)
    failures: dict[str, pl.Expr] = {}
    if column.min is not None:
        failures["below min"] = value < column.min
    if column.max is not None:
        failures["above max"] = value > column.max
    return failures


def _pattern_failure(column: Column) -> pl.Expr | None:
    """check_pattern's condition for column, or None without a pattern."""
    if column.pattern is None:
        return None
    value = pl.col(column.name)
    return value.is_not_null() & ~value.str.contains(
        full_match(column.pattern)
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


def _typed_allowed_values(column: Column) -> pl.Series:
    # The same casts as the data, so both sides compare as equal values.
    values = pl.Series(column.allowed_values, dtype=pl.String)
    cast = _CASTS.get(column.type)
    if cast is None:
        return values
    return pl.select(cast(pl.lit(values), column)).to_series()


def _rejections(lf: pl.LazyFrame, reasons: Sequence[pl.Expr]) -> pl.Expr:
    """REJECTIONS of lf, created if missing, plus the non-null reasons."""
    if REJECTIONS in lf.collect_schema().names():
        # A null list would make concat_list drop the new reasons.
        reasons = [pl.col(REJECTIONS).fill_null([]), *reasons]
    if not reasons:
        return pl.lit([], dtype=pl.List(pl.String)).alias(REJECTIONS)
    return pl.concat_list(reasons).list.drop_nulls().alias(REJECTIONS)
