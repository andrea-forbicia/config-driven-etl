"""BigQuery schema and table layout derived from a dataset config."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from google.cloud import bigquery

from config_driven_etl.dataset_config import ColumnType, DatasetConfig

# Datetimes are naive (offsets are converted to UTC by the transform), so
# they map to DATETIME, which has no time zone, rather than TIMESTAMP.
BIGQUERY_TYPES: dict[ColumnType, str] = {
    ColumnType.STRING: "STRING",
    ColumnType.INT: "INT64",
    ColumnType.FLOAT: "FLOAT64",
    ColumnType.BOOL: "BOOL",
    ColumnType.DATE: "DATE",
    ColumnType.DATETIME: "DATETIME",
}


@dataclass(frozen=True)
class TableSpec:
    """What a BigQuery table must look like: schema and layout."""

    schema: tuple[bigquery.SchemaField, ...]
    partition_by: str | None
    cluster_by: tuple[str, ...]


def table_spec(config: DatasetConfig) -> TableSpec:
    """The target table: config schema, partitioning and clustering."""
    return TableSpec(
        schema=tuple(bigquery_schema(config)),
        partition_by=config.target.partition_by,
        cluster_by=config.target.cluster_by,
    )


def rejected_table_spec(config: DatasetConfig) -> TableSpec:
    """The rejected rows table: never partitioned or clustered."""
    return TableSpec(
        schema=tuple(bigquery_rejected_schema(config)),
        partition_by=None,
        cluster_by=(),
    )


def bigquery_schema(config: DatasetConfig) -> list[bigquery.SchemaField]:
    """One field per config column, in config order.

    Non-nullable columns are REQUIRED; the transform has already rejected
    rows with a null there.
    """
    return [
        bigquery.SchemaField(
            column.name,
            BIGQUERY_TYPES[column.type],
            mode="NULLABLE" if column.nullable else "REQUIRED",
        )
        for column in config.columns
    ]


def bigquery_rejected_schema(
    config: DatasetConfig,
) -> list[bigquery.SchemaField]:
    """Fields of transform.rejected_schema, in the same order.

    raw keeps every value as read, so its fields are STRING and NULLABLE
    whatever the column's type and nullable rule.
    """
    raw = [
        bigquery.SchemaField(column.name, "STRING", mode="NULLABLE")
        for column in config.columns
    ]
    return [
        bigquery.SchemaField("row_number", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("reasons", "STRING", mode="REPEATED"),
        bigquery.SchemaField("raw", "RECORD", mode="REQUIRED", fields=raw),
    ]


@dataclass(frozen=True)
class SchemaDifference:
    """One way an existing table differs from the expected schema."""

    # Dotted for nested fields, e.g. "raw.city"; "" for the top level.
    path: str
    message: str
    breaking: bool


@dataclass(frozen=True)
class SchemaDiff:
    """Every difference between an existing and an expected schema."""

    differences: tuple[SchemaDifference, ...]

    @property
    def kind(self) -> Literal["none", "additive", "breaking"]:
        if any(item.breaking for item in self.differences):
            return "breaking"
        return "additive" if self.differences else "none"


# The API returns the legacy names; the specs use the standard ones.
_STANDARD_TYPES = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
    "STRUCT": "RECORD",
}


def compare_schema(
    actual: Sequence[bigquery.SchemaField],
    expected: Sequence[bigquery.SchemaField],
) -> SchemaDiff:
    """Compare the schema of an existing table with the expected one.

    Additive differences are those BigQuery itself accepts when it
    updates a schema on append: a NULLABLE or REPEATED field added after
    the existing ones, or a REQUIRED field relaxed to NULLABLE. Anything
    else is breaking: a field removed (a rename is a removal plus an
    addition), retyped, made REQUIRED, added before existing fields, or
    moved. RECORD fields are compared field by field. Names are case
    sensitive.
    """
    return SchemaDiff(tuple(_differences(actual, expected, "")))


def _differences(
    actual: Sequence[bigquery.SchemaField],
    expected: Sequence[bigquery.SchemaField],
    prefix: str,
) -> list[SchemaDifference]:
    wanted = {field.name: field for field in expected}
    differences = []
    for field in actual:
        path = prefix + field.name
        if field.name not in wanted:
            differences.append(SchemaDifference(path, "removed", True))
        else:
            differences += _field_differences(field, wanted[field.name], path)

    existing = {field.name for field in actual}
    kept = [field.name for field in expected if field.name in existing]
    if kept != [field.name for field in actual if field.name in wanted]:
        differences.append(
            SchemaDifference(
                prefix.removesuffix("."),
                "columns are in a different order",
                True,
            )
        )

    for index, field in enumerate(expected):
        if field.name in existing:
            continue
        path = prefix + field.name
        later = [other.name for other in expected[index + 1 :]]
        if any(name in existing for name in later):
            differences.append(
                SchemaDifference(path, "added before existing columns", True)
            )
        else:
            differences.append(
                SchemaDifference(
                    path,
                    f"added as {field.mode}",
                    field.mode == "REQUIRED",
                )
            )
    return differences


def _field_differences(
    actual: bigquery.SchemaField,
    expected: bigquery.SchemaField,
    path: str,
) -> list[SchemaDifference]:
    actual_type = _standard_type(actual.field_type)
    expected_type = _standard_type(expected.field_type)
    if actual_type != expected_type:
        return [
            SchemaDifference(
                path, f"type {actual_type}, config says {expected_type}", True
            )
        ]
    differences = []
    if actual.mode != expected.mode:
        relaxed = (actual.mode, expected.mode) == ("REQUIRED", "NULLABLE")
        differences.append(
            SchemaDifference(
                path,
                f"mode {actual.mode}, config says {expected.mode}",
                not relaxed,
            )
        )
    if actual_type == "RECORD":
        differences += _differences(actual.fields, expected.fields, f"{path}.")
    return differences


def _standard_type(field_type: str | None) -> str:
    name = (field_type or "").upper()
    return _STANDARD_TYPES.get(name, name)


def _field(
    name: str,
    type_: str,
    mode: str = "REQUIRED",
    fields: tuple[bigquery.SchemaField, ...] = (),
) -> bigquery.SchemaField:
    return bigquery.SchemaField(name, type_, mode=mode, fields=fields)


# One row per pipeline command, as built by pipeline.audit_row.
AUDIT_TABLE_SPEC = TableSpec(
    schema=(
        _field("run_id", "STRING"),
        _field("command", "STRING"),
        _field("version", "STRING"),
        _field("dataset", "STRING", "NULLABLE"),
        _field("started_at", "TIMESTAMP"),
        _field("duration_s", "FLOAT64"),
        _field("outcome", "STRING"),
        _field("exit_code", "INT64"),
        _field("category", "STRING", "NULLABLE"),
        _field("failed_stage", "STRING", "NULLABLE"),
        _field(
            "stages",
            "RECORD",
            "REPEATED",
            fields=(
                _field("stage", "STRING"),
                _field("rows_in", "INT64", "NULLABLE"),
                _field("rows_out", "INT64"),
                _field("rows_rejected", "INT64"),
                _field("duration_s", "FLOAT64"),
                _field(
                    "rejections",
                    "RECORD",
                    "REPEATED",
                    fields=(
                        _field("reason", "STRING"),
                        _field("rows", "INT64"),
                    ),
                ),
                # Fields added later are last and never REQUIRED, so an
                # audit table made by an older version can gain them.
                _field("dropped_columns", "STRING", "REPEATED"),
            ),
        ),
    ),
    partition_by=None,
    cluster_by=(),
)
