"""BigQuery schema and table layout derived from a dataset config."""

from dataclasses import dataclass

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
