import unittest
from pathlib import Path
from typing import Any

import polars as pl
from google.cloud import bigquery

from config_driven_etl.bq_schema import (
    AUDIT_TABLE_SPEC,
    BIGQUERY_TYPES,
    TableSpec,
    bigquery_rejected_schema,
    bigquery_schema,
    rejected_table_spec,
    table_spec,
)
from config_driven_etl.dataset_config import (
    ColumnType,
    DatasetConfig,
    load_dataset_config,
)
from config_driven_etl.pipeline import RunRecorder, StageResult, audit_row
from config_driven_etl.transform import rejected_schema
from fixture_outcomes import OUTCOMES, fixture_config

DATASETS_DIR = Path(__file__).parents[2] / "datasets"


def make_config(columns: list[dict[str, Any]]) -> DatasetConfig:
    return DatasetConfig.model_validate(
        {
            "name": "sample",
            "source": {"url": "https://example.org/sample.csv"},
            "columns": [
                {"source": column["name"], **column} for column in columns
            ],
            "target": {"table": "sample"},
        }
    )


class BigQuerySchemaTest(unittest.TestCase):
    def test_every_column_type_has_a_bigquery_type(self) -> None:
        self.assertEqual(set(BIGQUERY_TYPES), set(ColumnType))

    def test_maps_each_type(self) -> None:
        expected = {
            "string": "STRING",
            "int": "INT64",
            "float": "FLOAT64",
            "bool": "BOOL",
            "date": "DATE",
            "datetime": "DATETIME",
        }
        for type_, bigquery_type in expected.items():
            with self.subTest(type=type_):
                column: dict[str, Any] = {"name": "x", "type": type_}
                if type_ in ("date", "datetime"):
                    column["format"] = "%Y-%m-%d"

                (field,) = bigquery_schema(make_config([column]))

                self.assertEqual(field.field_type, bigquery_type)

    def test_nullable_maps_to_mode(self) -> None:
        schema = bigquery_schema(
            make_config(
                [
                    {"name": "a", "type": "int", "nullable": False},
                    {"name": "b", "type": "int", "nullable": True},
                    {"name": "c", "type": "int"},
                ]
            )
        )

        self.assertEqual(
            [field.mode for field in schema],
            ["REQUIRED", "NULLABLE", "NULLABLE"],
        )

    def test_one_field_per_column_in_config_order(self) -> None:
        schema = bigquery_schema(
            make_config(
                [
                    {"name": "zeta", "type": "string"},
                    {"name": "alpha", "type": "float", "nullable": False},
                ]
            )
        )

        self.assertEqual(
            schema,
            [
                bigquery.SchemaField("zeta", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("alpha", "FLOAT64", mode="REQUIRED"),
            ],
        )


class TableSpecTest(unittest.TestCase):
    def test_takes_schema_and_layout_from_the_config(self) -> None:
        config = DatasetConfig.model_validate(
            {
                "name": "sample",
                "source": {"url": "https://example.org/sample.csv"},
                "columns": [
                    {"source": "s", "name": "s", "type": "string"},
                    {
                        "source": "d",
                        "name": "d",
                        "type": "date",
                        "format": "%Y-%m-%d",
                    },
                ],
                "target": {
                    "table": "sample",
                    "partition_by": "d",
                    "cluster_by": ["s"],
                },
            }
        )

        self.assertEqual(
            table_spec(config),
            TableSpec(
                schema=tuple(bigquery_schema(config)),
                partition_by="d",
                cluster_by=("s",),
            ),
        )

    def test_no_layout_when_the_config_has_none(self) -> None:
        config = make_config([{"name": "s", "type": "string"}])

        spec = table_spec(config)

        self.assertEqual((spec.partition_by, spec.cluster_by), (None, ()))


class BigQueryRejectedSchemaTest(unittest.TestCase):
    def test_fields_and_modes(self) -> None:
        schema = bigquery_rejected_schema(
            make_config(
                [
                    {"name": "zeta", "type": "string"},
                    {"name": "alpha", "type": "int", "nullable": False},
                ]
            )
        )

        self.assertEqual(
            schema,
            [
                bigquery.SchemaField("row_number", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("reasons", "STRING", mode="REPEATED"),
                bigquery.SchemaField(
                    "raw",
                    "RECORD",
                    mode="REQUIRED",
                    fields=[
                        bigquery.SchemaField(
                            "zeta", "STRING", mode="NULLABLE"
                        ),
                        bigquery.SchemaField(
                            "alpha", "STRING", mode="NULLABLE"
                        ),
                    ],
                ),
            ],
        )

    def test_matches_the_rejected_parquet_schema(self) -> None:
        configs = {name: fixture_config(name) for name in OUTCOMES}
        for path in sorted(DATASETS_DIR.glob("*.yaml")):
            configs[path.name] = load_dataset_config(path)
        for label, config in configs.items():
            with self.subTest(config=label):
                expected = rejected_schema(config)
                raw = expected["raw"]
                assert isinstance(raw, pl.Struct)

                schema = bigquery_rejected_schema(config)

                self.assertEqual(
                    [field.name for field in schema], expected.names()
                )
                self.assertEqual(
                    [field.name for field in schema[-1].fields],
                    [field.name for field in raw.fields],
                )


class RejectedTableSpecTest(unittest.TestCase):
    def test_rejected_schema_without_layout(self) -> None:
        config = DatasetConfig.model_validate(
            {
                "name": "sample",
                "source": {"url": "https://example.org/sample.csv"},
                "columns": [{"source": "s", "name": "s", "type": "string"}],
                "target": {
                    "table": "sample",
                    "partition_by": None,
                    "cluster_by": ["s"],
                    "rejected_table": "sample_rejected",
                },
            }
        )

        self.assertEqual(
            rejected_table_spec(config),
            TableSpec(
                schema=tuple(bigquery_rejected_schema(config)),
                partition_by=None,
                cluster_by=(),
            ),
        )


class AuditTableSpecTest(unittest.TestCase):
    def test_never_partitioned_or_clustered(self) -> None:
        self.assertIsNone(AUDIT_TABLE_SPEC.partition_by)
        self.assertEqual(AUDIT_TABLE_SPEC.cluster_by, ())

    def test_every_audit_row_field_is_in_the_schema(self) -> None:
        recorder = RunRecorder("run", "1.2.3")
        recorder.start("transform")
        recorder.record(
            "transform",
            StageResult(
                rows_in=3,
                rows_out=2,
                rows_rejected=1,
                rejections={"id: null": 1},
                dropped_columns=("Notes",),
            ),
            0.5,
        )

        row = audit_row(recorder.finish(0, None))

        fields = {field.name: field for field in AUDIT_TABLE_SPEC.schema}
        self.assertEqual(list(row), list(fields))
        stage_fields = {field.name: field for field in fields["stages"].fields}
        stages = row["stages"]
        assert isinstance(stages, list)
        (stage,) = stages
        self.assertEqual(list(stage), list(stage_fields))
        (rejection,) = stage["rejections"]
        self.assertEqual(
            list(rejection),
            [field.name for field in stage_fields["rejections"].fields],
        )

    def test_dropped_columns_is_a_repeated_string(self) -> None:
        (stages,) = [
            field
            for field in AUDIT_TABLE_SPEC.schema
            if field.name == "stages"
        ]
        (dropped,) = [
            field for field in stages.fields if field.name == "dropped_columns"
        ]
        # REPEATED, never REQUIRED: older audit tables gain it on append.
        self.assertEqual(
            (dropped.field_type, dropped.mode), ("STRING", "REPEATED")
        )


if __name__ == "__main__":
    unittest.main()
