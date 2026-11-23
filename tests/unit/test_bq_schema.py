import unittest
from pathlib import Path
from typing import Any

import polars as pl
from google.cloud import bigquery

from config_driven_etl.bq_schema import (
    AUDIT_TABLE_SPEC,
    BIGQUERY_TYPES,
    SchemaDifference,
    TableSpec,
    bigquery_rejected_schema,
    bigquery_schema,
    compare_schema,
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


def field(
    name: str,
    type_: str = "STRING",
    mode: str = "NULLABLE",
    fields: tuple[bigquery.SchemaField, ...] = (),
) -> bigquery.SchemaField:
    return bigquery.SchemaField(name, type_, mode=mode, fields=fields)


class CompareSchemaTest(unittest.TestCase):
    BASE = (field("id", "INT64", "REQUIRED"), field("name"))

    def compare(
        self, actual: tuple[bigquery.SchemaField, ...]
    ) -> tuple[str, list[tuple[str, str, bool]]]:
        """Compare actual with BASE as expected; kind and differences."""
        diff = compare_schema(actual, self.BASE)
        return diff.kind, [
            (item.path, item.message, item.breaking)
            for item in diff.differences
        ]

    def test_same_schema_has_no_difference(self) -> None:
        self.assertEqual(self.compare(self.BASE), ("none", []))

    def test_legacy_type_names_match_standard_ones(self) -> None:
        # The API returns INTEGER, FLOAT, BOOLEAN and RECORD.
        legacy = compare_schema(
            (
                field("i", "INTEGER"),
                field("f", "FLOAT"),
                field("b", "BOOLEAN"),
                field("r", "RECORD", fields=(field("s"),)),
            ),
            (
                field("i", "INT64"),
                field("f", "FLOAT64"),
                field("b", "BOOL"),
                field("r", "STRUCT", fields=(field("s"),)),
            ),
        )

        self.assertEqual(legacy.kind, "none")

    def test_new_nullable_column_at_the_end_is_additive(self) -> None:
        actual = (self.BASE[0],)

        self.assertEqual(
            self.compare(actual),
            ("additive", [("name", "added as NULLABLE", False)]),
        )

    def test_new_repeated_column_at_the_end_is_additive(self) -> None:
        diff = compare_schema(
            self.BASE, (*self.BASE, field("tags", mode="REPEATED"))
        )

        self.assertEqual(diff.kind, "additive")

    def test_new_required_column_is_breaking(self) -> None:
        diff = compare_schema(
            self.BASE, (*self.BASE, field("code", mode="REQUIRED"))
        )

        self.assertEqual(
            [(d.path, d.message, d.breaking) for d in diff.differences],
            [("code", "added as REQUIRED", True)],
        )
        self.assertEqual(diff.kind, "breaking")

    def test_new_column_before_existing_ones_is_breaking(self) -> None:
        actual = (self.BASE[1],)

        self.assertEqual(
            self.compare(actual),
            ("breaking", [("id", "added before existing columns", True)]),
        )

    def test_removed_column_is_breaking(self) -> None:
        actual = (*self.BASE, field("old", "DATE"))

        self.assertEqual(
            self.compare(actual), ("breaking", [("old", "removed", True)])
        )

    def test_rename_is_a_removal_and_an_addition(self) -> None:
        actual = (self.BASE[0], field("title"))

        self.assertEqual(
            self.compare(actual),
            (
                "breaking",
                [
                    ("title", "removed", True),
                    ("name", "added as NULLABLE", False),
                ],
            ),
        )

    def test_type_change_is_breaking(self) -> None:
        actual = (field("id", "STRING", "REQUIRED"), self.BASE[1])

        self.assertEqual(
            self.compare(actual),
            ("breaking", [("id", "type STRING, config says INT64", True)]),
        )

    def test_relaxing_required_to_nullable_is_additive(self) -> None:
        actual = (self.BASE[0], field("name", mode="REQUIRED"))

        self.assertEqual(
            self.compare(actual),
            (
                "additive",
                [("name", "mode REQUIRED, config says NULLABLE", False)],
            ),
        )

    def test_tightening_nullable_to_required_is_breaking(self) -> None:
        actual = (field("id", "INT64"), self.BASE[1])

        self.assertEqual(
            self.compare(actual),
            (
                "breaking",
                [("id", "mode NULLABLE, config says REQUIRED", True)],
            ),
        )

    def test_repeated_mode_change_is_breaking(self) -> None:
        diff = compare_schema(
            (field("tags", mode="REPEATED"),), (field("tags"),)
        )

        self.assertEqual(diff.kind, "breaking")

    def test_reordered_columns_are_breaking(self) -> None:
        actual = (self.BASE[1], self.BASE[0])

        self.assertEqual(
            self.compare(actual),
            ("breaking", [("", "columns are in a different order", True)]),
        )

    def test_names_are_case_sensitive(self) -> None:
        actual = (self.BASE[0], field("Name"))

        kind, differences = self.compare(actual)

        self.assertEqual(kind, "breaking")
        self.assertIn(("Name", "removed", True), differences)

    def test_nested_fields_are_compared_with_their_path(self) -> None:
        actual = (field("raw", "RECORD", fields=(field("a"),)),)
        expected = (field("raw", "RECORD", fields=(field("a"), field("b"))),)

        diff = compare_schema(actual, expected)

        self.assertEqual(
            [(d.path, d.message, d.breaking) for d in diff.differences],
            [("raw.b", "added as NULLABLE", False)],
        )

    def test_nested_order_change_names_its_record(self) -> None:
        actual = (field("raw", "RECORD", fields=(field("b"), field("a"))),)
        expected = (field("raw", "RECORD", fields=(field("a"), field("b"))),)

        diff = compare_schema(actual, expected)

        self.assertEqual(
            diff.differences,
            (
                SchemaDifference(
                    "raw", "columns are in a different order", True
                ),
            ),
        )

    def test_every_difference_is_listed(self) -> None:
        actual = (field("id", "STRING", "REQUIRED"), field("old"))

        _, differences = self.compare(actual)

        self.assertEqual(
            differences,
            [
                ("id", "type STRING, config says INT64", True),
                ("old", "removed", True),
                ("name", "added as NULLABLE", False),
            ],
        )

    def listed(
        self,
        actual: tuple[bigquery.SchemaField, ...],
        expected: tuple[bigquery.SchemaField, ...],
    ) -> tuple[str, list[tuple[str, str, bool]]]:
        diff = compare_schema(actual, expected)
        return diff.kind, [
            (item.path, item.message, item.breaking)
            for item in diff.differences
        ]

    def test_additions_and_removals_alone_are_no_reorder(self) -> None:
        a, b, c, d = (field(name) for name in "abcd")

        self.assertEqual(
            self.listed((a, b, c), (a, c, d)),
            (
                "breaking",
                [("b", "removed", True), ("d", "added as NULLABLE", False)],
            ),
        )

    def test_reorder_is_found_among_additions(self) -> None:
        a, b, x = (field(name) for name in "abx")

        self.assertEqual(
            self.listed((a, b), (b, x, a)),
            (
                "breaking",
                [
                    ("", "columns are in a different order", True),
                    ("x", "added before existing columns", True),
                ],
            ),
        )

    def test_record_and_scalar_swaps_are_type_changes(self) -> None:
        record = field("raw", "RECORD", fields=(field("a"),))
        scalar = field("raw")

        for actual, expected, message in [
            (record, scalar, "type RECORD, config says STRING"),
            (scalar, record, "type STRING, config says RECORD"),
        ]:
            with self.subTest(message=message):
                self.assertEqual(
                    self.listed((actual,), (expected,)),
                    ("breaking", [("raw", message, True)]),
                )

    def test_nested_modes_follow_the_same_rule(self) -> None:
        def raw(mode: str) -> bigquery.SchemaField:
            return field("raw", "RECORD", fields=(field("a", mode=mode),))

        self.assertEqual(
            self.listed((raw("REQUIRED"),), (raw("NULLABLE"),)),
            (
                "additive",
                [("raw.a", "mode REQUIRED, config says NULLABLE", False)],
            ),
        )
        self.assertEqual(
            self.listed((raw("NULLABLE"),), (raw("REQUIRED"),)),
            (
                "breaking",
                [("raw.a", "mode NULLABLE, config says REQUIRED", True)],
            ),
        )

    def test_repeated_records_are_compared_field_by_field(self) -> None:
        def raw(mode: str, *names: str) -> bigquery.SchemaField:
            fields = tuple(field(name) for name in names)
            return field("raw", "RECORD", mode=mode, fields=fields)

        self.assertEqual(
            self.listed((raw("REPEATED", "a"),), (raw("REPEATED", "a", "b"),)),
            ("additive", [("raw.b", "added as NULLABLE", False)]),
        )
        self.assertEqual(
            self.listed((raw("NULLABLE", "a"),), (raw("REPEATED", "a", "b"),)),
            (
                "breaking",
                [
                    ("raw", "mode NULLABLE, config says REPEATED", True),
                    ("raw.b", "added as NULLABLE", False),
                ],
            ),
        )

    def test_nested_field_added_before_existing_ones_is_breaking(self) -> None:
        actual = (field("raw", "RECORD", fields=(field("a"),)),)
        expected = (field("raw", "RECORD", fields=(field("x"), field("a"))),)

        self.assertEqual(
            self.listed(actual, expected),
            ("breaking", [("raw.x", "added before existing columns", True)]),
        )

    def test_spec_schemas_match_themselves(self) -> None:
        for name in OUTCOMES:
            config = fixture_config(name)
            for spec in (table_spec(config), rejected_table_spec(config)):
                with self.subTest(dataset=name):
                    diff = compare_schema(spec.schema, spec.schema)
                    self.assertEqual(diff.kind, "none")


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
