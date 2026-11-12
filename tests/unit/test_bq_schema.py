import unittest
from typing import Any

from google.cloud import bigquery

from config_driven_etl.bq_schema import BIGQUERY_TYPES, bigquery_schema
from config_driven_etl.dataset_config import ColumnType, DatasetConfig


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


if __name__ == "__main__":
    unittest.main()
