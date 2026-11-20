import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from config_driven_etl.settings import (
    Settings,
    SettingsError,
    load_log_format,
    load_settings,
)


class SettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        # Settings reads .env from the working directory; isolate it from
        # any real .env in the repository. The temp dir must exist before
        # the environment is cleared, or tempfile falls back to the cwd.
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.addCleanup(os.chdir, Path.cwd())
        os.chdir(directory.name)

        patcher = mock.patch.dict(os.environ, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_defaults_without_environment(self) -> None:
        settings = Settings()

        self.assertIsNone(settings.gcp_project_id)
        self.assertIsNone(settings.bq_dataset)
        self.assertEqual(settings.data_dir, Path("./data"))

    def test_reads_environment_variables(self) -> None:
        os.environ.update(
            GCP_PROJECT_ID="my-project",
            BQ_DATASET="my_dataset",
            DATA_DIR="/tmp/etl",
        )

        settings = Settings()

        self.assertEqual(settings.gcp_project_id, "my-project")
        self.assertEqual(settings.bq_dataset, "my_dataset")
        self.assertEqual(settings.data_dir, Path("/tmp/etl"))

    def test_empty_values_fall_back_to_defaults(self) -> None:
        os.environ.update(GCP_PROJECT_ID="", DATA_DIR="")

        settings = Settings()

        self.assertIsNone(settings.gcp_project_id)
        self.assertEqual(settings.data_dir, Path("./data"))

    def test_reads_env_file(self) -> None:
        Path(".env").write_text("BQ_DATASET=from_file\n", encoding="utf-8")

        settings = Settings()

        self.assertEqual(settings.bq_dataset, "from_file")

    def test_environment_overrides_env_file(self) -> None:
        Path(".env").write_text("BQ_DATASET=from_file\n", encoding="utf-8")
        os.environ["BQ_DATASET"] = "from_env"

        settings = Settings()

        self.assertEqual(settings.bq_dataset, "from_env")

    def test_ignores_unknown_keys_in_env_file(self) -> None:
        Path(".env").write_text(
            "GOOGLE_APPLICATION_CREDENTIALS=/keys/sa.json\n", encoding="utf-8"
        )

        settings = Settings()

        self.assertNotIn(
            "google_application_credentials", settings.model_dump()
        )

    def test_log_format_defaults_to_json(self) -> None:
        self.assertEqual(load_log_format(), "json")
        self.assertEqual(load_settings().log_format, "json")

    def test_reads_the_log_format(self) -> None:
        os.environ["LOG_FORMAT"] = "pretty"

        self.assertEqual(load_log_format(), "pretty")
        self.assertEqual(load_settings().log_format, "pretty")

    def test_log_format_is_read_despite_other_invalid_settings(self) -> None:
        os.environ.update(LOG_FORMAT="pretty", BQ_AUDIT_TABLE="Bad-Name")

        self.assertEqual(load_log_format(), "pretty")
        with self.assertRaises(SettingsError):
            load_settings()

    def test_invalid_log_format_falls_back_to_json(self) -> None:
        os.environ["LOG_FORMAT"] = "fancy"

        self.assertEqual(load_log_format(), "json")
        with self.assertRaises(SettingsError) as context:
            load_settings()
        self.assertTrue(
            str(context.exception).startswith("LOG_FORMAT: "),
            str(context.exception),
        )
        self.assertNotIn("fancy", str(context.exception))

    def test_audit_table_is_optional(self) -> None:
        self.assertIsNone(Settings().bq_audit_table)

    def test_empty_audit_table_is_unset(self) -> None:
        # .env.example ships the key with an empty value.
        os.environ["BQ_AUDIT_TABLE"] = ""

        self.assertIsNone(load_settings().bq_audit_table)

    def test_reads_the_audit_table(self) -> None:
        os.environ.update(
            GCP_PROJECT_ID="my-project",
            BQ_DATASET="my_dataset",
            BQ_AUDIT_TABLE="etl_runs",
        )

        self.assertEqual(load_settings().bq_audit_table, "etl_runs")

    def test_invalid_audit_table_names_the_setting_not_the_value(
        self,
    ) -> None:
        os.environ.update(
            GCP_PROJECT_ID="secret-project-4f1c",
            BQ_DATASET="my_dataset",
            BQ_AUDIT_TABLE="Bad-Name",
        )

        with self.assertRaises(SettingsError) as context:
            load_settings()

        message = str(context.exception)
        self.assertTrue(message.startswith("BQ_AUDIT_TABLE: "), message)
        self.assertNotIn("Bad-Name", message)
        self.assertNotIn("secret-project-4f1c", message)
        self.assertIsNone(context.exception.__cause__)

    def test_audit_table_needs_project_and_dataset(self) -> None:
        for missing in ["GCP_PROJECT_ID", "BQ_DATASET"]:
            with self.subTest(missing=missing):
                os.environ.update(
                    GCP_PROJECT_ID="my-project",
                    BQ_DATASET="my_dataset",
                    BQ_AUDIT_TABLE="etl_runs",
                )
                del os.environ[missing]

                with self.assertRaises(SettingsError) as context:
                    load_settings()

                self.assertEqual(
                    str(context.exception),
                    "BQ_AUDIT_TABLE needs GCP_PROJECT_ID and BQ_DATASET",
                )
