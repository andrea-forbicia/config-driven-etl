import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from config_driven_etl.settings import Settings


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
