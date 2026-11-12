"""Runtime settings loaded from environment variables or a local .env."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-level settings shared by every dataset run.

    The GCP fields are optional so that commands which never reach
    BigQuery (validate, extract, transform) run without them.
    Credentials are left to the Google SDK (ADC) so they never pass
    through this object and cannot end up in logs.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    gcp_project_id: str | None = None
    bq_dataset: str | None = None
    data_dir: Path = Path("./data")
