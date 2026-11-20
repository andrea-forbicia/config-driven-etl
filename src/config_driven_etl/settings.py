"""Runtime settings loaded from environment variables or a local .env."""

from pathlib import Path
from typing import Self

from pydantic import ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from config_driven_etl.dataset_config import Identifier
from config_driven_etl.logging import LogFormat


class SettingsError(Exception):
    """A required environment setting is missing or invalid."""


class LogSettings(BaseSettings):
    """The log format alone, read before any other setting.

    Kept apart so that an invalid setting is still reported in the
    format that was asked for.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    log_format: LogFormat = "json"


class Settings(LogSettings):
    """Environment-level settings shared by every dataset run.

    The GCP fields are optional so that commands which never reach
    BigQuery (validate, extract, transform) run without them.
    Credentials are left to the Google SDK (ADC) so they never pass
    through this object and cannot end up in logs.
    """

    gcp_project_id: str | None = None
    bq_dataset: str | None = None
    data_dir: Path = Path("./data")
    bq_audit_table: Identifier | None = None

    @model_validator(mode="after")
    def _audit_table_needs_a_dataset(self) -> Self:
        # Every command appends to it, including those that never load.
        if self.bq_audit_table and not (
            self.gcp_project_id and self.bq_dataset
        ):
            raise ValueError(
                "BQ_AUDIT_TABLE needs GCP_PROJECT_ID and BQ_DATASET"
            )
        return self


def load_log_format() -> LogFormat:
    """Read LOG_FORMAT alone; JSON if it is invalid.

    An invalid value is reported by load_settings, in JSON.
    """
    try:
        return LogSettings().log_format
    except ValidationError:
        return "json"


def load_settings() -> Settings:
    """Read the settings, raising SettingsError if any is invalid.

    The message names each setting and the problem, never the value,
    which may be a project id.
    """
    try:
        return Settings()
    except ValidationError as error:
        problems = [
            _problem(detail["loc"], detail["msg"])
            for detail in error.errors(include_input=False)
        ]
        # Not chained: the original message quotes the values.
        raise SettingsError("\n".join(problems)) from None


def _problem(location: tuple[int | str, ...], message: str) -> str:
    message = message.removeprefix("Value error, ")
    if not location:
        return message
    return f"{str(location[0]).upper()}: {message}"
