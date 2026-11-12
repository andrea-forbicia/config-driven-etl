"""Command-line interface."""

import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import requests
import typer
from google.cloud import bigquery

from config_driven_etl import __version__
from config_driven_etl.dataset_config import (
    DatasetConfig,
    DatasetConfigError,
    load_dataset_config,
)
from config_driven_etl.extract import ExtractError
from config_driven_etl.load import LoadError
from config_driven_etl.logging import (
    configure_logging,
    log_error,
    log_event,
)
from config_driven_etl.pipeline import (
    StageResult,
    extract_dataset,
    load_dataset,
    transform_dataset,
)
from config_driven_etl.settings import Settings
from config_driven_etl.transform import TransformError

app = typer.Typer(no_args_is_help=True)

DatasetFile = Annotated[
    Path,
    typer.Argument(help="Dataset YAML file.", show_default=False),
]


def _print_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_print_version,
            is_eager=True,
            help="Show the version and exit.",
        ),
    ] = False,
) -> None:
    """Generic batch ETL from open-data CSV to BigQuery."""


@app.command()
def validate(
    paths: Annotated[
        list[Path],
        typer.Argument(
            help="Dataset YAML files, or directories whose *.yaml files "
            "are all validated.",
            show_default=False,
        ),
    ],
) -> None:
    """Validate dataset config files without touching any data.

    Every file is checked; exits with code 1 if any is invalid.
    """
    valid = True
    for path in paths:
        if not path.is_dir():
            valid &= _validate_file(path)
            continue
        files = sorted(path.glob("*.yaml"))
        # An empty directory would otherwise pass without checking anything.
        if not files:
            typer.echo(f"{path}: no *.yaml dataset files found", err=True)
            valid = False
        for file in files:
            valid &= _validate_file(file)
    if not valid:
        raise typer.Exit(code=1)


def _validate_file(path: Path) -> bool:
    try:
        config = load_dataset_config(path)
    except DatasetConfigError as error:
        typer.echo(str(error), err=True)
        return False
    typer.echo(f"ok: {path} ({config.name}, {len(config.columns)} columns)")
    return True


@app.command()
def extract(dataset: DatasetFile) -> None:
    """Download or copy the raw CSV into DATA_DIR/raw/<name>/."""
    with _command(dataset):
        config = load_dataset_config(dataset)
        _extract(config, Settings())


@app.command()
def transform(dataset: DatasetFile) -> None:
    """Clean the raw CSV into DATA_DIR/processed/<name>/<name>.parquet."""
    with _command(dataset):
        config = load_dataset_config(dataset)
        _transform(config, Settings())


@app.command()
def load(dataset: DatasetFile) -> None:
    """Load the processed Parquet into BigQuery, replacing the table data."""
    with _command(dataset):
        config = load_dataset_config(dataset)
        _load(config, Settings())


@app.command()
def run(dataset: DatasetFile) -> None:
    """Extract, transform and load one dataset."""
    with _command(dataset):
        config = load_dataset_config(dataset)
        settings = Settings()
        # Checked first so a missing setting does not waste a download.
        _bigquery_target(settings)
        _extract(config, settings)
        _transform(config, settings)
        _load(config, settings)


def _extract(config: DatasetConfig, settings: Settings) -> None:
    started = time.perf_counter()
    with requests.Session() as session:
        result = extract_dataset(config, settings.data_dir, session)
    _log_stage("extract", config, result, started)


def _transform(config: DatasetConfig, settings: Settings) -> None:
    started = time.perf_counter()
    result = transform_dataset(config, settings.data_dir)
    _log_stage("transform", config, result, started)


def _load(config: DatasetConfig, settings: Settings) -> None:
    started = time.perf_counter()
    project, dataset = _bigquery_target(settings)
    client = _bigquery_client(project)
    with client:
        result = load_dataset(config, settings.data_dir, client, dataset)
    _log_stage("load", config, result, started)


def _bigquery_target(settings: Settings) -> tuple[str, str]:
    project, dataset = settings.gcp_project_id, settings.bq_dataset
    if not project or not dataset:
        missing = [
            name
            for name, value in [
                ("GCP_PROJECT_ID", project),
                ("BQ_DATASET", dataset),
            ]
            if not value
        ]
        raise _SettingsError(f"missing setting: {', '.join(missing)}")
    return project, dataset


def _bigquery_client(project: str) -> bigquery.Client:
    # Credentials come from the environment (ADC), never from settings.
    return bigquery.Client(project=project)


def _log_stage(
    stage: str, config: DatasetConfig, result: StageResult, started: float
) -> None:
    fields: dict[str, object] = {
        "stage": stage,
        "dataset": config.name,
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_rejected": result.rows_rejected,
        "duration_s": round(time.perf_counter() - started, 3),
    }
    if result.rejections is not None:
        fields["rejections"] = result.rejections
    log_event("stage_completed", **fields)


class _SettingsError(Exception):
    """A required environment setting is missing."""


@contextmanager
def _command(dataset_file: Path) -> Generator[None]:
    """Log as JSON and turn an expected failure into exit code 1.

    The failure is one error event; nothing else is printed.
    """
    configure_logging()
    try:
        yield
    except (
        DatasetConfigError,
        ExtractError,
        TransformError,
        LoadError,
        _SettingsError,
    ) as error:
        log_error(
            "run_failed",
            dataset_file=str(dataset_file),
            error=str(error),
        )
        raise typer.Exit(code=1) from error
