"""Command-line interface."""

import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Annotated

import requests
import typer
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery

from config_driven_etl import __version__
from config_driven_etl.dataset_config import (
    DatasetConfig,
    DatasetConfigError,
    load_dataset_config,
)
from config_driven_etl.extract import ExtractError
from config_driven_etl.load import LoadError, rejected_parquet_path
from config_driven_etl.logging import (
    configure_logging,
    log_error,
    log_event,
)
from config_driven_etl.pipeline import (
    Command,
    QualityGateError,
    RunRecorder,
    RunSummary,
    Stage,
    StageResult,
    extract_dataset,
    load_dataset,
    transform_dataset,
)
from config_driven_etl.settings import Settings, SettingsError
from config_driven_etl.transform import TransformError


class ExitCode(IntEnum):
    """Process exit code for each error category."""

    OK = 0
    INTERNAL = 1
    USAGE = 2
    CONFIG = 3
    EXTRACT = 4
    TRANSFORM = 5
    LOAD = 6
    QUALITY_GATE = 7


_EXIT_CODE_HELP = {
    ExitCode.OK: "success",
    ExitCode.INTERNAL: "unexpected internal error",
    ExitCode.USAGE: "invalid command-line usage",
    ExitCode.CONFIG: "invalid dataset file or missing setting",
    ExitCode.EXTRACT: "source unreachable or raw CSV unreadable",
    ExitCode.TRANSFORM: "data does not match the dataset config",
    ExitCode.LOAD: "Parquet output or BigQuery load failed",
    ExitCode.QUALITY_GATE: "too many rejected rows, nothing loaded",
}

# Expected failures by category; anything else is a bug and exits with
# INTERNAL and a traceback.
_CATEGORIES: list[tuple[tuple[type[Exception], ...], ExitCode]] = [
    ((DatasetConfigError, SettingsError), ExitCode.CONFIG),
    ((ExtractError,), ExitCode.EXTRACT),
    ((TransformError,), ExitCode.TRANSFORM),
    ((LoadError, GoogleAuthError), ExitCode.LOAD),
    ((QualityGateError,), ExitCode.QUALITY_GATE),
]

app = typer.Typer(
    no_args_is_help=True,
    epilog="Exit codes: "
    + "; ".join(
        f"{code.value} {text}" for code, text in _EXIT_CODE_HELP.items()
    )
    + ".",
)

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

    Every file is checked; exits with code 3 if any is invalid.
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
        raise typer.Exit(code=ExitCode.CONFIG)


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
    with _command("extract", dataset) as job:
        _extract(job, Settings())


@app.command()
def transform(dataset: DatasetFile) -> None:
    """Clean the raw CSV into DATA_DIR/processed/<name>/<name>.parquet."""
    with _command("transform", dataset) as job:
        _transform(job, Settings())


@app.command()
def load(dataset: DatasetFile) -> None:
    """Load the processed Parquet into BigQuery, replacing the table data."""
    with _command("load", dataset) as job:
        _load(job, Settings())


@app.command()
def run(dataset: DatasetFile) -> None:
    """Extract, transform and load one dataset."""
    with _command("run", dataset) as job:
        settings = Settings()
        # Checked first so a missing setting does not waste a download.
        _bigquery_target(settings)
        _extract(job, settings)
        _transform(job, settings)
        _load(job, settings)


@dataclass(frozen=True)
class _Job:
    """The dataset of a command and the recorder of its stages."""

    config: DatasetConfig
    recorder: RunRecorder


def _extract(job: _Job, settings: Settings) -> None:
    job.recorder.start("extract")
    started = time.perf_counter()
    with requests.Session() as session:
        result = extract_dataset(job.config, settings.data_dir, session)
    _finish_stage(job, "extract", result, started)


def _transform(job: _Job, settings: Settings) -> None:
    job.recorder.start("transform")
    started = time.perf_counter()
    # Relative to DATA_DIR, which is a setting and stays out of the logs.
    rejected_file = rejected_parquet_path(Path(), job.config.name).as_posix()
    try:
        result = transform_dataset(job.config, settings.data_dir)
    except QualityGateError as error:
        # The counts by reason and the file say why the gate failed.
        _finish_stage(
            job,
            "transform",
            error.result,
            started,
            "stage_failed",
            rejected_file=rejected_file,
        )
        raise
    _finish_stage(
        job, "transform", result, started, rejected_file=rejected_file
    )


def _load(job: _Job, settings: Settings) -> None:
    job.recorder.start("load")
    started = time.perf_counter()
    project, dataset = _bigquery_target(settings)
    client = _bigquery_client(project)
    with client:
        result = load_dataset(job.config, settings.data_dir, client, dataset)
    extra: dict[str, object] = {}
    if result.rejected_rows_loaded is not None:
        # The table name only: the full id holds the project.
        extra = {
            "rejected_table": job.config.target.rejected_table,
            "rejected_rows_loaded": result.rejected_rows_loaded,
        }
    _finish_stage(job, "load", result, started, **extra)


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
        raise SettingsError(f"missing setting: {', '.join(missing)}")
    return project, dataset


def _bigquery_client(project: str) -> bigquery.Client:
    # Credentials come from the environment (ADC), never from settings.
    return bigquery.Client(project=project)


def _finish_stage(
    job: _Job,
    stage: Stage,
    result: StageResult,
    started: float,
    event: str = "stage_completed",
    /,
    **extra: object,
) -> None:
    """Log one stage line and record the stage in the run summary."""
    duration_s = round(time.perf_counter() - started, 3)
    job.recorder.record(
        stage, result, duration_s, failed=event == "stage_failed"
    )
    fields: dict[str, object] = {
        "run_id": job.recorder.run_id,
        "stage": stage,
        "dataset": job.config.name,
        **_counts(result),
        "duration_s": duration_s,
    }
    if result.rejections is not None:
        fields["rejections"] = result.rejections
    log_event(event, **fields, **extra)


def _counts(result: StageResult) -> dict[str, object]:
    return {
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_rejected": result.rows_rejected,
    }


@contextmanager
def _command(command: Command, dataset_file: Path) -> Generator[_Job]:
    """Load the dataset file, log as JSON, and set the exit code.

    An expected failure is one error event with its category. Every
    command that ends with an exit code, failed or not, ends with one
    run_finished event that summarizes it; an interrupt (Ctrl-C) does
    not. Nothing else is printed.
    """
    configure_logging()
    recorder = RunRecorder(command, __version__)
    expected = tuple(kind for kinds, _ in _CATEGORIES for kind in kinds)
    try:
        config = load_dataset_config(dataset_file)
        recorder.dataset = config.name
        yield _Job(config, recorder)
    except expected as error:
        code = _exit_code(error)
        log_error(
            "run_failed",
            run_id=recorder.run_id,
            dataset_file=str(dataset_file),
            category=code.name.lower(),
            exit_code=code.value,
            error=str(error),
        )
        _log_summary(recorder.finish(code, code.name.lower()))
        raise typer.Exit(code=code) from error
    except Exception:
        # A bug keeps its traceback, but the run is still summarized.
        code = ExitCode.INTERNAL
        _log_summary(recorder.finish(code, code.name.lower()))
        raise
    _log_summary(recorder.finish(ExitCode.OK, None))


def _log_summary(summary: RunSummary) -> None:
    log = log_event if summary.exit_code == ExitCode.OK else log_error
    log(
        "run_finished",
        run_id=summary.run_id,
        command=summary.command,
        version=summary.version,
        dataset=summary.dataset,
        started_at=summary.started_at.isoformat(timespec="milliseconds"),
        duration_s=summary.duration_s,
        outcome=summary.outcome,
        exit_code=summary.exit_code,
        category=summary.category,
        failed_stage=summary.failed_stage,
        stages=[
            {
                "stage": record.stage,
                **_counts(record.result),
                "duration_s": record.duration_s,
            }
            for record in summary.stages
        ],
    )


def _exit_code(error: Exception) -> ExitCode:
    for kinds, code in _CATEGORIES:
        if isinstance(error, kinds):
            return code
    return ExitCode.INTERNAL
