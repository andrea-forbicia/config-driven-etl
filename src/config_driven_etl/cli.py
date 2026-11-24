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
from config_driven_etl.bq_schema import AUDIT_TABLE_SPEC
from config_driven_etl.dataset_config import (
    DatasetConfig,
    DatasetConfigError,
    load_dataset_config,
)
from config_driven_etl.extract import ExtractError
from config_driven_etl.load import (
    LoadError,
    append_rows,
    bigquery_table_id,
    ensure_table,
    rejected_parquet_path,
)
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
    SchemaDriftError,
    Stage,
    StageResult,
    audit_row,
    extract_dataset,
    load_dataset,
    transform_dataset,
)
from config_driven_etl.settings import (
    Settings,
    SettingsError,
    load_log_format,
    load_settings,
)
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
    AUDIT = 8
    SCHEMA_DRIFT = 9


_EXIT_CODE_HELP = {
    ExitCode.OK: "success",
    ExitCode.INTERNAL: "unexpected internal error",
    ExitCode.USAGE: "invalid command-line usage",
    ExitCode.CONFIG: "invalid dataset file or missing setting",
    ExitCode.EXTRACT: "source unreachable or raw CSV unreadable",
    ExitCode.TRANSFORM: "data does not match the dataset config",
    ExitCode.LOAD: "Parquet output or BigQuery load failed",
    ExitCode.QUALITY_GATE: "data failed a quality gate, nothing loaded",
    ExitCode.AUDIT: "run succeeded, audit row not appended",
    ExitCode.SCHEMA_DRIFT: "table schema change not allowed, nothing loaded",
}

# Expected failures by category; anything else is a bug and exits with
# INTERNAL and a traceback.
_CATEGORIES: list[tuple[tuple[type[Exception], ...], ExitCode]] = [
    ((DatasetConfigError, SettingsError), ExitCode.CONFIG),
    ((ExtractError,), ExitCode.EXTRACT),
    ((TransformError,), ExitCode.TRANSFORM),
    ((LoadError, GoogleAuthError), ExitCode.LOAD),
    ((QualityGateError,), ExitCode.QUALITY_GATE),
    ((SchemaDriftError,), ExitCode.SCHEMA_DRIFT),
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
AllowSchemaChange = Annotated[
    bool,
    typer.Option(
        "--allow-schema-change",
        help="Replace the target and rejected table schemas whatever the "
        "change, for this run only. With write_mode partition, a breaking "
        "change to the target table is still refused.",
    ),
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
        _extract(job)


@app.command()
def transform(dataset: DatasetFile) -> None:
    """Clean the raw CSV into DATA_DIR/processed/<name>/<name>.parquet."""
    with _command("transform", dataset) as job:
        _transform(job)


@app.command()
def load(
    dataset: DatasetFile, allow_schema_change: AllowSchemaChange = False
) -> None:
    """Load the processed Parquet into BigQuery, replacing the table data.

    With write_mode partition, only the days in the file are replaced.
    """
    with _command("load", dataset) as job:
        _load(job, allow_schema_change)


@app.command()
def run(
    dataset: DatasetFile, allow_schema_change: AllowSchemaChange = False
) -> None:
    """Extract, transform and load one dataset."""
    with _command("run", dataset) as job:
        # Checked first so a missing setting does not waste a download.
        _bigquery_target(job.settings)
        _extract(job)
        _transform(job)
        _load(job, allow_schema_change)


@dataclass(frozen=True)
class _Job:
    """What a command runs on, and the recorder of its stages."""

    config: DatasetConfig
    settings: Settings
    recorder: RunRecorder


def _extract(job: _Job) -> None:
    job.recorder.start("extract")
    started = time.perf_counter()
    with requests.Session() as session:
        result = extract_dataset(job.config, job.settings.data_dir, session)
    _finish_stage(job, "extract", result, started)


def _transform(job: _Job) -> None:
    job.recorder.start("transform")
    started = time.perf_counter()
    # Relative to DATA_DIR, which is a setting and stays out of the logs.
    rejected_file = rejected_parquet_path(Path(), job.config.name).as_posix()
    try:
        result = transform_dataset(job.config, job.settings.data_dir)
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


def _load(job: _Job, allow_schema_change: bool) -> None:
    job.recorder.start("load")
    started = time.perf_counter()
    project, dataset = _bigquery_target(job.settings)
    client = _bigquery_client(project)
    with client:
        result = load_dataset(
            job.config,
            job.settings.data_dir,
            client,
            dataset,
            allow_schema_change=allow_schema_change,
        )
    extra: dict[str, object] = {}
    if result.rejected_rows_loaded is not None:
        # The table name only: the full id holds the project.
        extra = {
            "rejected_table": job.config.target.rejected_table,
            "rejected_rows_loaded": result.rejected_rows_loaded,
        }
        if result.rejected_schema_changes is not None:
            extra["rejected_schema_changes"] = list(
                result.rejected_schema_changes
            )
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
    if result.dropped_columns is not None:
        fields["dropped_columns"] = list(result.dropped_columns)
    if result.schema_changes is not None:
        fields["schema_changes"] = list(result.schema_changes)
    log_event(event, **fields, **extra)


def _counts(result: StageResult) -> dict[str, object]:
    return {
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_rejected": result.rows_rejected,
    }


@contextmanager
def _command(command: Command, dataset_file: Path) -> Generator[_Job]:
    """Read settings and the dataset file, log events, set the exit code.

    An expected failure is one error event with its category. Every
    command that ends with an exit code, failed or not, is appended to
    BQ_AUDIT_TABLE when it is set, then ends with one run_finished event
    that summarizes it; an interrupt (Ctrl-C) does neither. Nothing else
    is printed.
    """
    configure_logging(load_log_format())
    recorder = RunRecorder(command, __version__)
    settings: Settings | None = None
    expected = tuple(kind for kinds, _ in _CATEGORIES for kind in kinds)
    try:
        settings = load_settings()
        config = load_dataset_config(dataset_file)
        recorder.dataset = config.name
        yield _Job(config, settings, recorder)
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
        _finish_run(recorder, settings, code)
        raise typer.Exit(code=code) from error
    except Exception:
        # A bug keeps its traceback, but the run is still summarized.
        _finish_run(recorder, settings, ExitCode.INTERNAL)
        raise
    code = _finish_run(recorder, settings, ExitCode.OK)
    if code != ExitCode.OK:
        raise typer.Exit(code=code)


def _finish_run(
    recorder: RunRecorder, settings: Settings | None, code: ExitCode
) -> ExitCode:
    """Append the run to the audit table, log it, return the exit code.

    A failed append turns a successful run into AUDIT; a failed run
    keeps its own code, which matters more.
    """
    summary = recorder.finish(code, _category(code))
    table = settings.bq_audit_table if settings is not None else None
    try:
        if settings is not None and table is not None:
            _append_audit_row(settings, table, summary)
    except (LoadError, GoogleAuthError) as error:
        log_error("audit_failed", run_id=recorder.run_id, error=str(error))
        if code == ExitCode.OK:
            code = ExitCode.AUDIT
            summary = recorder.finish(code, _category(code))
    except Exception:
        # A bug in the audit exits 1 with its traceback; say so.
        code = ExitCode.INTERNAL
        summary = recorder.finish(code, _category(code))
        raise
    finally:
        log = log_event if code == ExitCode.OK else log_error
        log("run_finished", **audit_row(summary))
    return code


def _category(code: ExitCode) -> str | None:
    return None if code == ExitCode.OK else code.name.lower()


def _append_audit_row(
    settings: Settings, table: str, summary: RunSummary
) -> None:
    project, dataset = _bigquery_target(settings)
    table_id = bigquery_table_id(project, dataset, table)
    client = _bigquery_client(project)
    with client:
        ensure_table(client, table_id, AUDIT_TABLE_SPEC)
        append_rows(client, table_id, AUDIT_TABLE_SPEC, [audit_row(summary)])


def _exit_code(error: Exception) -> ExitCode:
    for kinds, code in _CATEGORIES:
        if isinstance(error, kinds):
            return code
    return ExitCode.INTERNAL
