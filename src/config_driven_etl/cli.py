"""Command-line interface."""

from pathlib import Path
from typing import Annotated

import typer

from config_driven_etl import __version__
from config_driven_etl.dataset_config import (
    DatasetConfigError,
    load_dataset_config,
)

app = typer.Typer(no_args_is_help=True)


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
