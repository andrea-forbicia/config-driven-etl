# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Project packaging with `pyproject.toml`: metadata, dependencies, `dev`
  extras, ruff, pytest and pyright configuration, `gcp` test marker.
- `src/` and `tests/` layout with `__version__` and a
  `python -m config_driven_etl --help` entry point.
- CI workflow running ruff check, ruff format check and unit tests on pull
  requests and pushes to `main`.
- This changelog.
- Runtime `Settings` loaded from environment variables or a local `.env`
  file (`GCP_PROJECT_ID`, `BQ_DATASET`, `DATA_DIR`), and `.env.example`
  listing every key.

[Unreleased]: https://github.com/andrea-forbicia/config-driven-etl/commits/main
