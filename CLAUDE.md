# CLAUDE.md

Guidance for Claude Code in this repository.

## Project

**config-driven-etl**: a portfolio data engineering project. A generic batch
ETL pipeline that extracts any public open-data CSV, cleans and transforms it
with **Polars**, writes **Parquet**, and loads it into **Google BigQuery**.

The pipeline knows nothing about any specific dataset. Each dataset is
described by a YAML file (source, schema, cleaning rules, BigQuery target);
adding a new dataset means adding a YAML file, not writing code.

It is built as a small SaaS-style product, not a script: stateless, configured
through environment variables and dataset files, containerized, idempotent,
observable, and versioned.

## Working mode

The author is building this to learn. Unless explicitly asked to implement:

- Suggest, explain, and review; do not write whole modules unprompted.
- When asked to implement, do one checkbox at a time and stop for review.
- Point out design trade-offs instead of silently choosing one.
- Tick a checkbox as soon as its goal is complete (merged, or verified for a
  manual step), in the pull request that completes it.

## Hard constraints

- **Free only**: BigQuery sandbox / free tier, no billing-enabled services.
  Sandbox means load jobs only (no DML, no streaming) and tables and
  partitions that expire after 60 days.
- **Dataset-agnostic**: no dataset name, column name, or dataset-specific rule
  anywhere in `src/`. If a dataset needs new behavior, add a generic rule type
  to the config model.
- **English** for code, identifiers, comments, docstrings, commits, and docs.
- **PEP 8**, enforced by ruff (line length 79). Type hints on public functions.
- **Minimal comments**: explain why, never what. Short docstrings on public API.
- **Minimal README**: purpose, architecture sketch, setup, run, adding a
  dataset, test. Nothing else.
- **No secrets in git**: no keys, project IDs, or `.env` files committed.

## Stack

| Concern        | Choice                                         |
|----------------|------------------------------------------------|
| Language       | Python 3.12                                    |
| Transform      | Polars (lazy API where possible)               |
| Storage format | Parquet (pyarrow, snappy)                      |
| Warehouse      | BigQuery (`google-cloud-bigquery`)             |
| Runtime config | `pydantic-settings`, env vars only             |
| Dataset config | YAML (`pyyaml`) validated by `pydantic` models |
| CLI            | `typer`                                        |
| Tests          | `pytest` runner, `unittest` (TestCase, mock)   |
| Lint / format  | `ruff`                                         |
| Packaging      | `pyproject.toml`, `src/` layout                |
| Container      | Docker                                         |
| CI/CD          | GitHub Actions                                 |

Do not add a dependency without asking.

## Structure

```
config-driven-etl/
├── .github/workflows/        # ci.yml, release.yml
├── datasets/                 # one YAML file per dataset
│   └── example.yaml
├── src/config_driven_etl/
│   ├── __init__.py           # __version__
│   ├── __main__.py           # python -m config_driven_etl
│   ├── cli.py                # validate | extract | transform | load | run
│   ├── settings.py           # runtime Settings (env vars)
│   ├── dataset_config.py     # pydantic models + YAML loader
│   ├── extract.py            # download / read raw CSV
│   ├── transform.py          # generic rules applied from the config
│   ├── bq_schema.py          # config columns -> BigQuery SchemaField
│   ├── load.py               # Parquet write, BigQuery load job
│   └── logging.py            # structured JSON logging
├── tests/
│   ├── unit/                 # no I/O, no network
│   ├── system/               # full pipeline on fixture datasets
│   └── fixtures/             # small CSV files + matching YAML configs
├── data/                     # raw/ and processed/, git-ignored
├── .env.example
├── .gitignore
├── CHANGELOG.md
├── Dockerfile
├── LICENSE
├── pyproject.toml
└── README.md
```

## Dataset configuration

The YAML file is the contract between a dataset and the pipeline. It is
validated by `dataset_config.py` before anything runs; an invalid file fails
with a clear error and touches no data. Indicative shape:

```yaml
name: city_weather
source:
  url: https://example.org/weather.csv   # or path: data/raw/weather.csv
  delimiter: ","
  encoding: utf-8
columns:
  - source: "Date"
    name: observation_date
    type: date
    format: "%Y-%m-%d"
    nullable: false
  - source: "Temp (C)"
    name: temperature_c
    type: float
    min: -90
    max: 60
rules:
  deduplicate_on: [observation_date, station_id]
  drop_unmapped_columns: true
target:
  table: city_weather
  partition_by: observation_date
  cluster_by: [station_id]
  write_mode: truncate
```

- Supported types are a closed set (`string`, `int`, `float`, `bool`, `date`,
  `datetime`), each mapped once to a Polars dtype and a BigQuery type.
- New rule types are added to the pydantic model first, then implemented
  generically in `transform.py`, then documented in `datasets/example.yaml`.
- Dataset files are committed to the repository, so they never contain
  secrets.

## Architecture rules

- `transform.py` holds pure functions: `(LazyFrame, DatasetConfig)` in,
  `LazyFrame` out. No I/O.
- All I/O lives in `extract.py` and `load.py`, behind small functions that take
  their client or path as an argument (so tests can inject fakes).
- The BigQuery schema is always derived from the dataset config. Never rely on
  schema autodetection.
- Runs are idempotent: re-running the same input produces the same table state
  (`WRITE_TRUNCATE` per partition, or a full truncate for small datasets).
- Bad rows are counted and logged, not silently dropped.
- Log one JSON line per stage with dataset name, row counts in, out, rejected,
  and duration.
- Fail fast with a non-zero exit code; no bare `except`.

## Runtime settings and secrets

Environment-level settings come from env vars, loaded in `settings.py`.
Dataset-level settings come from the YAML file.

```
GCP_PROJECT_ID=
BQ_DATASET=
DATA_DIR=./data
GOOGLE_APPLICATION_CREDENTIALS=   # local only, path outside the repo
```

- `.env` is git-ignored; `.env.example` lists keys with empty values.
- Local auth: `gcloud auth application-default login` or a key file kept
  outside the repository.
- CI auth: GitHub Actions secrets (prefer Workload Identity Federation over a
  JSON key).
- Never print settings or credentials in logs. Never hardcode a project ID.

## Commands

```
pip install -e ".[dev]"                                # setup
ruff check . && ruff format --check .
pytest tests/unit
pytest tests/system
pytest -m gcp                                          # real BigQuery
pytest --cov=config_driven_etl
python -m config_driven_etl validate datasets/example.yaml
python -m config_driven_etl run datasets/example.yaml
docker build -t config-driven-etl .
```

## Testing

- **Unit** (`tests/unit`): `unittest.TestCase` classes run by pytest. One test
  module per source module. BigQuery and HTTP are replaced with
  `unittest.mock`. Cover each rule type and each column type, including nulls,
  duplicates, malformed values, and empty input.
- **Config validation**: valid and invalid YAML files (unknown type, missing
  column, duplicate target name, bad partition column).
- **System** (`tests/system`): run the CLI end to end on **at least two fixture
  datasets with different shapes**, to prove the pipeline is generic. Assert
  on the Parquet output (schema, row count, values) with the BigQuery client
  faked.
- **GCP** (marker `gcp`): load into a real sandbox dataset with a unique
  temporary table, then delete it. Skipped automatically without credentials.
- Coverage target: 85% on `src/`. A bug fix starts with a failing test.

## Git

- Trunk-based: `main` is always green and protected; work happens on
  short-lived branches named `<type>/<short-description>`.
- [Conventional Commits](https://www.conventionalcommits.org):
  `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `ci`, `build`.
  Example: `feat(transform): add min/max range rule for numeric columns`.
- Small, single-purpose commits. Merge through pull requests, squash merge.
- Never commit to `main` directly, never force-push `main`, never commit data.
  The single exception is the bootstrap commit in Phase 0, made before
  branch protection can exist.

## Versioning

- [SemVer](https://semver.org). Single source of truth: `version` in
  `pyproject.toml`.
- A breaking change to the dataset config format is a MAJOR bump.
- `CHANGELOG.md` in [Keep a Changelog](https://keepachangelog.com) format,
  updated in the same pull request as the change.
- Releases are annotated tags `vMAJOR.MINOR.PATCH` on `main`.

## CI/CD

- `ci.yml` on every pull request and push to `main`: ruff check, ruff format
  check, validate every file in `datasets/`, unit tests, system tests,
  coverage threshold, Docker build.
- `release.yml` on `v*` tags: run CI, build the image, create a GitHub Release
  from the changelog entry.
- GCP tests run only on `main` and only when secrets are available.

## Phases

**Current phase: 3 — Transform**

How to use this plan:

- Work top to bottom. Each phase has a goal, a list of checkboxes, and an
  exit criterion.
- One checkbox is one small pull request (or one manual step marked
  *manual*). The suggested branch type is in brackets.
- Tests are part of every checkbox, not a separate step.
- Claude ticks `[x]` once a box is complete and updates **Current phase**
  when an exit criterion is met.

### Phase 0 — Setup

Goal: an empty but installable, linted, and tested project on a protected
`main`, with CI from the first pull request.

- [x] Pick two public datasets with different shapes: direct CSV URL with no
      login (data.gov or Socrata portals; Kaggle requires authentication),
      real dirty values, a `date`/`datetime` column in at least one of them,
      under ~100 MB *(manual)*
- [x] Bootstrap commit on `main`: `.gitignore`, `LICENSE`, `CLAUDE.md`,
      placeholder `README.md` with source and license of both datasets
      *(the only direct commit to `main`)*
- [x] Create the GitHub repository, push, enable branch protection (pull
      request and status checks required), add topics *(manual)*
- [x] `pyproject.toml`: metadata, version `0.1.0`, dependencies, `dev` extras,
      ruff and pytest config, `gcp` marker `[build]`
- [x] Scaffold `src/` and `tests/` with empty modules, `__version__`, and a
      working `python -m config_driven_etl --help` `[chore]`
- [x] Minimal `ci.yml`: ruff check, ruff format check, unit tests `[ci]`
- [x] `CHANGELOG.md` with an `Unreleased` section `[docs]`
- [x] `.env.example` and `settings.py` `[feat]`
- [x] GCP project and BigQuery sandbox dataset; local auth verified with
      application default credentials *(manual)*

Exit: `pip install -e ".[dev]"`, ruff, and pytest pass locally and in CI on a
pull request.

### Phase 1 — Dataset config

Goal: any YAML file can be validated without touching data.

- [x] Pydantic models (`Source`, `Column`, `Rules`, `Target`,
      `DatasetConfig`) with a closed column type enum `[feat]`
- [x] Cross-field validation: exactly one of `url` / `path`; unique target
      names; `partition_by`, `cluster_by`, `deduplicate_on` reference existing
      columns; partition column is `date` / `datetime`; `min` / `max` only on
      numeric types; `format` only on `date` / `datetime` `[feat]`
- [x] YAML loader with clear errors (file, field path, reason) `[feat]`
- [x] `datasets/example.yaml` documenting every option `[docs]`
- [x] YAML files for the two real datasets `[feat]`
- [x] `validate` CLI command, non-zero exit on an invalid file `[feat]`

Exit: `validate` passes on every file in `datasets/` and fails with a clear
message on each invalid fixture.

### Phase 2 — Extract

Goal: the raw CSV of any configured dataset lands in `data/raw/<name>/`.

- [x] Download from `source.url` (streamed, with timeout, HTTP errors fail
      fast) `[feat]`
- [x] Read from `source.path` `[feat]`
- [x] Skip the download when the file is present and unchanged (decide
      between ETag / Last-Modified and content hash) `[feat]`
- [x] Read the raw CSV into a `LazyFrame` with delimiter and encoding from the
      config, every column as string `[feat]`
- [x] Fixture CSVs for two different shapes with dirty rows (nulls, malformed
      dates, out-of-range values, duplicates, extra columns) and matching YAML
      `[test]`

Exit: both real datasets extract locally; unit tests use no network.

### Phase 3 — Transform

Goal: pure functions turn raw strings into typed, clean rows and report what
they rejected.

- [x] Rename source columns to target names, drop unmapped columns, fail on a
      missing mapped column `[feat]`
- [x] Cast `string`, `int`, `float`, `bool` `[feat]`
- [x] Parse `date` and `datetime` with the configured format `[feat]`
- [x] Reject invalid `format` strings at config validation (unknown
      directives, incomplete time parts), so `validate` fails before any
      data is read `[feat]`
- [ ] `nullable` rule `[feat]`
- [ ] `min` / `max` range rule `[feat]`
- [ ] `deduplicate_on` rule `[feat]`
- [ ] Split valid and rejected rows, count rejections by reason `[feat]`
- [ ] Single `transform(lf, config)` entry point composing the steps
      `[refactor]`

Exit: every type and rule is covered, including empty input; `transform.py`
does no I/O.

### Phase 4 — Parquet

Goal: a verified Parquet output per dataset.

- [ ] Write to `data/processed/<name>/` (snappy), replacing the previous run
      `[feat]`
- [ ] Re-read and verify schema and row count after writing `[feat]`

Exit: both fixture datasets produce Parquet whose schema matches the config.

### Phase 5 — Load

Goal: idempotent loads into the BigQuery sandbox.

- [ ] `bq_schema.py`: column type to BigQuery type, `nullable` to mode
      `[feat]`
- [ ] Create the table if missing, with partitioning and clustering from the
      config `[feat]`
- [ ] Load Parquet with a load job, explicit schema, `WRITE_TRUNCATE` `[feat]`
- [ ] Verify the loaded row count against the Parquet row count `[feat]`
- [ ] `gcp` test: unique temporary table, deleted afterwards, skipped without
      credentials `[test]`

Exit: both real datasets load into the sandbox; a second run leaves the same
table state.

### Phase 6 — Product hardening

Goal: a CLI and container that behave like a product.

- [ ] `extract`, `transform`, `load`, `run` CLI commands `[feat]`
- [ ] Structured JSON logging, one line per stage with row counts and
      duration `[feat]`
- [ ] Distinct exit codes per error category, documented in `--help` `[feat]`
- [ ] System tests on both fixture datasets, BigQuery faked `[test]`
- [ ] Coverage at or above 85% on `src/` `[test]`
- [ ] Dockerfile: slim image, non-root user, dataset file as argument,
      credentials mounted at runtime, never baked in `[build]`

Exit: `docker run` executes `run` on a real dataset end to end.

### Phase 7 — CI/CD

Goal: every pull request is fully checked; releases are automatic.

- [ ] Extend `ci.yml`: validate `datasets/`, system tests, coverage threshold,
      Docker build `[ci]`
- [ ] GCP authentication for `gcp` tests on `main` only (Workload Identity
      Federation preferred) `[ci]`
- [ ] `release.yml` on `v*` tags: CI, image build, GitHub Release from the
      changelog `[ci]`

Exit: a test tag produces a GitHub Release.

### Phase 8 — Release

Goal: `v1.0.0` published.

- [ ] Final README: architecture diagram, "add a dataset" section, sample
      query, note on sandbox table expiry `[docs]`
- [ ] `CHANGELOG.md` entry and version bump to `1.0.0` `[chore]`
- [ ] Annotated tag `v1.0.0` on `main` *(manual)*

Exit: the GitHub Release for `v1.0.0` exists and its image builds.

## Definition of done

A checkbox is done when the code is merged through a pull request, CI is green,
tests cover the change, and no secret or data file is in the diff. A *manual*
checkbox is done when its result has been verified by hand.
