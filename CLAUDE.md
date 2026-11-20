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
| Type check     | `pyright` (standard mode)                      |
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
│   ├── pipeline.py           # extract / transform / load stages
│   ├── errors.py             # error messages and categories
│   └── logging.py            # structured JSON logging
├── tests/
│   ├── unit/                 # no I/O, no network
│   ├── system/               # full pipeline on fixture datasets
│   ├── gcp/                  # real BigQuery sandbox, marker gcp
│   ├── fixtures/             # small CSV files + matching YAML configs
│   └── fixture_outcomes.py   # expected rows and rejections per fixture
├── data/                     # raw/ and processed/, git-ignored
├── .env.example
├── .gitignore
├── CHANGELOG.md
├── Dockerfile
├── LICENSE
├── pyproject.toml
├── README.md
└── ROADMAP.md                # phases, checkboxes, current phase
```

## Dataset configuration

The YAML file is the contract between a dataset and the pipeline. It is
validated by `dataset_config.py` before anything runs; an invalid file fails
with a clear error and touches no data. `datasets/example.yaml` documents
every option and a test keeps it complete, so it is the reference.

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
  The one exception is the audit table: it records events, not data state,
  so each run appends one row with `WRITE_APPEND`.
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
BQ_AUDIT_TABLE=                   # optional, one row per command
LOG_FORMAT=                       # optional, json (default) or pretty
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
pyright                                                # type check
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
  check, pyright, validate every file in `datasets/`, unit tests, system
  tests, coverage threshold, Docker build.
- `release.yml` on `v*` tags: run CI, build the image, create a GitHub Release
  from the changelog entry.
- GCP tests run only on `main` and only when secrets are available.

## Roadmap

The plan lives in `ROADMAP.md`: phases, checkboxes, the current phase and
the definition of done. Read it before planning or implementing a checkbox.
Tick `[x]` as soon as a checkbox's goal is complete (merged, or verified
for a manual step), in the pull request that completes it, and update
**Current phase** there when an exit criterion is met.
