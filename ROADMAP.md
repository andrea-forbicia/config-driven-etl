# Roadmap

Plan of **config-driven-etl**, phase by phase. Released versions are listed
in `CHANGELOG.md`.

**Current phase: 13 — Empty sources and readable logs**

How to use this plan:

- Work top to bottom. Each phase has a goal, a list of checkboxes, and an
  exit criterion.
- One checkbox is one small pull request (or one manual step marked
  *manual*). The suggested branch type is in brackets.
- Tests are part of every checkbox, not a separate step.
- Claude ticks `[x]` once a box is complete and updates **Current phase**
  when an exit criterion is met.

## Phase 0 — Setup

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

## Phase 1 — Dataset config

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

## Phase 2 — Extract

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

## Phase 3 — Transform

Goal: pure functions turn raw strings into typed, clean rows and report what
they rejected.

- [x] Rename source columns to target names, drop unmapped columns, fail on a
      missing mapped column `[feat]`
- [x] Cast `string`, `int`, `float`, `bool` `[feat]`
- [x] Parse `date` and `datetime` with the configured format `[feat]`
- [x] Reject invalid `format` strings at config validation (unknown
      directives, incomplete time parts), so `validate` fails before any
      data is read `[feat]`
- [x] `nullable` rule `[feat]`
- [x] `min` / `max` range rule `[feat]`
- [x] `deduplicate_on` rule `[feat]`
- [x] Split valid and rejected rows, count rejections by reason `[feat]`
- [x] Single `transform(lf, config)` entry point composing the steps
      `[refactor]`

Exit: every type and rule is covered, including empty input; `transform.py`
does no I/O.

## Phase 4 — Parquet

Goal: a verified Parquet output per dataset.

- [x] Write to `data/processed/<name>/` (snappy), replacing the previous run
      `[feat]`
- [x] Re-read and verify schema and row count after writing `[feat]`

Exit: both fixture datasets produce Parquet whose schema matches the config.

## Phase 5 — Load

Goal: idempotent loads into the BigQuery sandbox.

- [x] `bq_schema.py`: column type to BigQuery type, `nullable` to mode
      `[feat]`
- [x] Create the table if missing, with partitioning and clustering from the
      config `[feat]`
- [x] Load Parquet with a load job, explicit schema, `WRITE_TRUNCATE` `[feat]`
- [x] Verify the loaded row count against the Parquet row count `[feat]`
- [x] `gcp` test: unique temporary table, deleted afterwards, skipped without
      credentials `[test]`

Exit: both real datasets load into the sandbox; a second run leaves the same
table state.

## Phase 6 — Product hardening

Goal: a CLI and container that behave like a product.

- [x] `extract`, `transform`, `load`, `run` CLI commands `[feat]`
- [x] Structured JSON logging, one line per stage with row counts and
      duration `[feat]`
- [x] Distinct exit codes per error category, documented in `--help` `[feat]`
- [x] System tests on both fixture datasets, BigQuery faked `[test]`
- [x] Coverage at or above 85% on `src/` `[test]`
- [x] Dockerfile: slim image, non-root user, dataset file as argument,
      credentials mounted at runtime, never baked in `[build]`

Exit: `docker run` executes `run` on a real dataset end to end.

## Phase 7 — CI/CD

Goal: every pull request is fully checked; releases are automatic.

- [x] Extend `ci.yml`: validate `datasets/`, system tests, coverage threshold,
      Docker build `[ci]`
- [x] GCP authentication for `gcp` tests on `main` only (Workload Identity
      Federation preferred) `[ci]`
- [x] `release.yml` on `v*` tags: CI, image build, GitHub Release from the
      changelog `[ci]`

Exit: a test tag produces a GitHub Release.

## Phase 8 — Release

Goal: `v1.0.0` published.

- [x] Final README: architecture diagram, "add a dataset" section, sample
      query, note on sandbox table expiry `[docs]`
- [x] `CHANGELOG.md` entry and version bump to `1.0.0` `[chore]`
- [x] Annotated tag `v1.0.0` on `main` *(manual)*

Exit: the GitHub Release for `v1.0.0` exists and its image builds.

## Phase 9 — String quality rules

Goal: the most common dirty values in open data are handled from the config
alone. Every rule is an optional per-column field, so each one is a MINOR
change.

- [x] `null_values` per column: sentinel strings (`"N/A"`, `"-"`, `"NULL"`)
      turned into null before the cast. Exact, case-sensitive match on the
      trimmed value. A sentinel in a column with `nullable: false` is
      rejected as `<name>: null` `[feat]`
- [x] `allowed_values` per column: a closed set of values; any other value
      is rejected as `<name>: not allowed`. Values are validated against the
      column type in the config `[feat]`
- [x] `pattern` per `string` column: a regex the whole value must match,
      otherwise rejected as `<name>: no pattern match`. Validated at config
      time with the same engine Polars uses `[feat]`
- [x] Use the new rules in the real datasets where they apply `[feat]`
- [x] `CHANGELOG.md` entry, version bump to `1.2.0`, annotated tag `v1.2.0`
      `[chore]`

Exit: every rule is covered by unit and system tests, documented in
`datasets/example.yaml`, and used by at least one real dataset; the GitHub
Release for `v1.2.0` exists.

## Phase 10 — Housekeeping

Goal: a tidier base before larger features: type checks in CI, test
expectations stated once, and a deliberate rule for values that break more
than one check.

- [x] Pyright in `ci.yml` on `src/` and `tests/` with the existing
      `[tool.pyright]` settings. Decide first between adding a pinned
      `pyright` to the `dev` extras (a new dependency) and a pinned GitHub
      Action `[ci]`
- [x] State each fixture's expected outcome (rows in, valid, rejected,
      rejections by reason) once, and read it in the extract, pipeline,
      CLI, load and system tests instead of repeating the numbers `[test]`
- [x] Decide whether a value outside `min` / `max` or `pattern` that is
      also outside `allowed_values` gets both reasons or only the more
      specific one; implement, test and document the choice in
      `datasets/example.yaml` `[fix]` (or `[docs]` if both reasons stay)
- [x] Fix the flaky `test_failed_write_keeps_the_previous_output`: once in
      CI a failed `write_parquet` left its `.part` file behind. Start from
      a test that reproduces it, then fix the cause `[fix]`
- [x] Release `v1.2.1` with the overlap rule and Parquet write fixes:
      version bump, `CHANGELOG.md` section, annotated tag `[chore]`

Exit: pyright runs on every pull request; adding a fixture row changes its
expected numbers in one place; the overlap rule is tested and documented;
a failed Parquet write never leaves a temporary file behind.

## Phase 11 — Rejected rows quarantine

Goal: every rejected row can be inspected after a run, not only counted.
Rejected rows are written next to the valid ones and, when the config asks
for it, loaded into their own BigQuery table. The new config field is
optional, so the phase is a MINOR change.

- [x] Keep each rejected row's raw source values: today a value that fails
      its cast is already null in `TransformResult.rejected`, so the bad
      value is lost. Decide first between carrying the raw strings through
      the transform and re-joining them by row number; the valid rows and
      the transform's purity must not change `[feat]`
- [x] Write the rejected rows to
      `data/processed/<name>/<name>.rejected.parquet` in the `transform`
      stage: `row_number` (record position in the source file),
      `reasons` (list of strings) and `raw`, a struct with every target
      column as `string`. Replaced on every run, written even when
      empty, verified like the valid output `[feat]`
- [x] Optional `target.rejected_table` in the config: a distinct
      identifier, never partitioned or clustered. Validated, documented in
      `datasets/example.yaml` `[feat]`
- [x] `load` stage: when `rejected_table` is set, load the rejected
      Parquet with `WRITE_TRUNCATE` and an explicit schema derived from
      the config (in `bq_schema.py`), and verify its row count. Decide
      first whether a failure of this second load fails the run `[feat]`
- [x] Log the rejected output path and table in the stage lines, extend
      the system tests (the `gcp` test already loads the rejected rows),
      and use `rejected_table` in at least one real dataset `[test]`
- [x] Release `v1.3.0`: version bump, `CHANGELOG.md` section, annotated
      tag `[chore]`

Exit: after `run`, each rejected row of both fixture datasets is in the
rejected Parquet with its raw values and all its reasons; with
`rejected_table` set, a second run leaves both tables in the same state;
the GitHub Release for `v1.3.0` exists.

## Phase 12 — Quality gates

Goal: a run decides whether its data is good enough to load, and leaves a
queryable record of what it did. Both features are optional config or
additive outputs, so the phase is a MINOR change.

- [x] Optional `rules.max_rejected_ratio` (0.0 to 1.0) in the config:
      validated, documented in `datasets/example.yaml` `[feat]`
- [x] Enforce the gate after the `transform` stage: above the ratio, the
      run fails with a new exit code before any load, so a broken source
      never replaces a good table. The rejected Parquet is still written,
      to explain the failure. Decide first how empty input counts (ratio
      undefined) and whether the gate also applies to the standalone
      `transform` command `[feat]`
- [x] Run summary: a `run_id`, the package version, start time, per-stage
      row counts and durations, outcome and exit code, collected by
      `pipeline.py` and logged as one final JSON line `[feat]`
- [x] Optional audit table, shared by every dataset through the
      `BQ_AUDIT_TABLE` setting (environment-level, like `BQ_DATASET`):
      append the run summary with a load job (`WRITE_APPEND`, explicit
      schema from `bq_schema.py`) for every pipeline command, also when it
      fails. A failed append turns a successful run into exit code 8; a
      failed run keeps its own code `[feat]`
- [x] System tests for a run blocked by the gate and for the audit row of
      a successful and a failed run; `gcp` test appends to a temporary
      audit table; use `max_rejected_ratio` in at least one real dataset
      `[test]`
- [x] Release `v1.4.0`: version bump, `CHANGELOG.md` section, annotated
      tag `[chore]`

Exit: a fixture above its `max_rejected_ratio` exits with the gate's code
and leaves the BigQuery tables untouched; every `run` with
`BQ_AUDIT_TABLE` set appends exactly one row, success or failure; the
GitHub Release for `v1.4.0` exists.

## Phase 13 — Empty sources and readable logs

Goal: a source that returns too few rows cannot empty a good table, and
a run can be followed comfortably in a terminal. Both are optional
config or settings, so the phase is a MINOR change.

- [x] Optional `rules.min_rows` (a positive integer) in the config:
      validated, documented in `datasets/example.yaml`. Decide first
      whether it counts the rows read or the valid rows `[feat]`
- [x] Enforce it in the `transform` stage next to `max_rejected_ratio`,
      with the same outcome: rejected rows written, valid Parquet removed,
      exit code 7, category `quality_gate`, BigQuery untouched. When both
      gates fail, the message gives both reasons `[feat]`
- [x] Use `min_rows` in the real datasets, from their measured row
      counts, with a system test for a header-only source `[test]`
- [x] `LOG_FORMAT` setting: `json` (default, unchanged) or `pretty`, one
      aligned text line per event, errors included. Opt-in only, never
      chosen from whether stderr is a terminal. Decide first the time
      zone of the pretty timestamps and which format is used when the
      settings themselves are invalid `[feat]`
- [x] Document `LOG_FORMAT` in `.env.example`, the README run section and
      the CLAUDE.md settings block; system test of a `pretty` run `[docs]`
- [ ] Release `v1.5.0`: version bump, `CHANGELOG.md` section, annotated
      tag `[chore]`

Exit: a header-only source exits with the gate's code and leaves the
BigQuery tables untouched; `LOG_FORMAT=pretty` logs one readable line per
event and `json` output is unchanged; the GitHub Release for `v1.5.0`
exists.

## Definition of done

A checkbox is done when the code is merged through a pull request, CI is green,
tests cover the change, and no secret or data file is in the diff. A *manual*
checkbox is done when its result has been verified by hand.
