# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] - 2026-11-24

### Added

- `target.write_mode: partition`: a load replaces only the days present
  in the file, with one `WRITE_TRUNCATE` load job per day
  (`table$YYYYMMDD`), and keeps every other day, so a source that
  publishes only recent data no longer erases the history. It requires
  `target.partition_by` on a column with `nullable: false`. Each job
  checks its row count; if one fails, the run exits with code 6 and lists
  the days already replaced and those not, and rerunning the same file
  replaces them all. After the jobs, the row count of each replaced day
  is read back from the table, so a day the sandbox drops as expired
  fails the run instead of disappearing. The replaced days are listed as
  `partitions` in the load stage line (`N partitions first..last` in the
  pretty format) and in each stage of the run summary and audit row.
  Additive schema changes are applied; a breaking one exits with code 9
  even with `--allow-schema-change`. The `target.rejected_table` is still
  replaced whole, so it holds only the last run's rejections. Known
  limit: a row whose date changes between two runs stays in its old day
  too, since that day is kept.

### Changed

- `datasets/usgs_earthquakes.yaml` uses `write_mode: partition`, so
  each run replaces the 30 days of the feed and keeps the older days
  loaded by earlier runs. An existing `usgs_earthquakes` table keeps
  its rows until the next run replaces the days in the feed.

## [1.6.0] - 2026-11-23

### Added

- The unmapped source columns that `drop_unmapped_columns: true` drops
  are no longer silent: the `transform` stage line lists them as
  `dropped_columns` (`dropped '<name>', ...` in the pretty format), and
  each stage of the run summary and of the audit row carries them, so a
  new or renamed source header shows even when the run succeeds.
- Schema drift check before every load: the schema of an existing
  target table is compared with the one derived from the config. With
  the new optional `target.schema_change` (`additive`, the default, or
  `none`), a change beyond it fails the run with the new exit code 9,
  category `schema_drift`, before any load, and the message lists every
  column that differs. Additive changes are those BigQuery accepts on
  append: a nullable column added after the existing ones, or a
  non-nullable column made nullable. Anything else is breaking and is
  only applied with `--allow-schema-change` on `load` or `run`, for that
  run. Accepted changes are listed as `schema_changes` in the load
  stage line. The same check, setting and flag apply to
  `target.rejected_table` (its accepted changes are listed as
  `rejected_schema_changes`); both tables are checked before any load,
  and one message lists every table whose change is not allowed.

### Changed

- The audit append adds the fields that a table created by an earlier
  version lacks (`ALLOW_FIELD_ADDITION`), so upgrading keeps the same
  `BQ_AUDIT_TABLE`; any other schema difference still fails the append
  with exit code 8.

## [1.5.0] - 2026-11-20

### Added

- Optional `rules.min_rows` in the dataset config: the fewest valid rows
  (the rows that would be loaded) a run accepts. Below it, the
  `transform` stage behaves as for `max_rejected_ratio`: rejected rows
  written, no valid Parquet, exit code 7, BigQuery tables untouched. It
  stops a source that returns only its header, which
  `max_rejected_ratio` lets pass, and a source whose rows are all
  rejected. When both gates fail, the message gives both reasons.
- Optional `LOG_FORMAT` setting: `json` (default, unchanged) or
  `pretty`, one aligned text line per event for a terminal, with local
  `HH:MM:SS` times, the counts and rejections of each stage, errors on
  one line, and the `run_id` on the final line. It is read before the
  other settings, so their errors use the asked format; an invalid
  `LOG_FORMAT` exits with code 3 and is reported in JSON. Listed in
  `.env.example`, with a sample in the README.

### Changed

- The `--help` text of exit code 7 now reads "data failed a quality
  gate, nothing loaded", as it covers both gates.
- The real datasets set `min_rows` from their measured valid rows:
  `usgs_earthquakes` 1000 (a varying 30-day feed), `wa_ev_population`
  100000 (a growing registry), `nyc_squirrel_census` 3000 (a static
  census with 3,018 valid rows). Besides an empty source, they stop a truncated
  download.

## [1.4.0] - 2026-11-20

### Added

- Optional `rules.max_rejected_ratio` in the dataset config, from 0.0 to
  1.0: the highest share of rejected rows over the rows read. Above it,
  the `transform` stage (in both `transform` and `run`) writes the
  rejected rows, writes no valid Parquet and removes the previous one,
  and exits with the new code 7, so nothing is loaded and the BigQuery
  tables keep their data. A source with no rows always passes. The
  transform stage then logs a `stage_failed` line with the usual counts,
  the rejections by reason and `rejected_file`.
- Every `extract`, `transform`, `load` and `run` command ends with a
  `run_finished` log line, failed or not: `run_id`, `command`, `version`,
  `dataset`, `started_at` (UTC), `duration_s`, `outcome`, `exit_code`,
  `category` and `failed_stage` (both null on success), and the row
  counts and duration of each stage that produced them. Stage lines and
  `run_failed` carry the same `run_id`.
- Optional `BQ_AUDIT_TABLE` setting: a table in `BQ_DATASET`, shared by
  every dataset, that gets the `run_finished` summary of every pipeline
  command as one row, failed or not, with a `WRITE_APPEND` load job and
  an explicit schema (`stages` and their `rejections` as repeated
  records). If the append fails, a successful command exits with the new
  code 8 and a failed one keeps its code; both log `audit_failed`. The
  setting needs `GCP_PROJECT_ID` and `BQ_DATASET`.
- The `run_finished` summary lists each stage's rejections by reason.
- An invalid setting now fails with exit code 3 and a message that names
  the setting, never its value.
- `load` without the valid Parquet fails with "not found; run transform
  first" instead of a Parquet read error.

### Changed

- `usgs_earthquakes`, `wa_ev_population` and `nyc_squirrel_census` set
  `max_rejected_ratio: 0.01`; they reject at most 0.17% of their rows
  today.

### Fixed

- BigQuery error messages, logged in `run_failed` and `audit_failed`,
  no longer contain the project id: tables are named `dataset.table`,
  and the project id quoted in Google's own messages (for example in
  request URLs) is replaced with `<project>`.

## [1.3.0] - 2026-11-19

### Added

- The `transform` stage writes the rejected rows to
  `data/processed/<name>/<name>.rejected.parquet`, next to the valid
  ones: `row_number` (the record's position in the source file, the
  first record after the header being 1; a quoted value spanning lines
  counts once), `reasons` (every reason the row was rejected) and `raw`
  (a struct with every mapped column as read, before any rule changed
  it). The file is written on every run, empty when no row is rejected.
- Optional `target.rejected_table` in the dataset config. When set, the
  `load` stage loads the rejected rows into that table after the valid
  ones, with `WRITE_TRUNCATE`, a schema derived from the config
  (`reasons` as `ARRAY<STRING>`, `raw` as a `STRUCT`) and a row count
  check. A failure of this second load fails the run.
- Stage log lines say where the rejected rows are: `transform` logs
  `rejected_file`, relative to `DATA_DIR`; with `rejected_table`, `load`
  logs `rejected_table` and `rejected_rows_loaded`.
- `wa_ev_population` loads its rejected rows into
  `wa_ev_population_rejected`.

### Fixed

- The processed Parquet file is now verified before it replaces the
  previous output, so a file with the wrong schema or row count fails
  the run and leaves the previous output in place.

## [1.2.1] - 2026-11-18

### Changed

- CI type-checks `src/` and `tests/` with pyright, pinned in the `dev`
  extras so local and CI runs use the same version.

### Fixed

- A value that fails `min` / `max` or `pattern` is no longer also
  rejected as `<name>: not allowed` when the column has
  `allowed_values`. Every allowed value passes those checks, so the
  second reason was always redundant; rejection counts now give each
  value one reason per column.
- A failed Parquet write could leave its temporary `.part` file in the
  processed directory: Polars' streaming writer could still create it
  after raising. The already collected rows are now written
  synchronously.

## [1.2.0] - 2026-11-18

### Added

- `null_values` column option: sentinel strings (for example `"N/A"` or
  `"-"`) read as null before the type is checked. Matching is exact and
  case-sensitive on the trimmed value; other values are left as written.
  A sentinel in a column with `nullable: false` is rejected as
  `<name>: null`.
- `allowed_values` column option on string, int and date columns: a
  closed set of accepted values; rows with any other value are rejected
  as `<name>: not allowed`. Values are compared after the cast, so `"01"`
  matches `"1"` on an int column; strings are compared as written.
  `validate` rejects entries that are not valid for the column type,
  repeat another entry after the cast, fall outside `min` / `max`, or
  are also in `null_values`.
- `pattern` column option on string columns: a regex the whole value
  must match, otherwise the row is rejected as
  `<name>: no pattern match`. Values are matched as written. `validate`
  compiles the pattern with the Polars (Rust) regex engine, so
  look-around and backreferences are rejected before any data is read,
  and rejects `allowed_values` entries that do not match it.

### Changed

- The real dataset configs use the new rules:
  - `nyc_squirrel_census`: `above_ground_measurement` is now an `int`
    (`"FALSE"`, meaning on the ground, is read as null), `age` reads `"?"`
    as null, and IDs, hectares, shift, age, fur color and location are
    checked with `pattern` or `allowed_values`.
  - `wa_ev_population`: `electric_range` reads `0` ("range not
    researched") as null; VIN, state, postal code and census tract are
    checked with `pattern`, and vehicle type and CAFV eligibility with
    `allowed_values`. Postal codes that lost their leading zero upstream
    are now rejected.
  - `usgs_earthquakes`: network and event IDs are checked with `pattern`;
    no closed value sets, since the feed can add new values at any time.

## [1.1.0] - 2026-11-18

### Added

- Dataset config for the 2018 Central Park Squirrel Census
  (`nyc_squirrel_census`, NYC Open Data, not partitioned), added without
  code changes.

## [1.0.0] - 2026-11-12

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
- Dataset config models (`DatasetConfig`, `Source`, `Column`, `Rules`,
  `Target`) with a closed `ColumnType` enum, lowercase snake_case names,
  and rejection of unknown keys.
- Cross-field validation of dataset configs: exactly one of `source.url` /
  `source.path`; `min` / `max` only on numeric columns and `min` not above
  `max`; `format` required on `date` / `datetime` columns and rejected on
  others; unique column names and source headers; `partition_by`,
  `cluster_by` and `deduplicate_on` must reference existing columns without
  repeats; the partition column must be `date` or `datetime`; `cluster_by`
  rejects `float` columns, which BigQuery cannot cluster on. Every problem
  in a file is reported in one error.
- `load_dataset_config` reads a dataset YAML file and raises
  `DatasetConfigError` listing every problem as `file: field: reason`, with
  field paths such as `columns[2].type`. Unreadable files, invalid YAML,
  empty files, non-mapping documents and repeated keys are rejected; YAML
  errors include the line number.
- `datasets/example.yaml`, a fictional dataset documenting every config
  option, its allowed values and default.
- Dataset configs for USGS earthquakes of the past 30 days
  (`usgs_earthquakes`, partitioned by event time) and the Washington State
  electric vehicle population (`wa_ev_population`, not partitioned).
- `validate` CLI command: checks one or more dataset files, or every
  `*.yaml` file in a directory, prints `ok: <file>` for each valid file and
  every problem to stderr, and exits with code 3 if any file is invalid or a
  directory has no dataset files.
- Streamed download of `source.url` into `data/raw/<name>/<name>.csv`, with
  connect and read timeouts. The file is written under a temporary name
  and renamed when complete, so a failed download never leaves a truncated
  file; HTTP, timeout and network errors raise `ExtractError`.
- `requests` declared as a direct dependency.
- Local sources: `source.path` is copied into `data/raw/<name>/<name>.csv`
  with the same atomic write as downloads. A relative `source.path` is
  resolved against the directory of the dataset YAML file, not the working
  directory.
- Downloads are skipped when the source is unchanged: the previous
  `Last-Modified` is sent as `If-Modified-Since` and a `304` keeps the
  existing file. URL, `Last-Modified`, size and SHA-256 of each download
  are kept in `data/raw/<name>/<name>.meta.json`. ETags are not used,
  since some portals return unstable ones.
- `scan_raw_csv` reads a raw CSV lazily with every column as a string,
  using the configured delimiter and encoding. Values are not cleaned. A
  missing, empty or unreadable file and repeated header names raise
  `ExtractError`.
- `select_columns` renames mapped source columns to their target names, in
  config order, and drops unmapped columns. When `drop_unmapped_columns`
  is `false`, unmapped columns fail the run instead, as documented in
  `datasets/example.yaml`. Missing and unmapped source columns are listed
  in one `TransformError`.
- `cast_columns` casts `int`, `float` and `bool` columns to `Int64`,
  `Float64` and `Boolean`; `string` values are kept as written. Numeric
  and bool values are trimmed and blank ones become null. Bools accept
  `true` / `false` in any case; floats reject `nan` and `inf`. A value
  that cannot be cast becomes null and its row gets the reason
  `<name>: invalid <type>` in the `#rejections` list column.
- `cast_columns` also parses `date` and `datetime` columns with the
  column's `format` into `Date` and microsecond `Datetime`, with the same
  trimming, blank-to-null and rejection reasons as the other types.
  Datetimes with an offset (`%z`) are converted to UTC and stored without
  a time zone, so the dtype does not depend on the format. Years before
  1000 are rejected, since `%Y` also accepts two- and three-digit years.
- Dataset config validation checks `format` on `date` and `datetime`
  columns: a sample date is written and read back with it, so unknown or
  incomplete directives, formats Polars cannot parse (such as an hour
  without minutes) and formats without a full year, month and day fail
  `validate` before any data is read. `%f`, which misreads fractions of a
  second, and `%Z`, whose time zone name the parser ignores, are also
  rejected.
- `check_nullable` adds the reason `<name>: null` to rows with a null in a
  column declared `nullable: false`. A value already rejected as invalid
  by `cast_columns` is not reported again, so each bad value is counted
  once. Blank numeric, bool and temporal values count as null; empty
  strings in `string` columns are values.
- `check_range` adds `<name>: below min` or `<name>: above max` to rows
  whose `int` or `float` value is outside the column's inclusive `min` /
  `max` bounds. Nulls and values already rejected as invalid are not
  compared.
- `check_duplicates` applies `rules.deduplicate_on`: among rows not
  rejected yet, the first row of each key is kept and later ones get
  `<keys>: duplicate`. Keys compare typed values (strings exactly as
  written, `0.0` equal to `-0.0`), and null key values match each other.
- `split_rejected` returns a `TransformResult` with three lazy frames:
  `valid` rows without the `#rejections` column, `rejected` rows with
  their reasons, and `rejection_counts` with the number of rows per
  reason (`reason`, `rows`), most frequent first. The frames share a
  cached plan, so collecting them with `pl.collect_all` reads the input
  once.
- `transform(lf, config)` runs every step in order (select, cast,
  nullable, range, duplicates) and returns the `TransformResult`.
- `write_parquet` streams a frame to `data/processed/<name>/<name>.parquet`
  with snappy compression. It writes under a temporary name and renames
  the file when complete, so each run replaces the previous output and a
  failed run keeps it.
- `verify_parquet` re-reads a written file's footer and checks its row
  count and schema against the config: exactly the mapped columns, in
  config order, with the dtype of each column type (`output_schema`).
  Mismatches and unreadable files raise `LoadError` listing every
  problem.
- `bigquery_schema` derives the BigQuery schema from the config, one field
  per column in config order: `string` STRING, `int` INT64, `float`
  FLOAT64, `bool` BOOL, `date` DATE, `datetime` DATETIME; `nullable:
  false` columns are REQUIRED, the others NULLABLE.
- `ensure_table` creates the BigQuery target table when it is missing,
  with the config's schema, day partitioning on `target.partition_by`
  and clustering on `target.cluster_by`. An existing table whose
  partitioning or clustering differs from the config raises `LoadError`,
  since BigQuery cannot change them; Google API errors also become
  `LoadError`.
- `load_parquet` replaces the table's data with a Parquet file through a
  load job: explicit schema from the config (no autodetection),
  `WRITE_TRUNCATE`, and the config's partitioning and clustering. It
  waits for the job; an unreadable file or a failed job raises
  `LoadError`.
- `verify_table_rows` checks that the table holds exactly the row count
  of the loaded Parquet file (`parquet_row_count`, read from its
  footer), and raises `LoadError` otherwise. For a partitioned table the
  error points at partition expiration: BigQuery silently drops rows in
  expired partitions, after 60 days in the sandbox.
- `gcp` test (`pytest -m gcp`): loads both fixture datasets into unique
  temporary tables in the BigQuery sandbox twice, checks the row count
  and that the table data equals the Parquet file after each load, and
  deletes the tables. Partition dates are shifted to recent days so the
  sandbox's 60-day partition expiration does not drop them. Skipped
  without `GCP_PROJECT_ID`, `BQ_DATASET` or application default
  credentials.
- `extract`, `transform`, `load` and `run` CLI commands, each taking a
  dataset YAML file. `extract` downloads or copies the raw CSV and
  counts its rows, so a CSV whose body cannot be parsed fails there;
  `transform` writes the valid rows as verified Parquet; `load` loads
  it with the row check; `run` does all three and checks the BigQuery
  settings before downloading anything.
- Structured JSON logging on stderr for `extract`, `transform`, `load`
  and `run`: one `stage_completed` line per stage with the dataset name,
  `rows_in`, `rows_out`, `rows_rejected`, `duration_s` and, for
  `transform`, the rejected rows per reason; a failure is one
  `run_failed` error line with the dataset file and the message.
  Settings and credentials are never logged.
- Distinct exit codes per error category, listed in `--help` and logged
  with each failure as `category` and `exit_code`: 0 success, 1
  unexpected internal error, 2 invalid usage, 3 invalid dataset file or
  missing setting, 4 extract, 5 transform (data does not match the
  config), 6 load (Parquet output or BigQuery, including credential and
  network errors). `write_parquet` reports write failures as
  `LoadError`, and the raw CSV download and copy report file system
  errors (permissions, a full disk) as `ExtractError`; unreadable
  download metadata only causes a fresh download.
- System tests run `run` end to end on both fixture datasets against an
  in-memory BigQuery fake that reads the loaded Parquet back. They check
  the Parquet schema, row count and values, the loaded data, the
  partitioning and clustering sent to BigQuery, one log line per stage,
  and that a second run leaves the same state.
- Coverage threshold: `pytest --cov=config_driven_etl` fails below 85% of
  `src/` (currently about 99%), and `python -m config_driven_etl` is
  covered by a test.
- Dockerfile: two-stage build on `python:3.12-slim` that installs the
  package wheel, runs as the non-root user `app`, and uses
  `python -m config_driven_etl` as entry point, so the dataset file is
  the command argument. Committed dataset files are in the image;
  settings come from environment variables and credentials are mounted
  at run time, never baked in. `.dockerignore` is an allowlist, so only
  the package sources, dataset files and metadata reach the build
  context.
- CI also validates every file in `datasets/`, runs the system tests,
  enforces the coverage threshold on unit and system tests together, and
  builds the Docker image, checked by validating the dataset files inside
  it. The workflow can be called by other workflows.
- CI runs the `gcp` tests on pushes to `main`, authenticated to Google
  Cloud through Workload Identity Federation with no stored key. The job
  is skipped with a notice while the GCP repository secrets are missing,
  and fails, rather than skipping the tests, when credentials cannot be
  obtained.
- Release workflow on `v*` tags: runs CI, checks that the tag is an
  annotated tag on `main` matching the version in `pyproject.toml`,
  builds the Docker image and checks the version it reports, then creates
  a GitHub Release. `vX.Y.Z` takes its notes from the `[X.Y.Z]` changelog
  section; `vX.Y.Z-<suffix>` creates a pre-release from `[Unreleased]`.
- README: architecture diagram, setup, run (local and Docker), a sample
  query, the sandbox expiry rules, how to add a dataset, and tests.

### Fixed

- An error with an empty message no longer crashes reading a raw CSV,
  validating a date format or reading a Parquet file with an
  `IndexError`; the message names the error type instead.

[Unreleased]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.7.0...HEAD
[1.7.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/andrea-forbicia/config-driven-etl/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/andrea-forbicia/config-driven-etl/releases/tag/v1.0.0
