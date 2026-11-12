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
  every problem to stderr, and exits with code 1 if any file is invalid or a
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

[Unreleased]: https://github.com/andrea-forbicia/config-driven-etl/commits/main
