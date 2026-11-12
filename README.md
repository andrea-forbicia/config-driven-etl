# config-driven-etl

A generic batch ETL pipeline: any public open-data CSV is extracted, cleaned
and transformed with Polars, written as Parquet, and loaded into Google
BigQuery. Each dataset is described by a YAML file; adding a dataset means
adding a file, not writing code.

Work in progress.

## Datasets

| Dataset | Publisher | Source | License |
|---------|-----------|--------|---------|
| Earthquakes, past 30 days | U.S. Geological Survey | [all_month.csv](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv) | [Public domain](https://www.usgs.gov/information-policies-and-instructions/copyrights-and-credits) |
| Electric Vehicle Population | Washington State Department of Licensing | [data.wa.gov](https://data.wa.gov/d/f6w7-q2d2) | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) |

## License

[MIT](LICENSE)
