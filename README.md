<img src="https://github.com/RJSD3V/climate_works_db/assets/20220544/7303deac-dcfd-4ec7-baf8-d48785b6eac3" alt="ClimateWorksLogo" width="200"/>

# ClimateWorks DB - A Climatology Essay written purely in Data.

<p align="center">
  <img src="assets/images/dataengbytes-sydney.jpeg" alt="Raajas Sode presenting 'Building end-to-end Data Pipelines locally with duckdb' at the DataEngBytes meetup in Sydney" width="700"/>
  <br/>
  <em>Presented at the <strong>DataEngBytes</strong> meetup in Sydney: "Building end-to-end Data Pipelines locally with DuckDB".</em>
</p>

An end-to-end ELT pipeline over **NOAA's Global Historical Climatology Network - Daily (GHCN-D)**:
more than 100,000 weather stations and over 150 years of daily temperature, precipitation and snow
readings. It is turned into tested, documented analytics tables and an interactive dashboard.

```
NOAA public S3 bucket ──► get_data.py ──► DuckDB / MotherDuck (raw) ──► dbt (staging → intermediate → marts) ──► Streamlit
```

![climatology_model](https://github.com/RJSD3V/climate_works_dbt/assets/20220544/1fe77541-9987-40f1-91b4-67a76aab30ba)

## Highlights

- **Reproducible from a fresh clone.** `pip install`, `make ingest`, `make build`, `make dashboard`. No cloud account or AWS keys are needed: the NOAA bucket is public and is read over HTTPS.
- **Idempotent ingestion.** Re-loading a year replaces it instead of duplicating it. Gzipped source files are about 8× smaller than the CSVs.
- **Correct grain.** Raw long-format readings become one row per station per day, after dropping every value that failed NOAA quality control.
- **Enriched with metadata.** Every station is joined to its location, country and U.S. state / Canadian province.
- **Tested and documented.** Over 30 dbt tests (uniqueness, not-null, referential integrity, accepted values, plausibility checks) and column-level docs for every model.
- **Dashboard.** Compare up to 8 countries, look at monthly highs and lows, see yearly temperature anomalies, and map the reporting stations. Works in light and dark mode.

## Tech stack

| Layer          | Tool                                                                 |
| -------------- | -------------------------------------------------------------------- |
| Source data    | [NOAA GHCN-D](https://docs.opendata.aws/noaa-ghcn-pds/readme.html), public S3 bucket `noaa-ghcn-pds` |
| Ingestion      | Python + DuckDB's bulk CSV reader                                    |
| Warehouse      | [DuckDB](https://duckdb.org/) (local) or [MotherDuck](https://motherduck.com/) (cloud) |
| Transformation | [dbt](https://docs.getdbt.com/) with the `dbt-duckdb` adapter        |
| Dashboard      | [Streamlit](https://streamlit.io/) + [Altair](https://altair-viz.github.io/) |

## Quick start

```bash
python -m venv venv && source venv/bin/activate
make setup                     # pip install -r requirements.txt
make ingest YEARS="2022 2023"  # station metadata + two years of daily data (~350 MB download)
make build                     # dbt build: seeds, models and tests
make dashboard                 # opens http://localhost:8501
```

Run `make help` to list every task. Without `make`:

```bash
python get_data.py 2022 2023
dbt build
streamlit run dashboard.py
```

> Each year of GHCN-D is about 170 MB gzipped and 35M+ rows. One or two recent years are plenty to explore.
> Load a longer range (e.g. `make ingest YEARS="2000 2024"`) to see long-term trends in the anomaly chart.

## The Data

NOAA publishes GHCN-D as one file per year. Each row is a single reading from one station on one day,
in **vertical (long) format**:

| Column       | Description                                                  |
| ------------ | ------------------------------------------------------------ |
| `ID`         | Station identifier. The first two characters are the FIPS country code |
| `DATE`       | Observation date (`YYYYMMDD`)                                |
| `ELEMENT`    | Reading type. This project models the five core elements: `TMAX`, `TMIN`, `PRCP`, `SNOW`, `SNWD` |
| `DATA_VALUE` | Raw value (temperatures in tenths of °C, precipitation in tenths of mm, snow in mm) |
| `M_FLAG`     | Measurement flag (e.g. `T` = trace)                          |
| `Q_FLAG`     | Quality flag. Empty means the value passed every NOAA quality check |
| `S_FLAG`     | Source flag: which of about 35 national or international sources the value came from |
| `OBS_TIME`   | Observation time (`HHMM`)                                    |

Station metadata comes from three fixed-width files in the same bucket: `ghcnd-stations.txt`,
`ghcnd-countries.txt` and `ghcnd-states.txt`.

### Ingestion: `get_data.py`

```bash
python get_data.py 2023                  # station metadata + one year -> raw.ghcn_stations / ghcn_countries / ghcn_states / ghcn_daily
python get_data.py 2000 2024             # an inclusive range of years
python get_data.py 2023 --no-metadata    # daily data only
```

| Option          | Default                                        | Purpose |
| --------------- | ---------------------------------------------- | ------- |
| `--db`          | `$CLIMATE_DB`, otherwise `dev_database.duckdb` | Target database. Use `--db md:climate_works` for MotherDuck |
| `--no-metadata` | off                                            | Skip reloading stations, countries and states |

- **Safe to re-run.** Re-loading a year replaces its rows in `raw.ghcn_daily` in one transaction instead of duplicating them.
- **Resumable downloads.** Files are cached in `data/raw/` (git-ignored). A dropped connection is retried up to 5 times and resumes from where it stopped. If it still fails, re-running the same command picks up from the partial file.

DuckDB specializes in fast ingestion from formats like CSV and Parquet: a year of 35M+ rows loads in seconds once downloaded.

## Running the Data processing pipeline

The dbt project turns the raw readings into analytics tables:

```
raw.ghcn_daily ─────► stg_ghcn__daily_observations ─► int_station_daily_weather ─► fct_daily_station_weather ─┐
raw.ghcn_stations ──► stg_ghcn__stations ──┐                                                                    │
raw.ghcn_countries ─► stg_ghcn__countries ─┼──────────────────────────────────────► dim_stations ◄─────────────┤
raw.ghcn_states ────► stg_ghcn__states ────┘                                             │                      │
seeds: ghcn_elements, ghcn_source_flags                                                   └─► fct_monthly_country_climate
```

| Layer        | Model | What it does |
| ------------ | ----- | ------------ |
| staging      | `stg_ghcn__daily_observations` | Renames and types the raw columns, parses dates, and keeps only the five core elements |
| staging      | `stg_ghcn__stations`, `stg_ghcn__countries`, `stg_ghcn__states` | Types station metadata (lat/long, elevation, GSN/HCN flags) and cleans names |
| intermediate | `int_station_daily_weather` | Drops values that failed QC, converts units using the `ghcn_elements` seed, and pivots to one row per station per day |
| marts        | `fct_daily_station_weather` | Daily high, low, mean and range (°C), precipitation, snowfall and snow depth (mm). **Grain: station × day** |
| marts        | `dim_stations` | Every station with location, country and state names, and its coverage in the loaded data |
| marts        | `fct_monthly_country_climate` | Monthly averages and extremes per country. **Grain: country × month** |

Seeds:
- `ghcn_elements`: the core elements and their unit conversions.
- `ghcn_source_flags`: all source flags with NOAA's priority order, transcribed from the GHCN-D readme.

### Tests

`dbt build` runs every test alongside the models:

- **Generic tests:** `unique` / `not_null` on keys, `accepted_values` on elements, and `relationships` from facts to `dim_stations` and from source flags to the seed.
- **Singular tests** (`tests/`):
  - one row per station per day in `fct_daily_station_weather`
  - temperatures within world-record bounds (warn)
  - TMIN ≤ TMAX (warn)
  - every country code resolves to a name (warn)

### Docs

```bash
make docs    # dbt docs generate && dbt docs serve
```

This opens the dbt docs site, with descriptions for every model and column and the full lineage graph.

### Analyses

`analyses/warming_trend_by_country.sql` compares each country's first and last loaded years using only
stations that report in both. That way, changes in the station network don't show up as a fake climate
signal. Compile it with `dbt compile --select warming_trend_by_country` and run the SQL in DuckDB.

## Dashboard

```bash
make dashboard   # streamlit run dashboard.py
```

- **Country comparison:** average daily temperature and precipitation by month, for up to 8 countries. Hovering shows every country's value for that month.
- **Country detail:** average daily highs and lows, plus a yearly temperature anomaly chart (needs 3+ complete years loaded).
- **Station map:** every reporting station, colored to match its country's line in the charts.
- **Data table** with the numbers behind the charts.

> A local DuckDB file can have only one writer, and nothing can write to it while another process has it open.
> Stop the dashboard before running `get_data.py` or `dbt build` against the same file. MotherDuck doesn't have this limitation.

<img src="" alt="ClimateWorksLogo" width="200"/>

## Configuration

| Variable           | Used by                 | Default               |
| ------------------ | ----------------------- | --------------------- |
| `CLIMATE_DB`       | ingest, dbt, dashboard  | `dev_database.duckdb` |
| `CLIMATE_SCHEMA`   | dashboard               | `dev_sode`            |
| `MOTHERDUCK_TOKEN` | MotherDuck connections  | none                  |

dbt targets (`profiles.yml`):

| Target              | Database                     | Schema      |
| ------------------- | ---------------------------- | ----------- |
| `cw_dev` (default)  | local `dev_database.duckdb`  | `dev_sode`  |
| `cw_md_dev`         | MotherDuck `climate_works`   | `dev_sode`  |
| `cw_md_prod`        | MotherDuck `climate_works`   | `prod_sode` |

### Running on MotherDuck

```bash
export MOTHERDUCK_TOKEN=...                       # or put it in .env (see .env.example)
python get_data.py 2000 2024 --db md:climate_works
dbt build --target cw_md_prod
CLIMATE_DB=md:climate_works CLIMATE_SCHEMA=prod_sode streamlit run dashboard.py
```

## Project structure

```
climate_pulse_db/
├── get_data.py               # Download GHCN-D data + metadata from NOAA and load into the raw schema (resumable, safe to re-run)
├── dashboard.py              # Streamlit dashboard over the marts
├── Makefile                  # setup / ingest / build / docs / dashboard / clean
├── models/
│   ├── staging/              # sources + stg_ghcn__* models
│   ├── intermediate/         # int_station_daily_weather
│   ├── marts/                # fct_daily_station_weather, dim_stations, fct_monthly_country_climate
│   └── overview.md           # dbt docs landing page
├── seeds/                    # ghcn_elements.csv, ghcn_source_flags.csv
├── tests/                    # singular data tests
├── analyses/                 # warming_trend_by_country.sql
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
└── .env.example
```

## Data caveats

- **Station coverage changes over time.** Country averages mix whichever stations reported that month,
  so a shift in the network can move the average. The analysis in `analyses/` controls for this.
- **Source quality varies.** Values from source `S` (Global Summary of the Day) come from hourly
  synoptic reports, and NOAA advises using them with caution, especially for precipitation. Every daily
  row keeps its source flag, so these values can be filtered out.
- **Coverage is uneven.** Countries like the U.S., Canada and Australia have thousands of stations; many others have only a few.

## Upcoming changes

- Database to be dockerised with a ClickHouse instance.
- Incremental materialization for `fct_daily_station_weather` when many years are loaded.
- Climate normals (1991–2020 baselines) for anomalies relative to a standard period.
- More elements (e.g. `TAVG`, `AWND`), and state-level views for the U.S. and Canada.
- CI that runs `dbt build` against a small sample year on every pull request.

## Resources

- NOAA GHCN-D [readme on AWS Open Data](https://docs.opendata.aws/noaa-ghcn-pds/readme.html)
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- [DuckDB docs](https://duckdb.org/docs/) and [MotherDuck docs](https://motherduck.com/docs/)

---

> [!CAUTION]
> **Note on AI use.** I designed and built this project myself: the NOAA ingestion, the DuckDB/MotherDuck warehouse, the dbt models and the original dashboard. I later used AI (Claude) as an assistant to refactor and fix bugs in the pipeline, add dbt tests and documentation, rework the dashboard, and tidy up this README. I reviewed every change.
