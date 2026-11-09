"""Load NOAA GHCN-Daily data into DuckDB / MotherDuck.

Usage:
    python get_data.py 2023                  # one year (+ station metadata)
    python get_data.py 2000 2024             # an inclusive range of years
    python get_data.py 2023 --no-metadata    # skip stations/countries/states
    python get_data.py 2023 --db md:climate_works   # MotherDuck (needs MOTHERDUCK_TOKEN)

Raw data lands in the `raw` schema, which the dbt sources read from.
"""

import argparse
import os
import time
import urllib.error
import urllib.request

import duckdb
import pandas as pd

# The NOAA bucket is public, so files are downloaded over HTTPS (no AWS keys needed).
NOAA_BASE_URL = "https://noaa-ghcn-pds.s3.amazonaws.com"
# Downloads go to data/raw/, which is git-ignored.
RAW_DIR = './data/raw'
DB_PATH = os.getenv('CLIMATE_DB', './dev_database.duckdb')
RAW_SCHEMA = 'raw'

MAX_RETRIES = 5
TIMEOUT_SECONDS = 60

# The by_year files have NO header row, so the columns are named and typed here.
# Names match the dbt source (models/staging/_ghcn__sources.yml).
DAILY_COLUMNS = {
    'ID': 'VARCHAR',
    'DATE': 'VARCHAR',
    'ELEMENT': 'VARCHAR',
    'DATA_VALUE': 'INTEGER',
    'M_FLAG': 'VARCHAR',
    'Q_FLAG': 'VARCHAR',
    'S_FLAG': 'VARCHAR',
    'OBS_TIME': 'VARCHAR',
}

# Fixed-width layouts from the NOAA GHCN-D readme: (column, start, end), 1-indexed inclusive.
METADATA_FILES = {
    'ghcn_stations': ('ghcnd-stations.txt', [
        ('station_id', 1, 11),
        ('latitude', 13, 20),
        ('longitude', 22, 30),
        ('elevation', 32, 37),
        ('state_code', 39, 40),
        ('station_name', 42, 71),
        ('gsn_flag', 73, 75),
        ('hcn_crn_flag', 77, 79),
        ('wmo_id', 81, 85),
    ]),
    'ghcn_countries': ('ghcnd-countries.txt', [('country_code', 1, 2), ('country_name', 4, 64)]),
    'ghcn_states': ('ghcnd-states.txt', [('state_code', 1, 2), ('state_name', 4, 50)]),
}


def download_file(url, file_path):
    """Download `url` to `file_path`, retrying and resuming if the connection drops.

    Data is written to a `.part` file first and renamed when complete, so a half-finished
    download is never mistaken for a cached one.
    """
    if os.path.exists(file_path):
        print(f"The file {file_path} already exists")
        return True

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    part_path = file_path + '.part'

    for attempt in range(1, MAX_RETRIES + 1):
        done = os.path.getsize(part_path) if os.path.exists(part_path) else 0
        request = urllib.request.Request(url)
        if done:
            # Ask only for the bytes we don't have yet.
            request.add_header('Range', f'bytes={done}-')
            print(f"Resuming {url} from {done / 1e6:,.1f} MB")
        else:
            print(f"Downloading {url}")

        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as resp:
                # 206 = server honoured the Range request, so append; 200 = full file, start over.
                mode = 'ab' if resp.status == 206 else 'wb'
                with open(part_path, mode) as f:
                    # Stream in 1 MB chunks: a year of data is hundreds of MB.
                    while chunk := resp.read(1024 * 1024):
                        f.write(chunk)
            os.rename(part_path, file_path)
            print("Downloaded successfully")
            return True

        except urllib.error.HTTPError as e:
            if e.code == 416 and done:
                # Range not satisfiable: the .part file already holds the whole file.
                os.rename(part_path, file_path)
                return True
            if e.code == 404:
                print(f"Not found: {url}")
                return False
            error = e
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            error = e

        wait = 2 ** attempt
        print(f"Attempt {attempt}/{MAX_RETRIES} failed ({error}); retrying in {wait}s")
        time.sleep(wait)

    print(f"Giving up on {url} after {MAX_RETRIES} attempts. Re-run to resume.")
    return False


def get_noaa_file(year):
    url = f"{NOAA_BASE_URL}/csv.gz/by_year/{year}.csv.gz"
    return download_file(url, f'{RAW_DIR}/{year}.csv.gz')


def load_data_to_db(con, year):
    """Load one year into raw.ghcn_daily. Re-running a year replaces it instead of duplicating it."""
    file_path = f'{RAW_DIR}/{year}.csv.gz'
    table_name = f'{RAW_SCHEMA}.ghcn_daily'
    column_defs = ', '.join(f'{c} {t}' for c, t in DAILY_COLUMNS.items())
    # DuckDB reads the .gz directly (no unzip step, no pandas), which is much faster
    # and uses far less memory on files with tens of millions of rows.
    columns = '{' + ', '.join(f"'{c}': '{t}'" for c, t in DAILY_COLUMNS.items()) + '}'

    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        f"({column_defs}, source_year INTEGER, loaded_at TIMESTAMP)"
    )
    # Delete + insert in one transaction: a failed load leaves the old data untouched.
    con.begin()
    try:
        con.execute(f"DELETE FROM {table_name} WHERE source_year = ?", [year])
        con.execute(
            f"INSERT INTO {table_name} "
            f"SELECT *, ? AS source_year, current_timestamp AS loaded_at "
            f"FROM read_csv(?, header = false, columns = {columns})",
            [year, file_path],
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    rows = con.execute(
        f"SELECT count(*) FROM {table_name} WHERE source_year = ?", [year]
    ).fetchone()[0]
    print(f"{table_name}: {rows:,} rows for {year}")


def push_metadata(con):
    """Load stations, countries and states (NOAA fixed-width text files) into the raw schema.

    Every column is loaded as text; typing happens in the dbt staging models.
    """
    for table_name, (file_name, layout) in METADATA_FILES.items():
        file_path = f'{RAW_DIR}/{file_name}'
        if not download_file(f"{NOAA_BASE_URL}/{file_name}", file_path):
            raise RuntimeError(f"Could not download {file_name}")
        df = pd.read_fwf(
            file_path,
            colspecs=[(start - 1, end) for _, start, end in layout],
            names=[name for name, _, _ in layout],
            dtype=str,
            keep_default_na=False,
            encoding='latin-1',
        )
        df = df.replace('', None)
        con.register('metadata_df', df)
        con.execute(f"CREATE OR REPLACE TABLE {RAW_SCHEMA}.{table_name} AS SELECT * FROM metadata_df")
        con.unregister('metadata_df')
        print(f"{RAW_SCHEMA}.{table_name}: {len(df):,} rows")


def get_all_noaa_files(start_year, end_year, db_path=DB_PATH, metadata=True):
    print(f"Target database: {db_path}")
    failed = []
    with duckdb.connect(db_path) as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        if metadata:
            print("Getting station metadata")
            push_metadata(con)
        for y in range(start_year, end_year + 1):
            print(f"Getting NOAA Data for year: {y}")
            if get_noaa_file(y):
                load_data_to_db(con, y)
            else:
                print(f"Skipping {y}: download failed")
                failed.append(y)
    if failed:
        print(f"Finished with failures for: {failed}. Re-run those years to resume.")
        return False
    print("All files retrieved and loaded.")
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('start_year', type=int)
    parser.add_argument('end_year', type=int, nargs='?', help='defaults to start_year')
    parser.add_argument('--db', default=DB_PATH, help=f'DuckDB file or md:<database> (default: {DB_PATH})')
    parser.add_argument('--no-metadata', action='store_true', help='skip stations/countries/states')
    args = parser.parse_args()

    end_year = args.end_year or args.start_year
    if end_year < args.start_year:
        parser.error('end_year must be >= start_year')

    ok = get_all_noaa_files(args.start_year, end_year, args.db, metadata=not args.no_metadata)
    raise SystemExit(0 if ok else 1)
