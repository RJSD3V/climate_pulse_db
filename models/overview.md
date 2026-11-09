{% docs __overview__ %}
# ClimateWorks DB

An ELT pipeline over NOAA's **Global Historical Climatology Network - Daily (GHCN-D)**.

- `raw` schema: loaded by `get_data.py` straight from the public NOAA bucket.
- **staging** (`stg_ghcn__*`): rename, type and scope the raw tables.
- **intermediate** (`int_station_daily_weather`): drop values that failed NOAA quality
  control, pivot to one row per station per day, convert to °C and mm.
- **marts**: `fct_daily_station_weather`, `dim_stations`, `fct_monthly_country_climate`.

Source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html
{% enddocs %}
