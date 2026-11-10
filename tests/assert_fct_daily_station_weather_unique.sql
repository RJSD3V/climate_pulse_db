-- fct_daily_station_weather must have exactly one row per station per day.
select
    station_id,
    observation_date,
    count(*) as row_count
from {{ ref('fct_daily_station_weather') }}
group by station_id, observation_date
having count(*) > 1
