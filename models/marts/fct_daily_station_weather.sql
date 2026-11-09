-- Grain: one row per station per day.

select
    station_id,
    observation_date,
    tmax_c,
    tmin_c,
    case
        when tmax_c is not null and tmin_c is not null then (tmax_c + tmin_c) / 2
    end                                 as tavg_c,
    tmax_c - tmin_c                     as temperature_range_c,
    precipitation_mm,
    snowfall_mm,
    snow_depth_mm,
    temperature_source_flag,
    precipitation_source_flag

from {{ ref('int_station_daily_weather') }}
