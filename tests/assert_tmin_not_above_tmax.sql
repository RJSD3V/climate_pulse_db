-- A day's minimum temperature should not exceed its maximum.
-- Warn rather than fail: NOAA occasionally publishes such pairs when TMAX and TMIN
-- come from different observation windows.
{{ config(severity='warn') }}

select *
from {{ ref('fct_daily_station_weather') }}
where tmin_c > tmax_c
