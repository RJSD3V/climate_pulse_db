-- Quality-controlled temperatures should sit inside the world records
-- (-89.2 °C at Vostok, 56.7 °C at Death Valley), with a small margin.
{{ config(severity='warn') }}

select *
from {{ ref('fct_daily_station_weather') }}
where tmax_c not between -90 and 60
   or tmin_c not between -90 and 60
