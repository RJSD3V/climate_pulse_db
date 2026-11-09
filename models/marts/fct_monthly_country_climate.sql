-- Grain: one row per country per month.
-- Values are averages across every station that reported in that country and month.
-- The station mix changes over time, so compare long periods with care
-- (analyses/warming_trend_by_country.sql uses a fixed set of stations instead).

select
    stations.country_code,
    stations.country_name,
    date_trunc('month', daily.observation_date)::date      as month_start,
    count(distinct daily.station_id)                       as station_count,
    count(*)                                               as station_days,
    avg(daily.tmax_c)                                      as avg_tmax_c,
    avg(daily.tmin_c)                                      as avg_tmin_c,
    avg(daily.tavg_c)                                      as avg_tavg_c,
    max(daily.tmax_c)                                      as max_tmax_c,
    min(daily.tmin_c)                                      as min_tmin_c,
    avg(daily.precipitation_mm)                            as avg_daily_precipitation_mm,
    count(daily.tavg_c)                                    as temperature_station_days,
    count(daily.precipitation_mm)                          as precipitation_station_days

from {{ ref('fct_daily_station_weather') }} as daily
inner join {{ ref('dim_stations') }} as stations
    on daily.station_id = stations.station_id
group by 1, 2, 3
