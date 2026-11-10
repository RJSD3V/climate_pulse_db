-- Warming trend per country, using only stations that report in BOTH the first and the last
-- year loaded, so changes in the station network don't masquerade as climate change.
-- Run with:  dbt compile --select warming_trend_by_country
-- then paste target/compiled/.../warming_trend_by_country.sql into duckdb.

with yearly as (

    select
        station_id,
        year(observation_date)  as year,
        avg(tavg_c)             as tavg_c,
        count(tavg_c)           as days

    from {{ ref('fct_daily_station_weather') }}
    group by 1, 2
    -- require most of the year to be present
    having count(tavg_c) >= 330

),

bounds as (

    select min(year) as first_year, max(year) as last_year from yearly

),

paired as (

    select
        first_yr.station_id,
        last_yr.tavg_c - first_yr.tavg_c as change_c

    from yearly as first_yr
    inner join yearly as last_yr
        on first_yr.station_id = last_yr.station_id
    cross join bounds
    where first_yr.year = bounds.first_year
      and last_yr.year = bounds.last_year

)

select
    stations.country_name,
    count(*)                        as stations,
    round(avg(paired.change_c), 2)  as avg_change_c

from paired
inner join {{ ref('dim_stations') }} as stations
    on paired.station_id = stations.station_id
group by 1
having count(*) >= 10
order by avg_change_c desc
