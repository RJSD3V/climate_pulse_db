-- One row per GHCN-D station, with country/state names and each station's coverage
-- in the loaded data (observation_days = 0 for stations with nothing loaded).

with coverage as (

    select
        station_id,
        min(observation_date)   as first_observation_date,
        max(observation_date)   as last_observation_date,
        count(*)                as observation_days

    from {{ ref('fct_daily_station_weather') }}
    group by station_id

)

select
    stations.station_id,
    stations.station_name,
    stations.country_code,
    countries.country_name,
    stations.state_code,
    states.state_name,
    stations.latitude,
    stations.longitude,
    stations.elevation_m,
    stations.is_gsn_station,
    stations.hcn_crn_flag,
    stations.wmo_id,
    coverage.first_observation_date,
    coverage.last_observation_date,
    coalesce(coverage.observation_days, 0) as observation_days

from {{ ref('stg_ghcn__stations') }} as stations
left join {{ ref('stg_ghcn__countries') }} as countries
    on stations.country_code = countries.country_code
left join {{ ref('stg_ghcn__states') }} as states
    on stations.state_code = states.state_code
left join coverage
    on stations.station_id = coverage.station_id
