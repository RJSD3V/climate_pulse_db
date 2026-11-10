-- Pivots the long observations to one row per station per day, keeping only values that
-- passed NOAA quality control, and converts raw units using the ghcn_elements seed.
-- Trace amounts (measurement_flag = 'T') are kept: NOAA records them as 0.

with observations as (

    select
        obs.station_id,
        obs.observation_date,
        obs.element,
        obs.raw_value * elements.scale_factor as value,
        obs.source_flag

    from {{ ref('stg_ghcn__daily_observations') }} as obs
    inner join {{ ref('ghcn_elements') }} as elements
        on obs.element = elements.element
    where obs.quality_flag is null

)

select
    station_id,
    observation_date,
    max(case when element = 'TMAX' then value end)         as tmax_c,
    max(case when element = 'TMIN' then value end)         as tmin_c,
    max(case when element = 'PRCP' then value end)         as precipitation_mm,
    max(case when element = 'SNOW' then value end)         as snowfall_mm,
    max(case when element = 'SNWD' then value end)         as snow_depth_mm,
    max(case when element = 'TMAX' then source_flag end)   as temperature_source_flag,
    max(case when element = 'PRCP' then source_flag end)   as precipitation_source_flag

from observations
group by station_id, observation_date
