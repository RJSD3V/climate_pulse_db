select
    station_id,
    left(station_id, 2)                                     as country_code,
    state_code,
    station_name,
    latitude::double                                        as latitude,
    longitude::double                                       as longitude,
    -- NOAA uses -999.9 for "elevation unknown"
    nullif(elevation::double, -999.9)                       as elevation_m,
    coalesce(gsn_flag = 'GSN', false)                       as is_gsn_station,
    hcn_crn_flag,
    wmo_id

from {{ source('ghcn', 'ghcn_stations') }}
