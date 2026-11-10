select
    country_code,
    -- NOAA appends notes in brackets to some names, e.g. "Ukraine [Ukraine]"
    trim(regexp_replace(country_name, '\s*\[.*\]$', '')) as country_name

from {{ source('ghcn', 'ghcn_countries') }}
