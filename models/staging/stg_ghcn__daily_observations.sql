-- One row per station, per day, per core element. Renames, types and scopes the raw data;
-- quality filtering and unit conversion happen downstream in the intermediate layer.

select
    "ID"                                    as station_id,
    left("ID", 2)                           as country_code,
    strptime("DATE", '%Y%m%d')::date        as observation_date,
    "ELEMENT"                               as element,
    "DATA_VALUE"                            as raw_value,
    "M_FLAG"                                as measurement_flag,
    "Q_FLAG"                                as quality_flag,
    "S_FLAG"                                as source_flag,
    "OBS_TIME"                              as observation_time,
    source_year,
    loaded_at

from {{ source('ghcn', 'ghcn_daily') }}
where "ELEMENT" in (
    {%- for element in var('core_elements') %}
    '{{ element }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
