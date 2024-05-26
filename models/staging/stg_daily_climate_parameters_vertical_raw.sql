{{ config(materialized='table')}}



{% for i in range(2000,2025)%}

{%if not loop.first %}
    UNION ALL
{% endif %}


    SELECT

    ID          ::varchar   as record_id,
    DATE        ::varchar   as reading_date,
    ELEMENT     ::varchar   as reading_type,
    DATA_VALUE  ::numeric   as reading_value,
    M_FLAG      ::varchar   as measurement_flag,
    Q_FLAG      ::varchar   as quality_flag,
    S_FLAG      ::varchar   as source_flag,
    OBS_TIME    ::varchar   as obs_recorded_at

    from {{ source('noaa','noaa_ghcn_' ~ "%02d" | format(i)) }}
{% endfor %}