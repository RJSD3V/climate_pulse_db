with source as (

    SELECT
    ID          ::varchar   as record_id,
    DATE        ::varchar as reading_date,
    ELEMENT     ::varchar   as reading_type,
    DATA_VALUE  ::numeric   as reading_value,
    M_FLAG      ::varchar   as measurement_flag,
    Q_FLAG      ::varchar   as quality_flag,
    S_FLAG      ::varchar   as source_flag,
    OBS_TIME    ::varchar as obs_recorded_at

    from "dev_database"."dev_sode"."noaa_ghcn_2022"


    UNION ALL

    SELECT

    ID          ::varchar   as record_id,
    DATE        ::varchar as reading_date,
    ELEMENT     ::varchar   as reading_type,
    DATA_VALUE  ::numeric   as reading_value,
    M_FLAG      ::varchar   as measurement_flag,
    Q_FLAG      ::varchar   as quality_flag,
    S_FLAG      ::varchar   as source_flag,
    OBS_TIME    ::varchar as obs_recorded_at

    from "dev_database"."dev_sode"."noaa_ghcn_2023"
)

select * from source