with source as
    (

        select
            record_id ::varchar as record_id,
            array_slice(reading_date,1,4)::INTEGER as year,
            array_slice(reading_date, 5,6) ::INTEGER as month,
            array_slice(reading_date, 7,8) ::INTEGER as day,
            reading_type,
            reading_value,
            measurement_flag,
            quality_flag,
            source_flag,
            obs_recorded_at

    from "dev_database"."dev_sode"."stg_daily_climate_parameters_vertical_raw"

),

final as
(

  select
        record_id,
        make_date(year,month,day)::timestamp as reading_date,
        reading_type,
        reading_value,
        measurement_flag,
        quality_flag,
        source_flag,
        obs_recorded_at

     from source WHERE reading_type in ('PRCP','SNOW','SNWD','TMAX','TMIN') and quality_flag is null and measurement_flag != 'T'
)

select *
from final