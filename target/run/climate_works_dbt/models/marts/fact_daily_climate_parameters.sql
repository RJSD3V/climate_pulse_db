
  
    
    

    create  table
      "dev_database"."dev_sode"."fact_daily_climate_parameters__dbt_tmp"
  
    as (
      

with source as (
    select
    reading_date,
    source_flag,
    measurement_flag,
    precipitation_mm,
    daily_max_temperature_celsius,
    daily_min_temperature_celsius,
    snow_fall_mm,
    snow_depth_mm

    from "dev_database"."dev_sode"."int_daily_climate_params_pivoted"
    order by reading_date desc
)

select * from source
    );
  
  