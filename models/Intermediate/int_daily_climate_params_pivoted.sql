{{ config(materialized='view')}}
with source as (

    SELECT
      record_id,
      reading_date,
      reading_type,
      reading_value,
      source_flag,
      measurement_flag


    from {{ ref('stg_daily_climate_parameters')}}
),

pivoted as (
  select 
    *

    from (
      PIVOT source on reading_type IN ('PRCP','SNOW','SNWD','TMAX','TMIN') using first(reading_value) group by reading_date, source_flag, measurement_flag
    )
)



select 
    
    reading_date,
    source_flag,
    measurement_flag,
    PRCP/10 ::DECIMAL as precipitation_mm,
    TMAX/10 ::DECIMAL as daily_max_temperature_celsius,
    TMIN/10 ::DECIMAL as daily_min_temperature_celsius,
    SNWD    ::DECIMAL as snow_depth_mm,
    SNOW    ::DECIMAL as snow_fall_mm

    
     from pivoted