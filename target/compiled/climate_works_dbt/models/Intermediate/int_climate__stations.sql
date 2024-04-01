

with
stations as (
    select 
            'S' as source_label,
            'Global Summary of the Day (NCDC DSI-9618)' as source_name,
            'NCDC DSI-9618' as ncdc_code
    

    UNION ALL

    select 
            '0' as source_label,
            'U.S. Cooperative Summary of the Day' as source_name,
            'NCDC DSI-3200' as ncdc_code

    UNION ALL

    select 
            '6' as source_label,
            'CDMP Cooperative Summary of the Day ' as source_name,
            'NCDC DSI-3206' as ncdc_code

    UNION ALL 

    select
            '7' as source_label,
            'U.S. Cooperative Summary of the Day – Transmitted via WxCoder3' as source_name,
            'NCDC SI-3207' as ncdc_code
)

select * from stations