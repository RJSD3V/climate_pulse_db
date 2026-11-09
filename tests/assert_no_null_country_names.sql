-- Every country that has data should resolve to a name from ghcnd-countries.txt.
{{ config(severity='warn') }}

select distinct country_code
from {{ ref('fct_monthly_country_climate') }}
where country_name is null
