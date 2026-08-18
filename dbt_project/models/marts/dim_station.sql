{{ config(materialized='table') }}

select distinct
    station_key,
    station_name
from {{ ref('stg_weather') }}
where station_key is not null