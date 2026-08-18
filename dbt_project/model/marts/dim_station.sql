{{ config(materialized='table') }}

select distinct
    station_id,
    station_name,
    latitude,
    longitude,
    elevation
from {{ ref('stg_weather') }}
where station_id is not null