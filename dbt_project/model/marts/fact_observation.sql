{{ config(materialized='table') }}

select
    md5(concat(station_id, '_', observed_at)) as observation_id,
    station_id,
    observed_at,
    temperature,
    humidity,
    pressure,
    wind_speed,
    precipitation
from {{ ref('stg_weather') }}
where station_id is not null