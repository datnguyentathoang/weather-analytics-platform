{{ config(materialized='table') }}

select
    row_number() over (order by observation_timestamp, station_key) as observation_id,
    station_key,
    cast(observation_timestamp as date) as date_key,
    observation_timestamp,
    temperature,
    humidity,
    apparent_temperature,
    precipitation,
    wind_speed,
    pressure,
    case 
        when precipitation > 90.0 or temperature > 38.0 then 1 
        else 0 
    end as anomaly_flag
from {{ ref('stg_weather') }}
where station_key is not null