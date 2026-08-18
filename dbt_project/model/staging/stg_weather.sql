select
    cast(station_id as varchar) as station_id,
    cast(station_name as varchar) as station_name,
    cast(latitude as numeric) as latitude,
    cast(longitude as numeric) as longitude,
    cast(elevation as numeric) as elevation,
    cast(timestamp as timestamp) as observed_at,
    cast(temperature as numeric) as temperature,
    cast(humidity as numeric) as humidity,
    cast(pressure as numeric) as pressure,
    cast(wind_speed as numeric) as wind_speed,
    cast(precipitation as numeric) as precipitation
from public.staging_weather