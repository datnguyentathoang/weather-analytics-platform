select
    coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') as station_key,
    case 
        when coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') = 'CITY_HANOI' then 'Thành phố Hà Nội'
        when coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') = 'CITY_SAIGON' then 'Thành phố Hồ Chí Minh'
        when coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') = 'CITY_DANANG' then 'Thành phố Đà Nẵng'
        when coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') = 'CITY_DALAT' then 'Thành phố Đà Lạt'
        when coalesce(to_jsonb(s)->>'station_key', to_jsonb(s)->>'station_id') = 'CITY_HAIPHONG' then 'Thành phố Hải Phòng'
        else 'Thành phố Cần Thơ'
    end as station_name,
    coalesce(
        case 
            when to_jsonb(s)->>'ThoiGian' ~ '^\d{2}-\d{2}-\d{4}' 
            then to_timestamp(to_jsonb(s)->>'ThoiGian', 'DD-MM-YYYY HH24:MI:SS')
            else cast(to_jsonb(s)->>'ThoiGian' as timestamp)
        end,
        cast(to_jsonb(s)->>'timestamp' as timestamp),
        cast(to_jsonb(s)->>'observed_at' as timestamp)
    ) as observation_timestamp,
    cast(coalesce(to_jsonb(s)->>'NhietDo', to_jsonb(s)->>'temperature') as numeric) as temperature,
    cast(coalesce(to_jsonb(s)->>'DoAm', to_jsonb(s)->>'humidity') as numeric) as humidity,
    cast(coalesce(to_jsonb(s)->>'CamGiacNhu', to_jsonb(s)->>'apparent_temperature') as numeric) as apparent_temperature,
    cast(coalesce(to_jsonb(s)->>'LuongMua', to_jsonb(s)->>'precipitation') as numeric) as precipitation,
    cast(coalesce(to_jsonb(s)->>'TocDoGio', to_jsonb(s)->>'wind_speed') as numeric) as wind_speed,
    cast(coalesce(to_jsonb(s)->>'ApSuat', to_jsonb(s)->>'pressure') as numeric) as pressure
from public.staging_weather s