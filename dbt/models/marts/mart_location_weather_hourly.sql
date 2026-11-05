{{ config(
    partition_by={'field': 'time_utc', 'data_type': 'timestamp', 'granularity': 'day'},
    cluster_by=['location_id']
) }}

-- Published copy of the latest complete weather per location and hour.
select
    location_id,
    location_name,
    location_type,
    region_id,
    marine_zone_id,
    time_utc,
    time_local,
    temperature_2m,
    precipitation,
    precipitation_probability,
    cast(weather_code as int64) as weather_code,
    cape,
    wind_speed_10m,
    wind_direction_10m,
    wind_gusts_10m,
    wave_height,
    wave_period,
    wave_direction,
    forecast_run_id,
    forecast_fetched_at,
    marine_fetched_at,
    is_past
from {{ ref('int_location_weather_hourly') }}
