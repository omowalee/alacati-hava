-- Forecast converted to hour INTERVALS (time convention in config/alerts.yaml):
--   * a row labelled T holds instantaneous values at T (wind, gusts, CAPE, weather_code)
--     -> they describe the interval [T, T+1h)
--   * precipitation in row T is the sum of the PRECEDING hour -> interval [T-1h, T)
-- So the interval [H, H+1h) takes wind/CAPE/code from row H and precipitation from row H+1h.
-- The first interval of a series has precipitation only, the last one has no precipitation.
with instantaneous as (

    select
        location_id,
        time_utc as interval_start_utc,
        weather_code,
        cape,
        wind_speed_10m,
        wind_gusts_10m
    from {{ ref('stg_forecast_hourly') }}

),

precipitation as (

    select
        location_id,
        timestamp_sub(time_utc, interval 1 hour) as interval_start_utc,
        precipitation
    from {{ ref('stg_forecast_hourly') }}

)

select
    location_id,
    interval_start_utc,
    timestamp_add(interval_start_utc, interval 1 hour) as interval_end_utc,
    instantaneous.weather_code,
    instantaneous.cape,
    instantaneous.wind_speed_10m,
    instantaneous.wind_gusts_10m,
    precipitation.precipitation
from instantaneous
full outer join precipitation using (location_id, interval_start_utc)
