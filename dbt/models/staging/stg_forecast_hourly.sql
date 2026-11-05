-- Forecast rows as labelled by Open-Meteo (time_utc = T), deduplicated, complete runs
-- only, latest run wins per (location_id, time_utc). See macro latest_complete_rows.
with latest as (

    {{ latest_complete_rows(source('raw', 'forecast_hourly'), 'location_id', ref('stg_complete_runs')) }}

)

select
    location_id,
    time_utc,
    run_id,
    fetched_at,
    temperature_2m,
    precipitation,
    precipitation_probability,
    weather_code,
    cape,
    wind_speed_10m,
    wind_direction_10m,
    wind_gusts_10m
from latest
