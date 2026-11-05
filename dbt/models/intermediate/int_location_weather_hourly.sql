-- Latest complete forecast per location and hour (past N hours + the whole forecast),
-- with the marine values of the location's marine zone and location attributes.
-- Timestamps keep the Open-Meteo labelling (time_utc = T; precipitation = preceding hour).
with forecast as (

    select
        *,
        -- "past" = valid before the newest forecast fetch
        max(fetched_at) over () as latest_fetched_at
    from {{ ref('stg_forecast_hourly') }}
    where time_utc >= timestamp_sub({{ now_utc() }}, interval {{ var('mart_past_hours') }} hour)

)

select
    forecast.location_id,
    locations.location_name,
    locations.location_type,
    locations.region_id,
    locations.marine_zone_id,
    locations.shore_facing_deg,
    locations.shore_verified,
    forecast.time_utc,
    {{ to_local('forecast.time_utc') }} as time_local,
    forecast.temperature_2m,
    forecast.precipitation,
    forecast.precipitation_probability,
    forecast.weather_code,
    forecast.cape,
    forecast.wind_speed_10m,
    forecast.wind_direction_10m,
    forecast.wind_gusts_10m,
    marine.wave_height,
    marine.wave_period,
    marine.wave_direction,
    forecast.run_id as forecast_run_id,
    forecast.fetched_at as forecast_fetched_at,
    marine.fetched_at as marine_fetched_at,
    forecast.time_utc < forecast.latest_fetched_at as is_past
from forecast
inner join {{ ref('stg_locations') }} as locations
    on locations.location_id = forecast.location_id
left join {{ ref('stg_marine_hourly') }} as marine
    on marine.marine_zone_id = locations.marine_zone_id
    and marine.time_utc = forecast.time_utc
