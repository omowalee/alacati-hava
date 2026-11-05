-- Hourly rideable / ideal flags per surf spot (thresholds: config/surf.yaml).
-- An hour counts when ALL hold:
--   * local hour inside daylight_local_hours (start inclusive, end exclusive)
--   * wind speed inside the quality's [min, max] km/h range (inclusive)
--   * wind direction inside one of good_directions (inclusive, may wrap through 0)
--   * gust / mean wind <= max_gust_factor
-- Missing speed, gust or direction means the hour is not flagged.
-- 'ideal' ranges lie inside 'rideable' ranges, so every ideal hour is also rideable.
{% set daylight = var('surf')['daylight_local_hours'] -%}

with hourly as (

    select
        spots.spot_id,
        spots.sport,
        spots.directions_verified,
        spots.rideable_min_kmh,
        spots.rideable_max_kmh,
        spots.ideal_min_kmh,
        spots.ideal_max_kmh,
        spots.max_gust_factor,
        spots.good_directions,
        weather.time_utc,
        weather.time_local,
        weather.wind_speed_10m,
        weather.wind_gusts_10m,
        -- normalise 360 -> 0 so range checks see [0, 360)
        {{ fmod('weather.wind_direction_10m', 360) }} as wind_direction_10m
    from {{ ref('stg_surf_spots') }} as spots
    inner join {{ ref('int_location_weather_hourly') }} as weather
        on weather.location_id = spots.spot_id

),

checks as (

    select
        *,
        extract(hour from time_local) >= {{ daylight['start'] }}
            and extract(hour from time_local) < {{ daylight['end'] }} as is_daylight,
        coalesce(wind_gusts_10m <= wind_speed_10m * max_gust_factor, false) as gust_ok,
        wind_direction_10m is not null
            and (
                array_length(good_directions) = 0
                or exists (
                    select 1
                    from unnest(good_directions) as good
                    where if(
                        good.from_deg <= good.to_deg,
                        wind_direction_10m between good.from_deg and good.to_deg,
                        -- range wraps through north, e.g. 330 -> 30
                        wind_direction_10m >= good.from_deg or wind_direction_10m <= good.to_deg
                    )
                )
            ) as direction_ok
    from hourly

)

select
    spot_id,
    sport,
    directions_verified,
    time_utc,
    wind_speed_10m,
    wind_gusts_10m,
    wind_direction_10m,
    is_daylight,
    direction_ok,
    gust_ok,
    is_daylight and direction_ok and gust_ok
        and coalesce(wind_speed_10m between rideable_min_kmh and rideable_max_kmh, false) as is_rideable,
    is_daylight and direction_ok and gust_ok
        and coalesce(wind_speed_10m between ideal_min_kmh and ideal_max_kmh, false) as is_ideal
from checks
