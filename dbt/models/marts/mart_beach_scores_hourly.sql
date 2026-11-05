{{ config(
    partition_by={'field': 'time_utc', 'data_type': 'timestamp', 'granularity': 'day'},
    cluster_by=['location_id']
) }}

-- Beach shelter score per hour (formula: header of config/beach_score.yaml).
{% set bs = var('beach_score') -%}
{% set wind_term_expr = '(wind_speed_10m - %s) / (%s - %s)' | format(bs.wind.calm_kmh, bs.wind.strong_kmh, bs.wind.calm_kmh) -%}
{% set gust_term_expr = '(wind_gusts_10m - %s) / (%s - %s)' | format(bs.wind.calm_gust_kmh, bs.wind.strong_gust_kmh, bs.wind.calm_gust_kmh) -%}
{% set wave_term_expr = '(wave_height - %s) / (%s - %s)' | format(bs.waves.calm_wave_m, bs.waves.rough_wave_m, bs.waves.calm_wave_m) -%}

with beach_weather as (

    select
        location_id,
        location_name,
        region_id,
        shore_facing_deg,
        shore_verified,
        time_utc,
        time_local,
        wind_speed_10m,
        wind_gusts_10m,
        wind_direction_10m,
        wave_height
    from {{ ref('int_location_weather_hourly') }}
    where location_type = 'beach'
      -- no score without wind speed, wind direction or a known shore orientation
      and wind_speed_10m is not null
      and wind_direction_10m is not null
      and shore_facing_deg is not null

),

angles as (

    select
        *,
        -- smallest angle between wind direction (FROM) and shore_facing_deg, 0..180
        least(
            {{ fmod('abs(wind_direction_10m - shore_facing_deg)', 360) }},
            360 - {{ fmod('abs(wind_direction_10m - shore_facing_deg)', 360) }}
        ) as onshore_angle_deg
    from beach_weather

),

exposures as (

    select
        *,
        -- 1.0 onshore, offshore_exposure when the wind blows from land
        {{ bs.offshore_exposure }}
            + (1 - {{ bs.offshore_exposure }}) * (1 + cos(onshore_angle_deg * acos(-1) / 180)) / 2
            as exposure
    from angles

),

terms as (

    select
        *,
        {{ clamp01(wind_term_expr) }} as wind_term,
        -- missing gusts / waves contribute no penalty
        coalesce({{ clamp01(gust_term_expr) }}, 0) as gust_term,
        coalesce({{ clamp01(wave_term_expr) }}, 0) as wave_term
    from exposures

),

scored as (

    select
        *,
        cast(round(100 * (1 - least(greatest(
            exposure * ({{ bs.weights.w_wind }} * wind_term + {{ bs.weights.w_gust }} * gust_term)
                + {{ bs.weights.w_wave }} * wave_term,
            0), 1))) as int64) as score
    from terms

)

select
    location_id,
    location_name,
    region_id,
    time_utc,
    time_local,
    score,
    {{ score_label('score') }} as label_tr,
    cast(row_number() over (
        partition by time_utc
        order by score desc, wind_gusts_10m asc nulls last, location_id asc
    ) as int64) as rank_in_hour,
    extract(hour from time_local) >= {{ bs.daytime_local_hours.start }}
        and extract(hour from time_local) < {{ bs.daytime_local_hours['end'] }} as is_daytime,
    wind_speed_10m,
    wind_gusts_10m,
    wind_direction_10m,
    wave_height,
    onshore_angle_deg,
    exposure,
    shore_verified,
    {{ now_utc() }} as generated_at
from scored
