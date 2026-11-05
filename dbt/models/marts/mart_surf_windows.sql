{{ config(cluster_by=['spot_id']) }}

-- Contiguous rideable / ideal windows per spot (hours flagged in int_surf_hourly_quality).
-- Window hour T covers [T, T+1h). Windows shorter than min_window_hours are not reported.
-- Windows that have already ended are dropped; an ongoing window keeps its true start.
with flagged as (

    select
        spot_id, sport, directions_verified, 'rideable' as quality,
        time_utc, wind_speed_10m, wind_gusts_10m, wind_direction_10m
    from {{ ref('int_surf_hourly_quality') }}
    where is_rideable

    union all

    select
        spot_id, sport, directions_verified, 'ideal' as quality,
        time_utc, wind_speed_10m, wind_gusts_10m, wind_direction_10m
    from {{ ref('int_surf_hourly_quality') }}
    where is_ideal

),

islands as (

    select
        *,
        -- consecutive hours share the same (time - row number) value
        timestamp_sub(
            time_utc,
            interval row_number() over (partition by spot_id, quality order by time_utc) hour
        ) as island_id
    from flagged

),

windows as (

    select
        spot_id,
        sport,
        quality,
        directions_verified,
        min(time_utc) as window_start_utc,
        timestamp_add(max(time_utc), interval 1 hour) as window_end_utc,
        count(*) as hours,
        avg(wind_speed_10m) as avg_wind_kmh,
        max(wind_gusts_10m) as max_gust_kmh,
        -- circular mean of the FROM-directions (vector average of unit vectors)
        avg(sin(wind_direction_10m * acos(-1) / 180)) as mean_sin,
        avg(cos(wind_direction_10m * acos(-1) / 180)) as mean_cos
    from islands
    group by spot_id, sport, quality, directions_verified, island_id

)

select
    windows.spot_id,
    windows.sport,
    windows.quality,
    windows.window_start_utc,
    windows.window_end_utc,
    cast(windows.hours as int64) as hours,
    cast(windows.avg_wind_kmh as float64) as avg_wind_kmh,
    cast(windows.max_gust_kmh as float64) as max_gust_kmh,
    -- rounded to 0.1 degree (also avoids 359.99999 for a mean of 0); null when the vectors cancel out (no meaningful mean direction)
    if(
        abs(windows.mean_sin) + abs(windows.mean_cos) < 1e-9,
        cast(null as float64),
        {{ fmod('round(atan2(windows.mean_sin, windows.mean_cos) * 180 / acos(-1), 1)', 360) }}
    ) as mean_direction_deg,
    windows.directions_verified,
    latest_run.run_id as forecast_run_id,
    {{ now_utc() }} as generated_at
from windows
cross join {{ ref('int_latest_complete_run') }} as latest_run
where windows.hours >= {{ var('surf')['min_window_hours'] }}
  and windows.window_end_utc > {{ now_utc() }}
