-- Marine rows: all variables are instantaneous at T -> interval [T, T+1h).
-- Deduplicated, complete runs only, latest run wins per (marine_zone_id, time_utc).
with latest as (

    {{ latest_complete_rows(source('raw', 'marine_hourly'), 'marine_zone_id', ref('stg_complete_runs')) }}

)

select
    marine_zone_id,
    time_utc,
    time_utc as interval_start_utc,
    timestamp_add(time_utc, interval 1 hour) as interval_end_utc,
    run_id,
    fetched_at,
    wave_height,
    wave_direction,
    wave_period,
    wind_wave_height,
    swell_wave_height
from latest
