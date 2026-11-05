-- Alert events written by the Phase 2 notifier (empty until then).
select
    event_id,
    hazard,
    status,
    level,
    start_utc,
    end_utc,
    peak_value,
    peak_unit,
    peak_time_utc,
    affected_regions,
    region_levels,
    revision,
    last_updated_at,
    ended_at
from {{ source('ops', 'alert_state') }}
