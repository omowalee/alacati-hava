{{ config(cluster_by=['hazard']) }}

-- Alert events from ops.alert_state: active ones, plus ended ones for 24 hours.
select
    state.event_id,
    state.hazard,
    hazards.label_tr as hazard_label_tr,
    state.level,
    state.status,
    state.start_utc,
    state.end_utc,
    {{ to_local('state.start_utc') }} as start_local,
    {{ to_local('state.end_utc') }} as end_local,
    state.peak_value,
    state.peak_unit,
    {{ to_local('state.peak_time_utc') }} as peak_time_local,
    state.affected_regions,
    state.region_levels,
    state.revision,
    state.last_updated_at
from {{ ref('stg_alert_state') }} as state
inner join {{ ref('stg_hazards') }} as hazards
    on hazards.hazard = state.hazard
where state.status = 'active'
   or (state.status = 'ended'
       and state.ended_at >= timestamp_sub({{ now_utc() }}, interval 24 hour))
