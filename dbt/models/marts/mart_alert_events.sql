{{ config(cluster_by=['hazard']) }}

-- Alert episodes of the latest complete run (steps 1, 2 and 5 of config/alerts.yaml).
-- Lifecycle against ops.alert_state (steps 3-4) is the Phase 2 notifier's job.
with episode_hours as (

    select * from {{ ref('int_alert_episode_hours') }}

),

-- flagged (point, hour) rows that belong to each episode
episode_points as (

    select
        episode_hours.hazard,
        episode_hours.episode_start_utc,
        episode_hours.episode_end_utc,
        levels.point_id,
        levels.point_kind,
        levels.interval_start_utc,
        levels.level_rank,
        levels.primary_value
    from episode_hours
    inner join {{ ref('int_hazard_hourly_levels') }} as levels
        on levels.hazard = episode_hours.hazard
        and levels.interval_start_utc = episode_hours.interval_start_utc

),

-- Point -> region. A marine zone belongs to every region that has a location using it.
point_regions as (

    select distinct 'location' as point_kind, location_id as point_id, region_id
    from {{ ref('stg_locations') }}
    union distinct
    select distinct 'marine_zone', marine_zone_id, region_id
    from {{ ref('stg_locations') }}

),

episode_core as (

    select
        hazard,
        episode_start_utc,
        episode_end_utc,
        max(level_rank) as level_rank,
        array_agg(distinct point_id order by point_id) as affected_locations,
        -- peak: highest primary value; ties -> earliest hour, then point id
        array_agg(
            struct(primary_value, interval_start_utc, point_id)
            order by primary_value desc, interval_start_utc asc, point_id asc
            limit 1
        )[offset(0)] as peak
    from episode_points
    group by hazard, episode_start_utc, episode_end_utc

),

episode_hours_count as (

    select
        hazard,
        episode_start_utc,
        count(*) as hours_flagged
    from episode_hours
    group by hazard, episode_start_utc

),

region_levels as (

    select
        episode_points.hazard,
        episode_points.episode_start_utc,
        point_regions.region_id,
        max(episode_points.level_rank) as level_rank
    from episode_points
    inner join point_regions
        on point_regions.point_kind = episode_points.point_kind
        and point_regions.point_id = episode_points.point_id
    group by 1, 2, 3

),

region_summary as (

    select
        hazard,
        episode_start_utc,
        array_agg(region_id order by region_id) as affected_regions,
        array_agg(
            struct(region_id, {{ level_name('level_rank') }} as level)
            order by region_id
        ) as region_levels
    from region_levels
    group by hazard, episode_start_utc

)

select
    concat(core.hazard, '|', format_timestamp('%Y-%m-%dT%H:%M:%SZ', core.episode_start_utc)) as episode_key,
    core.hazard,
    {{ level_name('core.level_rank') }} as level,
    core.episode_start_utc as start_utc,
    core.episode_end_utc as end_utc,
    cast(core.peak.primary_value as float64) as peak_value,
    hazards.peak_unit,
    core.peak.interval_start_utc as peak_time_utc,
    regions.affected_regions,
    regions.region_levels,
    core.affected_locations,
    cast(counts.hours_flagged as int64) as hours_flagged,
    core.peak.point_id as peak_location_id,
    latest_run.run_id as forecast_run_id,
    {{ now_utc() }} as generated_at
from episode_core as core
inner join episode_hours_count as counts
    on counts.hazard = core.hazard and counts.episode_start_utc = core.episode_start_utc
inner join region_summary as regions
    on regions.hazard = core.hazard and regions.episode_start_utc = core.episode_start_utc
inner join {{ ref('stg_hazards') }} as hazards
    on hazards.hazard = core.hazard
cross join {{ ref('int_latest_complete_run') }} as latest_run
