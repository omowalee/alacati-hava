-- Step 2 of the algorithm in config/alerts.yaml: peninsula-wide episodes per hazard.
-- Output: one row per flagged hour that belongs to a kept episode, with the episode window.
--   a. flagged hours of all points, restricted to hour intervals overlapping
--      [now, now + horizon_hours); an interval that already started but has not ended yet
--      (the current hour) counts.
--   b. flagged hours separated by a gap <= merge_gap_hours are merged into one episode
--      (gap = unflagged hours between them, e.g. hours 0 and 2 -> gap 1 h).
--   c. episodes shorter than min_duration_hours are dropped AFTER merging
--      (duration = end - start, merged gap hours included).
-- Suppression (step 5): hours of a suppressed hazard (e.g. heavy_rain) that lie inside an
-- episode of its suppressor (thunderstorm) are removed BEFORE the suppressed hazard builds
-- its own episodes, so one storm = one event.
{% set alerts = var('alerts') -%}
{% set pairs = [] -%}
{% for suppressor_id, hazard in alerts['hazards'].items() -%}
    {% for suppressed_id in hazard.get('suppresses', []) -%}
        {% do pairs.append([suppressor_id, suppressed_id]) -%}
    {% endfor -%}
{% endfor -%}
{% set suppressed_ids = pairs | map(attribute=1) | list -%}

with flagged_hours as (

    select distinct
        hazard,
        interval_start_utc
    from {{ ref('int_hazard_hourly_levels') }}
    where timestamp_add(interval_start_utc, interval 1 hour) > {{ now_utc() }}
      and interval_start_utc < timestamp_add({{ now_utc() }}, interval {{ alerts['horizon_hours'] }} hour)

),

suppression_pairs as (

    select *
    from unnest(array<struct<suppressor string, suppressed string>>[
        {%- for suppressor, suppressed in pairs %}
        ({{ sql_str(suppressor) }}, {{ sql_str(suppressed) }}){{ "," if not loop.last }}
        {%- endfor %}
    ])

),

-- pass 1: hazards that nobody suppresses
independent_hours as (

    select *
    from flagged_hours
    where {% if suppressed_ids %}hazard not in ({% for h in suppressed_ids %}{{ sql_str(h) }}{{ ", " if not loop.last }}{% endfor %}){% else %}true{% endif %}

),

independent_episode_hours as {{ build_episode_hours('independent_hours') }},

-- pass 2: suppressed hazards, minus the hours covered by a suppressor episode
remaining_hours as (

    select *
    from flagged_hours
    where {% if suppressed_ids %}hazard in ({% for h in suppressed_ids %}{{ sql_str(h) }}{{ ", " if not loop.last }}{% endfor %}){% else %}false{% endif %}
      and not exists (
        select 1
        from suppression_pairs
        inner join independent_episode_hours as suppressing
            on suppressing.hazard = suppression_pairs.suppressor
        where suppression_pairs.suppressed = flagged_hours.hazard
          and flagged_hours.interval_start_utc >= suppressing.episode_start_utc
          and flagged_hours.interval_start_utc < suppressing.episode_end_utc
    )

),

suppressed_episode_hours as {{ build_episode_hours('remaining_hours') }}

select * from independent_episode_hours
union all
select * from suppressed_episode_hours
