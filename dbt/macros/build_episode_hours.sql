{#
  Gaps-and-islands over flagged hours.
  Input relation: (hazard, interval_start_utc), hour-aligned, one row per flagged hour.
  Output: (hazard, interval_start_utc, episode_start_utc, episode_end_utc) for the hours of
  episodes that are at least episode.min_duration_hours long. Returned as a parenthesised
  subquery so it can be used as a CTE body.
#}
{% macro build_episode_hours(flagged_hours) -%}
(
    with spaced as (

        select
            hazard,
            interval_start_utc,
            timestamp_diff(
                interval_start_utc,
                lag(interval_start_utc) over (partition by hazard order by interval_start_utc),
                hour
            ) as hours_since_previous
        from {{ flagged_hours }}

    ),

    numbered as (

        select
            hazard,
            interval_start_utc,
            -- a new episode starts when the gap (hours_since_previous - 1) exceeds merge_gap_hours
            sum(
                case
                    when hours_since_previous is null
                        or hours_since_previous - 1 > {{ var('alerts')['episode']['merge_gap_hours'] }}
                    then 1 else 0
                end
            ) over (partition by hazard order by interval_start_utc) as episode_no
        from spaced

    ),

    episodes as (

        select
            hazard,
            interval_start_utc,
            min(interval_start_utc) over (partition by hazard, episode_no) as episode_start_utc,
            timestamp_add(
                max(interval_start_utc) over (partition by hazard, episode_no),
                interval 1 hour
            ) as episode_end_utc
        from numbered

    )

    select
        hazard,
        interval_start_utc,
        episode_start_utc,
        episode_end_utc
    from episodes
    where timestamp_diff(episode_end_utc, episode_start_utc, hour)
        >= {{ var('alerts')['episode']['min_duration_hours'] }}
)
{%- endmacro %}
