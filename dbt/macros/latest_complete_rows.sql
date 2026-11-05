{#
  Shared staging logic for the raw hourly tables (forecast / marine):
    1. scan only recent partitions (fetched_at >= now - staging_lookback_days)
    2. drop at-least-once duplicates: one row per (message_id, time_utc)
    3. keep rows of runs with ops.ingest_runs.status = 'complete' only
    4. per (point, time_utc) keep the row of the most recent complete run
  `raw` is the raw relation, `point_col` the point id column, `runs` the complete runs.
#}
{% macro latest_complete_rows(raw, point_col, runs) -%}
with recent as (

    select *
    from {{ raw }}
    where fetched_at >= timestamp_sub({{ now_utc() }}, interval {{ var('staging_lookback_days') }} day)

),

deduplicated as (

    select *
    from recent
    qualify row_number() over (
        partition by message_id, time_utc
        order by ingested_at desc
    ) = 1

),

complete_runs_only as (

    select
        deduplicated.*,
        runs.started_at as run_started_at
    from deduplicated
    inner join {{ runs }} as runs using (run_id)

)

select * except (run_started_at)
from complete_runs_only
qualify row_number() over (
    partition by {{ point_col }}, time_utc
    order by run_started_at desc, fetched_at desc, message_id desc
) = 1
{%- endmacro %}
