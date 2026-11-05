{{ config(
    partition_by={'field': 'date_local', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['location_id']
) }}

-- One row per beach and local date over the daytime hours (beach_score.yaml: daytime_local_hours).
with daytime as (

    select
        *,
        date(time_local) as date_local
    from {{ ref('mart_beach_scores_hourly') }}
    where is_daytime

),

daily as (

    select
        date_local,
        location_id,
        any_value(location_name) as location_name,
        any_value(region_id) as region_id,
        avg(score) as avg_score,
        min(score) as min_score,
        -- calmest hour: best score, then lower gusts, then earliest
        array_agg(time_local order by score desc, coalesce(wind_gusts_10m, 1e9) asc, time_local asc limit 1)[offset(0)]
            as best_hour_local,
        logical_and(shore_verified) as shore_verified
    from daytime
    group by date_local, location_id

)

select
    date_local,
    location_id,
    location_name,
    region_id,
    cast(avg_score as float64) as avg_score,
    cast(min_score as int64) as min_score,
    best_hour_local,
    cast(row_number() over (
        partition by date_local
        order by avg_score desc, min_score desc, location_id asc
    ) as int64) as rank_in_day,
    {{ score_label('avg_score') }} as label_tr,
    shore_verified,
    {{ now_utc() }} as generated_at
from daily
