-- Step 1 of the algorithm in config/alerts.yaml:
-- hourly level per (point, hour interval, hazard) = the MAX level reached by any variable
-- of the hazard (thresholds are inclusive, >=). Only flagged rows (level_rank > 0) are kept.
--   level_rank   1-based index into alerts.levels (1 = yellow ... 3 = red)
--   primary_value value of the hazard's primary_variable, used to find the peak
-- Forecast hazards use the forecast points (point_kind = 'location'); marine hazards use
-- the marine zones (point_kind = 'marine_zone').
{% set hazards = var('alerts')['hazards'] -%}
{% set levels = var('alerts')['levels'] -%}

with all_rows as (

    {%- for hazard_id, hazard in hazards.items() %}
    {% if hazard.source == 'forecast' %}
    {%- set point_col = 'location_id' %}{% set point_kind = 'location' %}{% set relation = ref('int_forecast_derived') %}
    {%- else %}
    {%- set point_col = 'marine_zone_id' %}{% set point_kind = 'marine_zone' %}{% set relation = ref('stg_marine_hourly') %}
    {%- endif %}
    select
        {{ sql_str(hazard_id) }} as hazard,
        {{ point_col }} as point_id,
        {{ sql_str(point_kind) }} as point_kind,
        interval_start_utc,
        greatest(
            {%- if hazard.trigger_weather_codes is defined %}
            {#- Thunderstorm: a trigger weather code gives at least base_level; CAPE /
                precipitation can escalate it. Without a trigger code the hour is not flagged. #}
            case
                when weather_code in ({{ hazard.trigger_weather_codes | join(', ') }}) then
                    greatest(
                        {{ levels.index(hazard.base_level) + 1 }}
                        {%- for lvl, conditions in hazard.escalate.items() %},
                        case
                            when
                                {%- for col, limit in conditions.items() %}
                                coalesce({{ col }} >= {{ limit }}, false){{ " or" if not loop.last }}
                                {%- endfor %}
                            then {{ levels.index(lvl) + 1 }}
                            else 0
                        end
                        {%- endfor %}
                    )
                else 0
            end
            {%- else %}
            {#- Threshold hazards: max over variables; a null variable counts as not flagged. #}
            {%- for variable, thresholds in hazard.thresholds.items() %}
            coalesce({{ level_rank_case(variable, thresholds) }}, 0){{ "," if not loop.last }}
            {%- endfor %}
            {%- endif %}
        ) as level_rank,
        coalesce({{ hazard.primary_variable }}, 0) as primary_value
    from {{ relation }}
    {{ "union all" if not loop.last }}
    {%- endfor %}

)

select
    hazard,
    point_id,
    point_kind,
    interval_start_utc,
    level_rank,
    primary_value
from all_rows
where level_rank > 0
