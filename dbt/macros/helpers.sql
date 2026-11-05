{# Small SQL-building helpers shared by the models. #}

{# "now" for the whole project; fixed through --vars '{"now_utc": "..."}' in tests/backfills. #}
{% macro now_utc() -%}
    {%- if var('now_utc', none) -%}
        timestamp('{{ var("now_utc") }}')
    {%- else -%}
        current_timestamp()
    {%- endif -%}
{%- endmacro %}

{# Quoted SQL string literal. #}
{% macro sql_str(value) -%}
    '{{ value | string | replace("\\", "\\\\") | replace("'", "\'") }}'
{%- endmacro %}

{# UTC timestamp -> local DATETIME for display columns (*_local). #}
{% macro to_local(ts) -%}
    datetime({{ ts }}, '{{ var("display_timezone") }}')
{%- endmacro %}

{# Clamp a numeric expression to [0, 1]. #}
{% macro clamp01(expr) -%}
    least(greatest({{ expr }}, 0), 1)
{%- endmacro %}

{# Modulo for FLOAT64 (BigQuery MOD only supports integers). #}
{% macro fmod(expr, divisor) -%}
    (({{ expr }}) - {{ divisor }} * floor(({{ expr }}) / {{ divisor }}))
{%- endmacro %}

{#
  Alert level helpers. Levels are ordered in config/alerts.yaml (`levels`, lowest first);
  a level is represented as its 1-based rank, 0 = not flagged.
#}

{# CASE expression: highest level whose threshold `expr` reaches (>=), else 0. #}
{% macro level_rank_case(expr, thresholds) -%}
    {%- set levels = var('alerts')['levels'] -%}
    case
    {%- for lvl in levels | reverse %}
        when {{ expr }} >= {{ thresholds[lvl] }} then {{ levels.index(lvl) + 1 }}
    {%- endfor %}
        else 0
    end
{%- endmacro %}

{# Score band label (config/beach_score.yaml labels_tr, highest `min` first). #}
{% macro score_label(score_expr) -%}
    {%- set bands = var('beach_score')['labels_tr'] | sort(attribute='min', reverse=true) -%}
    case
    {%- for band in bands %}
        when {{ score_expr }} >= {{ band['min'] }} then {{ sql_str(band['label']) }}
    {%- endfor %}
    end
{%- endmacro %}

{# Level rank -> level name. #}
{% macro level_name(rank_expr) -%}
    {%- set levels = var('alerts')['levels'] -%}
    case {{ rank_expr }}
    {%- for lvl in levels %}
        when {{ loop.index }} then {{ sql_str(lvl) }}
    {%- endfor %}
    end
{%- endmacro %}
