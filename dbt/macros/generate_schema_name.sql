{#
  Models land in exactly two datasets, regardless of the target schema:
    +schema: marts   -> BQ_DATASET_MARTS    (default alacati_marts)
    anything else    -> BQ_DATASET_STAGING  (default alacati_staging)
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name | trim == 'marts' -%}
        {{ env_var('BQ_DATASET_MARTS', 'alacati_marts') }}
    {%- else -%}
        {{ env_var('BQ_DATASET_STAGING', 'alacati_staging') }}
    {%- endif -%}
{%- endmacro %}
