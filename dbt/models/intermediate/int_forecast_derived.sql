-- Derived variables from the `derived` block of config/alerts.yaml, computed on the full
-- staged hour-interval series (including past hours) so the first hours of every window
-- are correct. Currently: trailing sums, e.g. precipitation_6h_sum.
--   value at interval [H, H+1h) = aggregate of the N intervals ending at H+1h.
-- A RANGE frame over epoch seconds keeps the window correct even if an hour is missing.
select
    forecast.*
    {%- for name, spec in var('alerts')['derived'].items() %}
        {%- if spec.agg != 'sum' or spec.alignment != 'trailing' %}
            {{ exceptions.raise_compiler_error("Only agg: sum with alignment: trailing is implemented for derived." ~ name) }}
        {%- endif %}
    ,
    sum({{ spec.source_variable }}) over (
        partition by location_id
        order by unix_seconds(interval_start_utc)
        range between {{ (spec.window_hours - 1) * 3600 }} preceding and current row
    ) as {{ name }}
    {%- endfor %}
from {{ ref('stg_forecast_intervals') }} as forecast
