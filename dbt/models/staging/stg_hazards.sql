-- Hazard metadata from config/alerts.yaml (var 'alerts').
select *
from unnest([
    {%- for hazard_id, hazard in var('alerts')['hazards'].items() %}
    struct(
        {{ sql_str(hazard_id) }} as hazard,
        {{ sql_str(hazard.label_tr) }} as label_tr,
        {{ sql_str(hazard.peak_unit) }} as peak_unit,
        {{ sql_str(hazard.source) }} as source,
        {{ sql_str(hazard.primary_variable) }} as primary_variable
    ){{ "," if not loop.last }}
    {%- endfor %}
])
