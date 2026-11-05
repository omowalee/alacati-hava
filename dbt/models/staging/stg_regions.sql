-- Alert regions from config/locations.yaml (var 'regions').
select *
from unnest([
    {%- for region in var('regions') %}
    struct({{ sql_str(region.id) }} as region_id, {{ sql_str(region.name) }} as region_name){{ "," if not loop.last }}
    {%- endfor %}
])
