-- Marine zones (open-sea grid cells) from config/locations.yaml (var 'marine_zones').
select *
from unnest([
    {%- for zone in var('marine_zones') %}
    struct(
        {{ sql_str(zone.id) }} as marine_zone_id,
        {{ sql_str(zone.name) }} as marine_zone_name,
        cast({{ zone.lat }} as float64) as latitude,
        cast({{ zone.lon }} as float64) as longitude
    ){{ "," if not loop.last }}
    {%- endfor %}
])
