-- Locations from config/locations.yaml (passed in as var 'locations'); no seed file.
select *
from unnest([
    {%- for loc in var('locations') %}
    struct(
        {{ sql_str(loc.id) }} as location_id,
        {{ sql_str(loc.name) }} as location_name,
        {{ sql_str(loc.type) }} as location_type,
        {{ sql_str(loc.region) }} as region_id,
        {{ sql_str(loc.marine_zone) }} as marine_zone_id,
        cast({{ loc.lat }} as float64) as latitude,
        cast({{ loc.lon }} as float64) as longitude,
        cast({{ loc.shore_facing_deg }} as float64) as shore_facing_deg,
        {{ loc.shore_verified | lower }} as shore_verified,
        {{ loc.coords_verified | lower }} as coords_verified
    ){{ "," if not loop.last }}
    {%- endfor %}
])
