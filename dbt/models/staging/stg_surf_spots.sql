-- Surf / kite spots from config/surf.yaml (var 'surf'); no seed file.
-- good_directions are inclusive FROM-ranges in degrees; a range may wrap through 0
-- (from_deg > to_deg).
select *
from unnest([
    {%- for spot_id, spot in var('surf')['spots'].items() %}
    struct(
        {{ sql_str(spot_id) }} as spot_id,
        {{ sql_str(spot.sport) }} as sport,
        cast({{ spot.wind_kmh.rideable.min }} as float64) as rideable_min_kmh,
        cast({{ spot.wind_kmh.rideable.max }} as float64) as rideable_max_kmh,
        cast({{ spot.wind_kmh.ideal.min }} as float64) as ideal_min_kmh,
        cast({{ spot.wind_kmh.ideal.max }} as float64) as ideal_max_kmh,
        cast({{ spot.max_gust_factor }} as float64) as max_gust_factor,
        array<struct<from_deg float64, to_deg float64>>[
            {%- for d in spot.good_directions %}
            struct<from_deg float64, to_deg float64>({{ d.from_deg }}, {{ d.to_deg }}){{ "," if not loop.last }}
            {%- endfor %}
        ] as good_directions,
        {{ spot.directions_verified | lower }} as directions_verified
    ){{ "," if not loop.last }}
    {%- endfor %}
])
