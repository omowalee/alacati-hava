from datetime import datetime

import regions as r

LABELS = [
    {"min": 75, "label": "Sakin"},
    {"min": 50, "label": "Hafif rüzgârlı"},
    {"min": 25, "label": "Rüzgârlı"},
    {"min": 0, "label": "Uygun değil"},
]


def test_hull_drops_interior_points():
    pts = [(0, 0), (2, 0), (2, 2), (0, 2), (1, 1), (1, 0.5)]
    assert set(r.convex_hull(pts)) == {(0, 0), (2, 0), (2, 2), (0, 2)}


def test_hull_small_inputs():
    assert r.convex_hull([(1, 1)]) == [(1, 1)]
    assert r.convex_hull([(1, 1), (1, 1)]) == [(1, 1)]


def test_single_point_region_is_circle():
    assert len(r.region_polygon([(26.3, 38.3)])) == r.CIRCLE_VERTICES


def test_multi_point_region_covers_all_centres():
    coords = [(26.30, 38.30), (26.35, 38.32), (26.33, 38.28)]
    poly = r.region_polygon(coords)
    lons = [p[0] for p in poly]
    assert min(lons) < 26.30 and max(lons) > 26.35


def test_precip_bins_edges():
    assert [r.precip_bin(x) for x in (0, 0.09, 0.1, 1.99, 2, 10, 24.9, 25)] == [
        0,
        0,
        1,
        1,
        2,
        3,
        3,
        4,
    ]
    assert r.precip_bin(None) is None
    assert r.precip_bin(float("nan")) is None


def test_score_band():
    assert r.score_band(75, LABELS) == 0
    assert r.score_band(74, LABELS) == 1
    assert r.score_band(0, LABELS) == 3
    assert r.score_band(None, LABELS) is None


def test_plaj_swatches_darker_is_calmer():
    sw = r.plaj_swatches(LABELS)
    assert sw[0].label == "Sakin" and sw[0].color == r.PLAJ_COLORS[0]
    assert sw[-1].range_text == "0–24"


def test_aggregate_plaj_mean():
    regs = [{"id": "a", "name": "A"}]
    out = r.aggregate_regions("Plaj", regs, {"a": ["x", "y"]}, {"x": 80, "y": 50}, LABELS)
    assert out[0].value == 65 and out[0].label == "Hafif rüzgârlı"


def test_aggregate_precip_max():
    regs = [{"id": "a", "name": "A"}]
    out = r.aggregate_regions("Yağış", regs, {"a": ["x", "y"]}, {"x": 0.0, "y": 12.0}, LABELS)
    assert out[0].label == "Kuvvetli" and out[0].color == r.PRECIP_COLORS[3]


def test_aggregate_surf_best_and_no_spot():
    regs = [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]
    out = r.aggregate_regions("Sörf", regs, {"a": ["s1", "s2"]}, {"s1": 1, "s2": 2}, LABELS)
    assert out[0].label == "İdeal" and out[0].color == r.SURF_IDEAL_COLOR
    assert out[1].label == "Sörf noktası yok" and out[1].color == r.NO_DATA_COLOR


def test_aggregate_all_none_is_no_data():
    regs = [{"id": "a", "name": "A"}]
    out = r.aggregate_regions("Yağış", regs, {"a": ["x"]}, {"x": None}, LABELS)
    assert out[0].label == "Veri yok"


def test_best_surf_rank_overlap_and_best_quality():
    d0, d1 = datetime(2026, 11, 5), datetime(2026, 11, 6)
    wins = [
        ("s", "rideable", datetime(2026, 11, 5, 10), datetime(2026, 11, 5, 13)),
        ("s", "ideal", datetime(2026, 11, 5, 14), datetime(2026, 11, 5, 16)),
        ("t", "ideal", datetime(2026, 11, 6, 10), datetime(2026, 11, 6, 12)),
    ]
    assert r.best_surf_rank(wins, d0, d1) == {"s": 2}


def test_hex_to_rgba():
    assert r.hex_to_rgba("#ff8000", 0.5) == [255, 128, 0, 128]


def test_view_bounds_centre():
    assert r.view_bounds([(26.0, 38.0), (27.0, 39.0)]) == (38.5, 26.5)


def _bearing_of(a, b):
    import math

    dx = (b[0] - a[0]) * math.cos(math.radians(a[1]))
    dy = b[1] - a[1]
    return math.degrees(math.atan2(dx, dy)) % 360


def test_arrow_from_north_points_south():
    shaft = r.arrow_paths(26.3, 38.3, 0, 2.0)[0]
    assert abs(_bearing_of(*shaft) - 180) < 0.5


def test_arrow_from_east_points_west():
    shaft = r.arrow_paths(26.3, 38.3, 90, 2.0)[0]
    assert abs(_bearing_of(*shaft) - 270) < 0.5


def test_arrow_has_shaft_and_two_head_strokes_ending_at_tip():
    paths = r.arrow_paths(26.3, 38.3, 45, 2.0)
    assert len(paths) == 3
    assert paths[1][1] == paths[0][1] == paths[2][1]


def test_arrow_length_clamped_and_monotonic():
    assert r.arrow_length_km(0) == r.ARROW_MIN_KM
    assert r.arrow_length_km(500) == r.ARROW_MAX_KM
    assert r.arrow_length_km(10) < r.arrow_length_km(30)
    assert r.arrow_length_km(None) == r.ARROW_MIN_KM


def test_sector_wraps_through_north():
    poly = r.sector_polygon(26.3, 38.3, 315, 45)
    assert poly[0] == (26.3, 38.3)
    bearings = [_bearing_of(poly[0], p) for p in poly[1:]]
    assert abs(bearings[0] - 315) < 0.5
    assert abs(bearings[-1] - 45) < 0.5
    assert any(b < 1 or b > 359 for b in bearings) or any(abs(b) < 3 for b in bearings)
    assert all(p[1] > 38.3 - 1e-9 or abs(p[1] - 38.3) < 1e-6 for p in poly)  # northern half


def test_sector_south_is_south_of_spot():
    poly = r.sector_polygon(26.3, 38.3, 160, 225)
    assert all(p[1] <= 38.3 for p in poly)


def test_compass16():
    assert r.compass16(0) == "K"
    assert r.compass16(45) == "KD"
    assert r.compass16(22.5) == "KKD"
    assert r.compass16(359) == "K"
    assert r.compass16(None) == "–"


def test_default_hour():
    assert r.default_hour(True, 14, 8, 20) == 14
    assert r.default_hour(True, 3, 8, 20) == 8
    assert r.default_hour(True, 22, 8, 20) == 19
    assert r.default_hour(False, 9, 8, 20) == 14
    assert r.default_hour(False, 9, 8, 12) == 11
