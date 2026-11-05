from datetime import date, datetime

import formatting as f
from settings import load_config


def test_to_local_adds_three_hours():
    assert f.to_local(datetime(2026, 7, 1, 21, 30)) == datetime(2026, 7, 2, 0, 30)


def test_kmh_to_knots():
    assert round(f.kmh_to_knots(18.52), 2) == 10.0


def test_fmt_day_time_labels():
    today = date(2026, 11, 5)
    assert f.fmt_day_time(datetime(2026, 11, 5, 14), today) == "Bugün 14:00"
    assert f.fmt_day_time(datetime(2026, 11, 6, 9), today) == "Yarın 09:00"
    assert f.fmt_day_time(datetime(2026, 11, 8, 9), today) == "Paz 08.11. 09:00"


def test_fmt_range_same_and_different_day():
    today = date(2026, 11, 5)
    assert f.fmt_range(datetime(2026, 11, 5, 14), datetime(2026, 11, 5, 17), today) == (
        "Bugün 14:00 – 17:00"
    )
    assert f.fmt_range(datetime(2026, 11, 5, 22), datetime(2026, 11, 6, 2), today) == (
        "Bugün 22:00 – Yarın 02:00"
    )


def test_fmt_number():
    assert f.fmt_number(3.25, "m") == "3,2 m"
    assert f.fmt_number(64.6, "km/h") == "65 km/h"


def test_compass_and_arrow():
    assert f.compass(0) == "K (0°)"
    assert f.compass(350) == "K (350°)"
    assert f.compass(45) == "KD (45°)"
    assert f.compass(None) == "–"
    assert f.wind_arrow(0) == "↓"  # northerly wind blows towards the south
    assert f.wind_arrow(270) == "→"
    assert f.wind_arrow(float("nan")) == ""


def test_score_color_endpoints():
    assert f.score_color(0)[0] > f.score_color(0)[1] - 100 and f.score_color(0)[1] < 100
    assert f.score_color(100)[1] > f.score_color(100)[0]
    assert f.score_color(None) == [150, 150, 150]
    assert f.score_color(500) == f.score_color(100)


def test_level_badge_and_regions():
    assert "Turuncu" in f.level_badge_html("orange")
    assert f.region_list(["a", "x"], {"a": "Alaçatı"}) == "Alaçatı, x"


def test_config_loads_from_repo():
    cfg = load_config()
    assert cfg.horizon_hours == 48
    assert "strong_wind" in cfg.hazard_labels
    assert cfg.disclaimer and "MGM" not in cfg.attribution
    assert "alacati_surf" in cfg.locations_by_id
