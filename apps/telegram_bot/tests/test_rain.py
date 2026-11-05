"""/yagis: bins, aggregation, day split, wet ranges, thunder, texts (no network)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from test_bot import CFG, FRESH, NOW, FakeStore

from alacati_bot import bot
from alacati_bot.messages import Row, help_text
from alacati_bot.rain import fmt_mm, format_rain, pick_bin, rain_window

BINS = CFG.params.rain.daily_bins
NOW_R = datetime(2026, 11, 5, 10, 30)  # local; today = Thu 05.11., tomorrow = 06.11.


def _row(
    start: datetime,
    mm: float,
    region: str = "ilica",
    loc: str | None = None,
    prob: float | None = 50.0,
    code: int | None = 61,
) -> Row:
    """Row for the interval starting at `start` (local) -> mart time T = start + 1h."""
    t_utc = (start + timedelta(hours=1) - timedelta(hours=3)).replace(tzinfo=UTC)
    return {
        "region_id": region,
        "location_id": loc or f"{region}_1",
        "time_utc": t_utc,
        "precipitation": mm,
        "precipitation_probability": prob,
        "weather_code": code,
    }


def _d(day: int, hour: int) -> datetime:
    return datetime(2026, 11, day, hour)


def _rain(rows: list[Row]) -> str:
    return format_rain(rows, CFG, NOW_R)


@pytest.mark.parametrize(
    ("total", "label"),
    [
        (0.0, "Kuru"),
        (0.0999, "Kuru"),
        (0.1, "Hafif"),
        (1.999, "Hafif"),
        (2.0, "Orta"),
        (9.99, "Orta"),
        (10.0, "Kuvvetli"),
        (24.99, "Kuvvetli"),
        (25.0, "Çok kuvvetli"),
        (400.0, "Çok kuvvetli"),
    ],
)
def test_bins_at_edges(total: float, label: str) -> None:
    assert pick_bin(total, BINS).label == label


def test_decimal_comma_and_line_format() -> None:
    rows = [_row(_d(5, 14), 0.6, prob=40), _row(_d(5, 15), 0.8, prob=60), _row(_d(5, 16), 0.0)]
    out = _rain(rows)
    assert "🌦️ Ilıca · Hafif · 1,4 mm (en yoğun 15:00, 0,8 mm/sa) · olasılık %60" in out
    assert fmt_mm(3.25) in ("3,2", "3,3")
    assert "." not in out.split("Yağışlı saatler")[0].split("\n", 1)[1].replace("05.11", "")


def test_region_total_is_max_over_locations_not_sum() -> None:
    rows = [
        _row(_d(5, 14), 1.0, region="ilica", loc="a"),
        _row(_d(5, 14), 1.5, region="ilica", loc="b"),
        _row(_d(5, 15), 1.0, region="ilica", loc="a"),
    ]
    out = _rain(rows)
    assert "2,0 mm" in out  # loc a: 2.0, loc b: 1.5 -> max 2.0 (a sum would give 3.5)
    assert "Orta" in out  # 2.0 mm falls in the second bin edge


def test_today_remaining_vs_tomorrow() -> None:
    rows = [
        _row(_d(5, 9), 5.0),  # earlier today: excluded
        _row(_d(5, 10), 0.5),  # current hour: included
        _row(_d(5, 23), 0.5, region="alacati"),
        _row(_d(6, 0), 3.0, region="dalyan"),  # tomorrow
    ]
    out = _rain(rows)
    today_part, tomorrow_part = out.split("<b>Yarın</b>")
    assert "Ilıca · Hafif · 0,5 mm" in today_part
    assert "Alaçatı · Hafif · 0,5 mm" in today_part
    assert "5,0" not in today_part
    assert "Dalyan · Orta · 3,0 mm" in tomorrow_part
    assert "Dalyan" not in today_part


def test_regions_sorted_by_total_desc() -> None:
    rows = [_row(_d(6, 10), 0.5, region="alacati"), _row(_d(6, 11), 3.0, region="ildir")]
    tomorrow = _rain(rows).split("<b>Yarın</b>")[1].split("<b>Yağışlı")[0]
    assert tomorrow.index("Ildır") < tomorrow.index("Alaçatı")


def test_all_dry_texts() -> None:
    out = _rain([_row(_d(5, 14), 0.05), _row(_d(6, 14), 0.0)])
    assert out.count("☀️ Yağış beklenmiyor") == 2
    assert "Önümüzdeki 24 saatte yağış yok ☀️" in out
    assert "<b>🌧️ Yağış — bugün ve yarın</b>" in out
    assert _rain([]).count("☀️ Yağış beklenmiyor") == 2


def test_wet_ranges_merge_and_gaps() -> None:
    rows = [
        _row(_d(5, 20), 0.2, region="alacati"),
        _row(_d(5, 21), 0.3, region="alacati"),
        _row(_d(5, 23), 0.1, region="alacati"),  # gap at 22:00 -> separate range
        _row(_d(6, 9), 0.05),  # below wet_hour_mm
        _row(_d(6, 5), 1.0, region="ilica"),
        _row(_d(6, 6), 3.2, region="alacati", loc="x"),
        _row(_d(6, 6), 2.0, region="ilica"),
        _row(_d(6, 7), 1.0, region="ilica"),
        _row(_d(6, 10), 1.0, region="ilica"),  # beyond the 24 h span (starts < 06.11. 10:00)
    ]
    wet = _rain(rows).split("(önümüzdeki 24 saat)</b>\n")[1].split("\n\n")[0].splitlines()
    assert wet[0] == "20:00–22:00 · en çok 0,3 mm/sa · Alaçatı"
    assert wet[1] == "23:00–00:00 · en çok 0,1 mm/sa · Alaçatı"
    assert wet[2] == "Yarın 05:00–08:00 · en çok 3,2 mm/sa · Alaçatı, Ilıca"
    assert len(wet) == 3


def test_thunder_marker() -> None:
    rows = [_row(_d(6, 5), 1.0, code=61), _row(_d(6, 6), 1.0, code=95)]
    assert "Yarın 05:00–07:00 · en çok 1,0 mm/sa · Ilıca ⛈️ gök gürültülü" in _rain(rows)
    assert "⛈️ gök" not in _rain([_row(_d(6, 5), 1.0, code=61)])


def test_range_crossing_midnight_names_both_days() -> None:
    rows = [_row(_d(5, 23), 1.0), _row(_d(6, 0), 1.0), _row(_d(6, 1), 1.0)]
    assert "23:00 – Yarın 02:00" in _rain(rows)


def test_window_covers_now_to_end_of_tomorrow() -> None:
    start, end = rain_window(NOW_R)
    assert start == datetime(2026, 11, 5, 8, 0, tzinfo=UTC)  # 11:00 local
    assert end == datetime(2026, 11, 6, 21, 0, tzinfo=UTC)  # 07.11. 00:00 local


class RainStore(FakeStore):
    def __init__(self, rows: list[Row], **kw: Any) -> None:
        super().__init__(**kw)
        self.rows = rows
        self.windows_asked: list[tuple[datetime, datetime]] = []

    def rain_hours(self, start_utc: datetime, end_utc: datetime) -> list[Row]:
        self.windows_asked.append((start_utc, end_utc))
        return self.rows


def test_reply_has_header_notice_footer_and_one_query() -> None:
    store = RainStore([_row(_d(5, 14), 1.0)])
    out = bot.reply_rain(store, CFG, NOW)
    assert out.startswith("📅 Per 05.11.2026 · 🕒 10:00")
    assert "<b>🌧️ Yağış — bugün ve yarın</b>" in out
    assert "mgm.gov.tr" in out and "Open-Meteo" in out
    assert len(store.windows_asked) == 1
    stale = bot.reply_rain(RainStore([], fetched=FRESH - timedelta(hours=10)), CFG, NOW)
    assert stale.splitlines()[1].startswith("⚠️ Veriler güncel değil")


def test_alias_registered_and_help() -> None:
    app = bot.build_application("123:abc", FakeStore(), CFG)
    cmds = {c for h in app.handlers[0] for c in getattr(h, "commands", ())}
    assert {"yagis", "yagmur"} <= cmds
    assert "/yagis" in help_text(CFG)
