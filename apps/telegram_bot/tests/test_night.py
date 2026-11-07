"""Night summary: window, verdict, rain/wind/alert sections, kinds, freshness (no network)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from test_bot import CFG, FakeSender, FakeStore

from alacati_bot.messages import Row
from alacati_bot.night import format_night, night_window, pick_wind_bin, query_range
from alacati_bot.summary import EXIT_NOT_FRESH, EXIT_OK, check_fresh, run_summary

NIGHT_NOW = datetime(2026, 11, 7, 21, 30)  # local, Saturday
NIGHT_UTC = datetime(2026, 11, 7, 18, 30, tzinfo=UTC)
MORNING_UTC = datetime(2026, 11, 7, 5, 0, tzinfo=UTC)  # 08:00 local
BINS = CFG.params.night.wind_bins


def _utc(local: datetime) -> datetime:
    return (local - timedelta(hours=3)).replace(tzinfo=UTC)


def _row(
    t_local: datetime,
    mm: float = 0.0,
    wind: float = 8.0,
    gust: float | None = 15.0,
    direction: float = 0.0,
    region: str = "alacati",
    code: int = 1,
) -> Row:
    """Mart row at local time T (rain covers [T-1h, T))."""
    return {
        "region_id": region,
        "location_id": f"{region}_1",
        "time_utc": _utc(t_local),
        "precipitation": mm,
        "precipitation_probability": 20.0,
        "weather_code": code,
        "wind_speed_10m": wind,
        "wind_gusts_10m": gust,
        "wind_direction_10m": direction,
    }


def _night_rows(**kw: object) -> list[Row]:
    start = datetime(2026, 11, 7, 22, 0)
    return [_row(start + timedelta(hours=i), **kw) for i in range(11)]  # type: ignore[arg-type]


def test_window_runs_to_morning() -> None:
    start, end = night_window(NIGHT_NOW, CFG)
    assert start == datetime(2026, 11, 7, 22, 0)
    assert end == datetime(2026, 11, 8, CFG.params.night.end_local_hour, 0)
    # after midnight the window ends this morning
    _, end2 = night_window(datetime(2026, 11, 8, 0, 30), CFG)
    assert end2.date() == date(2026, 11, 8)
    t0, t1 = query_range(start, end)
    assert t0 == datetime(2026, 11, 7, 19, 0, tzinfo=UTC)
    assert t1 == datetime(2026, 11, 8, 5, 0, tzinfo=UTC)


def test_wind_bins() -> None:
    assert pick_wind_bin(5, BINS).label == "Sakin"
    assert pick_wind_bin(12, BINS).label == "Hafif rüzgâr"
    assert pick_wind_bin(35, BINS).label == "Rüzgârlı"
    assert pick_wind_bin(80, BINS).label == "Kuvvetli rüzgâr"


def test_quiet_night() -> None:
    text = format_night(_night_rows(), [], [], CFG, NIGHT_NOW)
    assert "Sakin bir gece" in text
    assert "Bu gece için uyarı yok" in text
    assert "Yağış beklenmiyor" in text
    assert "Cmt 22:00 → Paz 08:00" in text
    assert CFG.disclaimer in text and CFG.attribution in text


def test_rainy_windy_night() -> None:
    rows = _night_rows(wind=40.0, gust=65.0, direction=315.0)
    rows[3] = _row(datetime(2026, 11, 8, 1, 0), mm=4.0, wind=45.0, gust=70.0, direction=315.0)
    text = format_night(rows, [], [], CFG, NIGHT_NOW)
    assert "Sakin bir gece" not in text
    assert "🌧️ yağmurlu" in text and "rüzgârlı" in text
    assert "Yağışlı saatler" in text and "00:00–01:00" in text
    assert "En güçlü 45 km/sa" in text and "hamle en çok 70 km/sa" in text
    assert "KB yönlü" in text
    assert "En rüzgârlı: Alaçatı, Yarın 01:00" in text


def test_strong_gusts_are_not_a_quiet_night() -> None:
    gust = CFG.params.night.quiet_gust_max_kmh
    text = format_night(_night_rows(wind=20.0, gust=gust + 5), [], [], CFG, NIGHT_NOW)
    assert "Sakin bir gece" not in text and "sert hamleli" in text


def test_thunder_verdict() -> None:
    rows = _night_rows()
    rows[2] = _row(datetime(2026, 11, 8, 0, 0), mm=8.0, code=95)
    assert "⛈️ gök gürültülü" in format_night(rows, [], [], CFG, NIGHT_NOW)


def test_rain_before_window_is_ignored() -> None:
    # the value at 22:00 covers 21:00-22:00 -> before the window (starts 22:00)
    rows = [_row(datetime(2026, 11, 7, 22, 0), mm=10.0), *_night_rows()[1:]]
    assert "Sakin bir gece" in format_night(rows, [], [], CFG, NIGHT_NOW)


def test_only_alerts_starting_tonight() -> None:
    def ev(start_local: datetime) -> Row:
        return {
            "hazard": "strong_wind",
            "level": "yellow",
            "start_utc": _utc(start_local),
            "end_utc": _utc(start_local + timedelta(hours=4)),
            "peak_value": 70.0,
            "peak_unit": "km/h",
            "affected_regions": ["alacati"],
        }

    later = format_night(_night_rows(), [], [ev(datetime(2026, 11, 8, 14, 0))], CFG, NIGHT_NOW)
    assert "Bu gece için uyarı yok" in later
    tonight = format_night(_night_rows(), [], [ev(datetime(2026, 11, 8, 3, 0))], CFG, NIGHT_NOW)
    assert "🟡" in tonight and "Uyarı var" in tonight


def test_no_data() -> None:
    text = format_night([], [], [], CFG, NIGHT_NOW)
    assert "Yağış verisi yok" in text and "Rüzgâr verisi yok" in text


def test_morning_and_night_are_separate_once_a_day() -> None:
    store, sender = FakeStore(rain=_night_rows(), fetched=NIGHT_UTC), FakeSender()
    assert run_summary(store, sender, CFG, "1", MORNING_UTC) == EXIT_OK
    assert run_summary(store, sender, CFG, "1", NIGHT_UTC, kind="night") == EXIT_OK
    assert run_summary(store, sender, CFG, "1", NIGHT_UTC, kind="night") == EXIT_OK
    assert len(sender.messages) == 2
    assert store.kinds == ["morning", "night"]
    assert "Gece özeti" in sender.messages[1]


def test_night_not_sent_too_early() -> None:
    store, sender = FakeStore(), FakeSender()
    noon = datetime(2026, 11, 7, 9, 0, tzinfo=UTC)  # 12:00 local
    assert run_summary(store, sender, CFG, "1", noon, kind="night") == EXIT_OK
    assert not sender.messages


def test_check_fresh() -> None:
    limit = timedelta(minutes=CFG.params.fresh_max_age_minutes)
    now = NIGHT_UTC
    assert check_fresh(FakeStore(fetched=now - limit + timedelta(minutes=1)), CFG, now) == 0
    old = FakeStore(fetched=now - limit - timedelta(minutes=1))
    assert check_fresh(old, CFG, now) == EXIT_NOT_FRESH
    assert check_fresh(FakeStore(fetched=None), CFG, now) == EXIT_NOT_FRESH
