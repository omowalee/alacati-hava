"""Pure helpers for the region-painted map (no Streamlit / BigQuery)."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

MODES = ("Plaj", "Yağış", "Sörf")
MODE_LABELS = {"Plaj": "🏖️ Plaj", "Yağış": "🌧️ Yağış", "Sörf": "🏄 Sörf"}

CIRCLE_RADIUS_KM = 2.0  # wide enough that a region reads as a painted area, not a strip
CIRCLE_VERTICES = 24
FILL_ALPHA = 0.45

# Presentation bins for the rain map (mm per local day). NOT alert thresholds:
# those live in config/alerts.yaml.
PRECIP_BIN_UPPER_MM = (0.1, 2.0, 10.0, 25.0)  # exclusive upper edges of the first four bins
PRECIP_LABELS = ("Kuru", "Hafif", "Orta", "Kuvvetli", "Çok kuvvetli")
PRECIP_RANGES = ("0 mm", "0,1–2 mm", "2–10 mm", "10–25 mm", "≥ 25 mm")
PRECIP_COLORS = ("#cde2fb", "#9ec5f4", "#5598e7", "#2a78d6", "#184f95")

# Beach: one teal ramp, darker = calmer. Order follows labels_tr (calmest first).
PLAJ_COLORS = ("#11707a", "#2a9d9f", "#6cc4c2", "#b5e3e0")

# Surf: orange ordinal; gray = nothing rideable; darker gray = no spot / no data.
SURF_NONE_COLOR = "#9a9a96"
SURF_RIDEABLE_COLOR = "#f19a6e"
SURF_IDEAL_COLOR = "#d95926"
NO_DATA_COLOR = "#6b6b68"
SURF_QUALITY_RANK = {"rideable": 1, "ideal": 2}
SURF_LABELS = {0: "Uygun değil", 1: "Yapılabilir", 2: "İdeal"}
SURF_COLORS = {0: SURF_NONE_COLOR, 1: SURF_RIDEABLE_COLOR, 2: SURF_IDEAL_COLOR}
NO_SPOT_LABEL = "Sörf noktası yok"
NO_DATA_LABEL = "Veri yok"


@dataclass(frozen=True)
class Swatch:
    """One legend entry."""

    color: str
    label: str
    range_text: str


@dataclass(frozen=True)
class RegionValue:
    """A region value for the active mode."""

    region_id: str
    name: str
    value: float | None
    value_text: str
    label: str
    color: str


def _missing(v: float | None) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def hex_to_rgb(color: str) -> list[int]:
    c = color.lstrip("#")
    return [int(c[i : i + 2], 16) for i in (0, 2, 4)]


def hex_to_rgba(color: str, alpha: float) -> list[int]:
    return [*hex_to_rgb(color), round(alpha * 255)]


def circle_points(
    lon: float, lat: float, radius_km: float = CIRCLE_RADIUS_KM, n: int = CIRCLE_VERTICES
) -> list[tuple[float, float]]:
    """Approximate circle as ``n`` (lon, lat) vertices."""
    dlat = radius_km / 110.574
    dlon = radius_km / (111.320 * math.cos(math.radians(lat)))
    return [
        (lon + dlon * math.cos(2 * math.pi * i / n), lat + dlat * math.sin(2 * math.pi * i / n))
        for i in range(n)
    ]


def _cross(o: tuple[float, float], a: tuple[float, float], b: tuple[float, float]) -> float:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def convex_hull(points: Iterable[tuple[float, float]]) -> list[tuple[float, float]]:
    """Monotone-chain convex hull; counter-clockwise, no repeated closing point."""
    pts = sorted(set(points))
    if len(pts) <= 2:
        return pts
    lower: list[tuple[float, float]] = []
    for p in pts:
        while len(lower) >= 2 and _cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper: list[tuple[float, float]] = []
    for p in reversed(pts):
        while len(upper) >= 2 and _cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def region_polygon(coords: Sequence[tuple[float, float]]) -> list[tuple[float, float]]:
    """Hull of circles around (lon, lat) points; one point gives a plain circle."""
    pts = [v for lon, lat in coords for v in circle_points(lon, lat)]
    return convex_hull(pts)


def centroid(coords: Sequence[tuple[float, float]]) -> tuple[float, float]:
    return (sum(c[0] for c in coords) / len(coords), sum(c[1] for c in coords) / len(coords))


def view_bounds(coords: Sequence[tuple[float, float]]) -> tuple[float, float]:
    """(lat, lon) centre of the bounding box of all (lon, lat) locations."""
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return (min(lats) + max(lats)) / 2, (min(lons) + max(lons)) / 2


def precip_bin(mm: float | None) -> int | None:
    """Index into PRECIP_LABELS, None when there is no data."""
    if _missing(mm):
        return None
    assert mm is not None
    for i, upper in enumerate(PRECIP_BIN_UPPER_MM):
        if mm < upper:
            return i
    return len(PRECIP_BIN_UPPER_MM)


def _ordered(labels: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(labels, key=lambda b: b["min"], reverse=True)


def score_band(score: float | None, labels: Sequence[Mapping[str, Any]]) -> int | None:
    """Index of the labels_tr band (min descending) containing ``score``."""
    if _missing(score):
        return None
    assert score is not None
    ordered = _ordered(labels)
    for i, band in enumerate(ordered):
        if score >= band["min"]:
            return i
    return len(ordered) - 1


def plaj_swatches(labels: Sequence[Mapping[str, Any]]) -> list[Swatch]:
    ordered = _ordered(labels)
    if len(ordered) > len(PLAJ_COLORS):
        raise ValueError("more beach_score labels than palette steps")
    out = []
    upper: float | None = None
    for band, color in zip(ordered, PLAJ_COLORS, strict=False):
        rng = f"≥ {band['min']}" if upper is None else f"{band['min']}–{int(upper) - 1}"
        out.append(Swatch(color, band["label"], rng))
        upper = band["min"]
    return out


def precip_swatches() -> list[Swatch]:
    return [
        Swatch(c, lbl, rng)
        for c, lbl, rng in zip(PRECIP_COLORS, PRECIP_LABELS, PRECIP_RANGES, strict=True)
    ]


def surf_swatches() -> list[Swatch]:
    return [
        Swatch(SURF_COLORS[2], SURF_LABELS[2], "günün en iyi penceresi"),
        Swatch(SURF_COLORS[1], SURF_LABELS[1], "günün en iyi penceresi"),
        Swatch(SURF_COLORS[0], SURF_LABELS[0], "pencere yok"),
        Swatch(NO_DATA_COLOR, NO_SPOT_LABEL, "—"),
    ]


def swatches_for(mode: str, labels: Sequence[Mapping[str, Any]]) -> list[Swatch]:
    if mode == "Plaj":
        return plaj_swatches(labels)
    if mode == "Yağış":
        return precip_swatches()
    return surf_swatches()


def best_surf_rank(
    windows: Iterable[tuple[str, str, datetime, datetime]],
    day_start: datetime,
    day_end: datetime,
) -> dict[str, int]:
    """Best quality rank per spot for windows overlapping [day_start, day_end).

    ``windows`` are (spot_id, quality, start_local, end_local) with naive local times.
    """
    best: dict[str, int] = {}
    for spot, quality, start, end in windows:
        if start < day_end and end > day_start:
            best[spot] = max(best.get(spot, 0), SURF_QUALITY_RANK.get(quality, 0))
    return best


def _fmt_mm(mm: float) -> str:
    return f"{mm:.1f}".replace(".", ",") + " mm"


def value_style(
    mode: str, v: float | None, labels: Sequence[Mapping[str, Any]]
) -> tuple[float | None, str, str, str]:
    """(value, value_text, label, color) for a raw value in the given mode."""
    if _missing(v):
        return None, "", NO_DATA_LABEL, NO_DATA_COLOR
    assert v is not None
    if mode == "Plaj":
        idx = score_band(v, labels)
        assert idx is not None
        return v, f"{round(v)}", str(_ordered(labels)[idx]["label"]), PLAJ_COLORS[idx]
    if mode == "Yağış":
        idx = precip_bin(v)
        assert idx is not None
        return v, _fmt_mm(v), PRECIP_LABELS[idx], PRECIP_COLORS[idx]
    rank = int(v)
    return float(rank), SURF_LABELS[rank], SURF_LABELS[rank], SURF_COLORS[rank]


def aggregate_regions(
    mode: str,
    regions: Sequence[Mapping[str, Any]],
    members: Mapping[str, Sequence[str]],
    values: Mapping[str, float | None],
    labels: Sequence[Mapping[str, Any]],
) -> list[RegionValue]:
    """Per-region value: Plaj = mean, Yağış = max, Sörf = best rank.

    ``members`` maps region id to the location ids relevant for the mode (beaches for
    Plaj, all locations for Yağış, surf spots for Sörf); ``values`` maps location id to
    its value (score, mm or quality rank).
    """
    out: list[RegionValue] = []
    for reg in regions:
        ids = members.get(reg["id"], [])
        if mode == "Sörf" and not ids:
            out.append(RegionValue(reg["id"], reg["name"], None, "", NO_SPOT_LABEL, NO_DATA_COLOR))
            continue
        vals = [float(v) for i in ids if not _missing(v := values.get(i))]  # type: ignore[arg-type]
        agg: float | None = None
        if vals:
            agg = sum(vals) / len(vals) if mode == "Plaj" else max(vals)
        v, text, label, color = value_style(mode, agg, labels)
        out.append(RegionValue(reg["id"], reg["name"], v, text, label, color))
    return out
