"""Streamlit rendering of each dashboard section."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

import data
import formatting as fmt
import regions as rg
from settings import AppConfig

NOTE_SHORE_UNVERIFIED = "* Kıyı yönü henüz doğrulanmadı; skor tahmini yöne göre hesaplandı."


def _now_local() -> datetime:
    return fmt.to_local(datetime.now(fmt.UTC))


def _today() -> date:
    return _now_local().date()


def _py(ts: object) -> datetime:
    return pd.Timestamp(ts).to_pydatetime()


def render_header() -> None:
    st.title("Çeşme Yarımadası Hava")
    ts = data.last_update()
    if ts is None:
        st.caption("Son veri güncellemesi: bilinmiyor")
    else:
        st.caption(f"Son veri güncellemesi: {fmt.to_local(ts):%d.%m.%Y %H:%M} (yerel saat)")


def _alert_card(
    level: str, label: str, when: str, peak: str, regions: str, extra: str = ""
) -> None:
    st.markdown(
        f"{fmt.level_badge_html(level)} &nbsp; **{label}** {extra}<br>"
        f"🕒 {when} &nbsp;|&nbsp; Tepe değer: **{peak}**<br>📍 {regions}",
        unsafe_allow_html=True,
    )
    st.divider()


def render_alerts(cfg: AppConfig) -> None:
    st.header("Uyarılar")
    today = _today()
    names = cfg.region_names
    active = data.active_alerts()
    events = data.alert_events(cfg.horizon_hours)

    if active.empty and events.empty:
        st.success(f"Önümüzdeki {cfg.horizon_hours} saatte uyarı yok")
        return

    if not active.empty:
        st.subheader("Aktif uyarılar")
        for row in active.itertuples():
            _alert_card(
                row.level,
                cfg.hazard_labels.get(row.hazard, row.hazard_label_tr),
                fmt.fmt_range(_py(row.start_local), _py(row.end_local), today),
                fmt.fmt_number(row.peak_value, row.peak_unit),
                fmt.region_list(list(row.affected_regions), names),
            )
    if not events.empty:
        st.subheader(f"Önümüzdeki {cfg.horizon_hours} saat")
        for row in events.itertuples():
            _alert_card(
                row.level,
                cfg.hazard_labels.get(row.hazard, row.hazard),
                fmt.fmt_range(
                    fmt.to_local(_py(row.start_utc)), fmt.to_local(_py(row.end_utc)), today
                ),
                fmt.fmt_number(row.peak_value, row.peak_unit),
                fmt.region_list(list(row.affected_regions), names),
            )


MAP_HEIGHT = 520
MAP_ZOOM = 10.8
# deck.gl TextLayer characterSet. pydeck evaluates plain strings as JS expressions, so the
# value must be a quoted JS string literal; 'auto' collects every glyph used in the data
# (needed for Turkish letters like ç, ş, ı, ğ).
MAP_LABEL_CHARS = "'auto'"
SURFACE_RGB = [26, 26, 25]
RING_RGB = [245, 245, 243]


def _swatch_html(color: str) -> str:
    return (
        f'<span style="display:inline-block;width:14px;height:14px;border-radius:3px;'
        f"background:{color};border:1px solid rgba(128,128,128,.6);"
        f'vertical-align:-2px;margin-right:6px"></span>'
    )


def _legend_html(swatches: list[rg.Swatch]) -> str:
    items = "".join(
        f'<span style="margin-right:18px;white-space:nowrap">{_swatch_html(s.color)}'
        f'<b>{s.label}</b> <span style="opacity:.75">({s.range_text})</span></span>'
        for s in swatches
    )
    return f'<div style="line-height:2">{items}</div>'


def _location_values(
    cfg: AppConfig, mode: str, day: date, daily: pd.DataFrame
) -> dict[str, float | None]:
    """Per-location raw value for the mode (score, mm or surf quality rank)."""
    locs = cfg.locations_by_id
    if mode == "Plaj":
        d = daily[pd.to_datetime(daily["date_local"]) == pd.Timestamp(day)]
        return {r.location_id: float(r.avg_score) for r in d.itertuples()}
    if mode == "Yağış":
        p = data.precip_daily(day)
        return {
            r.location_id: (None if pd.isna(r.precip_mm) else float(r.precip_mm))
            for r in p.itertuples()
        }
    win = data.surf_windows()
    tuples = [
        (
            r.spot_id,
            r.quality,
            fmt.to_local(_py(r.window_start_utc)),
            fmt.to_local(_py(r.window_end_utc)),
        )
        for r in win.itertuples()
    ]
    start = datetime(day.year, day.month, day.day)
    best = rg.best_surf_rank(tuples, start, start + timedelta(days=1))
    return {i: float(best.get(i, 0)) for i, loc in locs.items() if loc["type"] == "surf_spot"}


def render_map(cfg: AppConfig, hourly: pd.DataFrame) -> None:
    st.header("Harita")
    del hourly  # the map uses daily / hourly marts through data.* helpers
    today = _today()
    ctrl1, ctrl2 = st.columns([3, 2])
    with ctrl1:
        mode = st.radio(
            "Harita türü",
            rg.MODES,
            format_func=rg.MODE_LABELS.get,
            horizontal=True,
            key="map_mode",
        )
    with ctrl2:
        day_choice = st.radio("Gün", ["Bugün", "Yarın"], horizontal=True, key="map_day")
    day = today + timedelta(days=0 if day_choice == "Bugün" else 1)

    labels = cfg.beach_score["labels_tr"]
    locs = cfg.locations_by_id
    daily = data.beach_scores_daily()
    values = _location_values(cfg, mode, day, daily)

    wanted_type = "surf_spot" if mode == "Sörf" else "beach"
    members: dict[str, list[str]] = {}
    for loc_id, loc in locs.items():
        if mode == "Yağış" or loc["type"] == wanted_type:
            members.setdefault(loc["region"], []).append(loc_id)
    regions = cfg.locations["regions"]
    region_vals = {
        v.region_id: v for v in rg.aggregate_regions(mode, regions, members, values, labels)
    }

    poly_rows, label_rows = [], []
    for reg in regions:
        coords = [(loc["lon"], loc["lat"]) for loc in locs.values() if loc["region"] == reg["id"]]
        if not coords:
            continue
        rv = region_vals[reg["id"]]
        value_part = f" · {rv.value_text}" if rv.value_text and rv.value_text != rv.label else ""
        poly_rows.append(
            {
                "polygon": [list(p) for p in rg.region_polygon(coords)],
                "fill": rg.hex_to_rgba(rv.color, rg.FILL_ALPHA),
                "tip": f"<b>{rv.name}</b><br>{rv.label}{value_part}",
            }
        )
        lon_c, lat_c = rg.centroid(coords)
        label_rows.append({"lon": lon_c, "lat": lat_c, "text": reg["name"]})

    dot_rows = []
    for loc_id, loc in locs.items():
        if loc_id in values and mode in ("Plaj", "Yağış", "Sörf"):
            _, text, label, color = rg.value_style(mode, values[loc_id], labels)
            tip = f"<b>{loc['name']}</b><br>{label}" + (
                f" · {text}" if text and text != label else ""
            )
        else:
            color = rg.NO_DATA_COLOR
            tip = f"<b>{loc['name']}</b>"
        dot_rows.append(
            {"lon": loc["lon"], "lat": loc["lat"], "fill": rg.hex_to_rgb(color) + [255], "tip": tip}
        )

    layers = [
        pdk.Layer(
            "PolygonLayer",
            pd.DataFrame(poly_rows),
            get_polygon="polygon",
            get_fill_color="fill",
            get_line_color=SURFACE_RGB,
            line_width_min_pixels=2,
            stroked=True,
            filled=True,
            pickable=True,
            auto_highlight=True,
        ),
        pdk.Layer(
            "ScatterplotLayer",
            pd.DataFrame(dot_rows),
            get_position="[lon, lat]",
            get_fill_color="fill",
            get_line_color=RING_RGB,
            get_radius=180,
            radius_min_pixels=8,
            stroked=True,
            line_width_min_pixels=2,
            pickable=True,
        ),
        pdk.Layer(
            "TextLayer",
            pd.DataFrame(label_rows),
            get_position="[lon, lat]",
            get_text="text",
            get_size=14,
            # deck.gl renders only ASCII glyphs by default; add the Turkish letters explicitly.
            character_set=MAP_LABEL_CHARS,
            get_color=[255, 255, 255, 255],
            get_text_anchor="'middle'",
            get_alignment_baseline="'center'",
            background=True,
            get_background_color=[26, 26, 25, 170],
            background_padding=[4, 2],
            pickable=False,
        ),
    ]
    lat0, lon0 = rg.view_bounds([(loc["lon"], loc["lat"]) for loc in locs.values()])
    view = pdk.ViewState(latitude=lat0, longitude=lon0, zoom=MAP_ZOOM)
    st.pydeck_chart(
        pdk.Deck(layers=layers, initial_view_state=view, tooltip={"html": "{tip}"}),
        height=MAP_HEIGHT,
        width="stretch",
    )

    st.markdown(_legend_html(rg.swatches_for(mode, labels)), unsafe_allow_html=True)
    notes = {
        "Plaj": "Bölge rengi: bölgedeki plajların günlük ortalama sakinlik skoru "
        "(koyu = daha sakin).",
        "Yağış": "Bölge rengi: günlük toplam yağış, bölgedeki en yağışlı konum.",
        "Sörf": "Bölge rengi: bölgedeki sörf/kite noktalarının günün en iyi penceresi.",
    }
    st.caption(f"{notes[mode]} {day_choice}: {day:%d.%m.%Y}.")

    per_row = 3
    for start in range(0, len(regions), per_row):
        cols = st.columns(per_row)
        for col, reg in zip(cols, regions[start : start + per_row], strict=False):
            rv = region_vals[reg["id"]]
            extra = f" · {rv.value_text}" if rv.value_text and rv.value_text != rv.label else ""
            with col, st.container(border=True):
                st.markdown(
                    f"**{rv.name}**<br>{_swatch_html(rv.color)}{rv.label}{extra}",
                    unsafe_allow_html=True,
                )


def _shore_note(verified: bool) -> str:
    return "" if verified else "*"


def render_ranking(cfg: AppConfig, hourly: pd.DataFrame) -> None:
    st.header("Plaj sıralaması")
    names = cfg.region_names
    today = _today()
    daily = data.beach_scores_daily()
    if daily.empty:
        st.info("Plaj skoru verisi yok.")
        return

    day_choice = st.radio("Gün", ["Bugün", "Yarın"], horizontal=True, key="rank_day")
    target = pd.Timestamp(today) + pd.Timedelta(days=0 if day_choice == "Bugün" else 1)
    day_df = daily[pd.to_datetime(daily["date_local"]) == target].sort_values("rank_in_day")
    if day_df.empty:
        st.info("Bu gün için plaj skoru yok.")
    else:
        table = pd.DataFrame(
            {
                "Sıra": day_df["rank_in_day"],
                "Plaj": [
                    n + _shore_note(v)
                    for n, v in zip(day_df["location_name"], day_df["shore_verified"], strict=True)
                ],
                "Bölge": [names.get(r, r) for r in day_df["region_id"]],
                "Ort. skor": day_df["avg_score"].round(0).astype(int),
                "Durum": day_df["label_tr"],
                "En iyi saat": [f"{pd.Timestamp(t):%H:%M}" for t in day_df["best_hour_local"]],
            }
        )
        st.dataframe(table, hide_index=True, width="stretch")
        st.caption(NOTE_SHORE_UNVERIFIED)

    st.subheader("Saatlik sıralama")
    if hourly.empty:
        st.info("Saatlik skor verisi yok.")
        return
    hours = sorted(hourly["time_local"].unique())
    pick = st.selectbox(
        "Saat",
        hours,
        format_func=lambda t: fmt.fmt_day_time(_py(t), today),
        key="rank_hour",
    )
    h = hourly[hourly["time_local"] == pick].sort_values("rank_in_hour")
    table = pd.DataFrame(
        {
            "Sıra": h["rank_in_hour"],
            "Plaj": [
                n + _shore_note(v)
                for n, v in zip(h["location_name"], h["shore_verified"], strict=True)
            ],
            "Bölge": [names.get(r, r) for r in h["region_id"]],
            "Skor": h["score"],
            "Durum": h["label_tr"],
            "Rüzgâr (km/sa)": h["wind_speed_10m"].round(0),
            "Hamle (km/sa)": h["wind_gusts_10m"].round(0),
            "Yön": [f"{fmt.wind_arrow(d)} {fmt.compass(d)}" for d in h["wind_direction_10m"]],
        }
    )
    st.dataframe(table, hide_index=True, width="stretch")
    st.caption(NOTE_SHORE_UNVERIFIED)


def render_charts(cfg: AppConfig) -> None:
    st.header("Saatlik grafikler")
    locs = cfg.locations_by_id
    loc_id = st.selectbox(
        "Konum", list(locs), format_func=lambda i: locs[i]["name"], key="chart_loc"
    )
    df = data.weather_series(loc_id, cfg.horizon_hours)
    if df.empty:
        st.info("Bu konum için saatlik veri yok.")
        return
    df = df.copy()
    df["time_local"] = pd.to_datetime(df["time_local"])
    df["yon"] = [f"{fmt.wind_arrow(d)} {fmt.compass(d)}" for d in df["wind_direction_10m"]]
    x = alt.X("time_local:T", title="Saat (yerel)", axis=alt.Axis(format="%d.%m %H:%M"))
    # Tooltip columns are copies: a column can't be both an id_var and a value_var in melt.
    df["tip_wind"] = df["wind_speed_10m"]
    df["tip_gust"] = df["wind_gusts_10m"]
    tip = [
        alt.Tooltip("time_local:T", title="Saat", format="%d.%m %H:%M"),
        alt.Tooltip("tip_wind:Q", title="Rüzgâr (km/sa)", format=".0f"),
        alt.Tooltip("tip_gust:Q", title="Hamle (km/sa)", format=".0f"),
        alt.Tooltip("yon:N", title="Yön"),
    ]
    wind = df.melt(
        id_vars=["time_local", "yon", "tip_wind", "tip_gust"],
        value_vars=["wind_speed_10m", "wind_gusts_10m"],
        var_name="seri",
        value_name="km_sa",
    )
    wind["seri"] = wind["seri"].map({"wind_speed_10m": "Rüzgâr", "wind_gusts_10m": "Hamle"})
    st.subheader("Rüzgâr ve hamle (km/sa)")
    st.altair_chart(
        alt.Chart(wind)
        .mark_line(point=True)
        .encode(
            x=x,
            y=alt.Y("km_sa:Q", title="km/sa"),
            color=alt.Color(
                "seri:N",
                title="",
                scale=alt.Scale(domain=["Rüzgâr", "Hamle"], range=["#1f77b4", "#ff7f0e"]),
            ),
            tooltip=tip,
        )
        .properties(height=260),
        width="stretch",
    )
    st.subheader("Yağış (mm)")
    st.altair_chart(
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=x,
            # Keep a visible 0–5 mm axis on dry days instead of a collapsed 0.000000 axis.
            y=alt.Y(
                "precipitation:Q",
                title="mm",
                axis=alt.Axis(format=".1f"),
                scale=alt.Scale(domainMin=0, domainMax=5, clamp=False, nice=True),
            ),
            tooltip=[
                alt.Tooltip("time_local:T", title="Saat", format="%d.%m %H:%M"),
                alt.Tooltip("precipitation:Q", title="Yağış (mm)", format=".1f"),
            ],
        )
        .properties(height=200),
        width="stretch",
    )
    st.subheader("Dalga yüksekliği (m)")
    if df["wave_height"].notna().any():
        st.altair_chart(
            alt.Chart(df)
            .mark_line(point=True, color="#1f77b4")
            .encode(
                x=x,
                y=alt.Y("wave_height:Q", title="m"),
                tooltip=[
                    alt.Tooltip("time_local:T", title="Saat", format="%d.%m %H:%M"),
                    alt.Tooltip("wave_height:Q", title="Dalga (m)", format=".1f"),
                ],
            )
            .properties(height=200),
            width="stretch",
        )
    else:
        st.caption("Bu konum için dalga verisi yok.")


def render_surf(cfg: AppConfig) -> None:
    st.header("Sörf / Kite")
    today = _today()
    locs = cfg.locations_by_id
    win = data.surf_windows()
    if win.empty:
        st.info("Önümüzdeki günlerde uygun pencere yok")
        return
    for spot_id, grp in win.groupby("spot_id", sort=True):
        name = locs.get(spot_id, {}).get("name", spot_id)
        sport = fmt.SPORT_NAMES_TR.get(grp["sport"].iloc[0], grp["sport"].iloc[0])
        st.subheader(f"{name} · {sport}")
        if not grp["directions_verified"].all():
            st.caption("⚠ Uygun rüzgâr yönü aralığı henüz doğrulanmadı.")
        table = pd.DataFrame(
            {
                "Pencere": [
                    fmt.fmt_range(fmt.to_local(_py(s)), fmt.to_local(_py(e)), today)
                    for s, e in zip(grp["window_start_utc"], grp["window_end_utc"], strict=True)
                ],
                "Kalite": [fmt.QUALITY_NAMES_TR.get(q, q) for q in grp["quality"]],
                "Süre (sa)": grp["hours"],
                "Ort. rüzgâr (km/sa)": grp["avg_wind_kmh"].round(0),
                "Ort. rüzgâr (kt)": [round(fmt.kmh_to_knots(v)) for v in grp["avg_wind_kmh"]],
                "Maks. hamle (km/sa)": grp["max_gust_kmh"].round(0),
            }
        )
        st.dataframe(table, hide_index=True, width="stretch")


def render_footer(cfg: AppConfig) -> None:
    """Disclaimer and attribution; must render even if data queries failed."""
    st.divider()
    st.caption(cfg.disclaimer)
    st.caption(f"{cfg.attribution} · [open-meteo.com](https://open-meteo.com/)")
