"""Streamlit rendering of each dashboard section."""

from __future__ import annotations

from datetime import date, datetime

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

import data
import formatting as fmt
from settings import AppConfig

NOTE_UNVERIFIED = "yön doğrulanmadı"
SURF_COLOR = [30, 90, 200]


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


def render_map(cfg: AppConfig, hourly: pd.DataFrame) -> None:
    st.header("Harita")
    locs = cfg.locations_by_id
    wnow = data.weather_now().set_index("location_id") if not data.weather_now().empty else None

    scores = pd.DataFrame()
    when = ""
    if not hourly.empty:
        first = hourly["time_local"].min()
        scores = hourly[hourly["time_local"] == first].set_index("location_id")
        when = f"{pd.Timestamp(first):%H:%M}"

    beach_rows, surf_rows = [], []
    for loc_id, loc in locs.items():
        base = {"lon": loc["lon"], "lat": loc["lat"], "name": loc["name"]}
        if loc["type"] == "beach" and loc_id in scores.index:
            r = scores.loc[loc_id]
            note = f"<br><i>{NOTE_UNVERIFIED}</i>" if not r["shore_verified"] else ""
            beach_rows.append(
                {
                    **base,
                    "color": fmt.score_color(r["score"]),
                    "tip": (
                        f"<b>{loc['name']}</b><br>Skor: {int(r['score'])} ({r['label_tr']})<br>"
                        f"Rüzgâr: {fmt.fmt_number(r['wind_speed_10m'], 'km/sa')} "
                        f"{fmt.wind_arrow(r['wind_direction_10m'])} "
                        f"{fmt.compass(r['wind_direction_10m'])}{note}"
                    ),
                }
            )
        elif loc["type"] == "surf_spot":
            tip = f"<b>{loc['name']}</b><br>Sörf / kite noktası"
            if wnow is not None and loc_id in wnow.index:
                w = wnow.loc[loc_id]
                tip += (
                    f"<br>Rüzgâr: {fmt.fmt_number(w['wind_speed_10m'], 'km/sa')} "
                    f"({fmt.fmt_number(fmt.kmh_to_knots(w['wind_speed_10m']), 'kt')}) "
                    f"{fmt.wind_arrow(w['wind_direction_10m'])} "
                    f"{fmt.compass(w['wind_direction_10m'])}"
                )
            surf_rows.append({**base, "color": SURF_COLOR, "tip": tip})

    layers = []
    if beach_rows:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                pd.DataFrame(beach_rows),
                get_position="[lon, lat]",
                get_fill_color="color",
                get_radius=350,
                pickable=True,
            )
        )
    if surf_rows:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                pd.DataFrame(surf_rows),
                get_position="[lon, lat]",
                get_fill_color="color",
                get_line_color=[255, 255, 255],
                get_radius=450,
                stroked=True,
                line_width_min_pixels=3,
                pickable=True,
            )
        )
    lats = [loc["lat"] for loc in locs.values()]
    lons = [loc["lon"] for loc in locs.values()]
    view = pdk.ViewState(latitude=sum(lats) / len(lats), longitude=sum(lons) / len(lons), zoom=10.2)
    st.pydeck_chart(
        pdk.Deck(layers=layers, initial_view_state=view, tooltip={"html": "{tip}"}),
        width="stretch",
    )
    suffix = f" ({when} saati)" if when else ""
    st.caption(
        f"Plaj rengi: sakinlik skoru{suffix}, yeşil = sakin, kırmızı = rüzgârlı. "
        "Mavi, beyaz çerçeveli noktalar sörf/kite noktalarıdır."
    )


def _shore_note(verified: bool) -> str:
    return "" if verified else f" ⚠ {NOTE_UNVERIFIED}"


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
