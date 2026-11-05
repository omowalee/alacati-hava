"""Streamlit entry point: Çeşme Yarımadası Hava dashboard (Phase 1)."""

from __future__ import annotations

import streamlit as st

import data
import ui
from settings import load_config

st.set_page_config(page_title="Çeşme Yarımadası Hava", page_icon="🌬️", layout="wide")


def _safe(label: str, fn, *args) -> None:  # noqa: ANN001, ANN002
    """Run a section; show an error box instead of killing the page."""
    try:
        fn(*args)
    except Exception as exc:  # noqa: BLE001
        st.error(f"{label} yüklenemedi: {type(exc).__name__}: {exc}")


def main() -> None:
    cfg = load_config()
    _safe("Başlık", ui.render_header)
    _safe("Uyarılar", ui.render_alerts, cfg)
    hourly = None
    try:
        hourly = data.beach_scores_hourly(cfg.horizon_hours)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Plaj skorları yüklenemedi: {type(exc).__name__}: {exc}")
    if hourly is not None:
        _safe("Harita", ui.render_map, cfg, hourly)
        _safe("Plaj sıralaması", ui.render_ranking, cfg, hourly)
    _safe("Grafikler", ui.render_charts, cfg)
    _safe("Sörf / Kite", ui.render_surf, cfg)
    ui.render_footer(cfg)


main()
