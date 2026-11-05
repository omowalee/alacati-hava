"""Streamlit entry point: Çeşme Yarımadası Hava dashboard (Phase 1)."""

from __future__ import annotations

import logging

import streamlit as st

import data
import ui
from settings import load_config

log = logging.getLogger(__name__)

st.set_page_config(page_title="Çeşme Yarımadası Hava", page_icon="🌬️", layout="wide")


def _safe(label: str, fn, *args) -> None:  # noqa: ANN001, ANN002
    """Run a section; show an error box instead of killing the page.

    Details go to the server log only: BigQuery errors can contain project, dataset
    or service-account names that visitors should not see.
    """
    try:
        fn(*args)
    except Exception:  # noqa: BLE001
        log.exception("section %s failed", label)
        st.error(f"{label} şu anda yüklenemedi. Lütfen daha sonra tekrar deneyin.")


def main() -> None:
    cfg = load_config()
    _safe("Başlık", ui.render_header)
    _safe("Uyarılar", ui.render_alerts, cfg)
    hourly = None
    try:
        hourly = data.beach_scores_hourly(cfg.horizon_hours)
    except Exception:  # noqa: BLE001
        log.exception("beach scores query failed")
        st.error("Plaj skorları şu anda yüklenemedi. Lütfen daha sonra tekrar deneyin.")
    if hourly is not None:
        _safe("Harita", ui.render_map, cfg, hourly)
        _safe("Plaj sıralaması", ui.render_ranking, cfg, hourly)
    _safe("Grafikler", ui.render_charts, cfg)
    _safe("Sörf / Kite", ui.render_surf, cfg)
    ui.render_footer(cfg)


main()
