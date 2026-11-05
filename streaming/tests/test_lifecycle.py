"""Lifecycle decision (config/alerts.yaml steps 3-4): pure-function tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from alacati_streaming.lifecycle import (
    AlertState,
    Decision,
    Episode,
    LifecycleConfig,
    RegionLevel,
    decide,
    new_event_id,
    notification_id,
)
from alacati_streaming.settings import load_yaml_config

H = timedelta(hours=1)
T0 = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)  # "now" of run r1 (dbt build time)

CFG = LifecycleConfig(
    levels=("yellow", "orange", "red"),
    notify_min_level="yellow",
    horizon_hours=48,
    match_tolerance_hours=2,
    notify_window_shift_hours=3,
    end_after_absent_runs=2,
    downgrade_confirm_runs=2,
)


def ep(
    start: datetime,
    end: datetime,
    level: str = "orange",
    hazard: str = "strong_wind",
    regions: tuple[str, ...] = ("alacati",),
    run: str = "r1",
) -> Episode:
    return Episode(
        episode_key=f"{hazard}|{start.isoformat()}",
        hazard=hazard,
        level=level,
        start_utc=start,
        end_utc=end,
        peak_value=85.0,
        peak_unit="km/h",
        peak_time_utc=start,
        affected_regions=regions,
        region_levels=tuple(RegionLevel(r, level) for r in regions),
        affected_locations=("alacati_surf",),
        forecast_run_id=run,
    )


def run_decide(
    episodes: list[Episode],
    states: list[AlertState],
    run: str,
    runs: list[str],
    now: datetime,
    cfg: LifecycleConfig = CFG,
) -> Decision:
    return decide(episodes, states, run_id=run, complete_runs=runs, now=now, config=cfg)


def first_event(
    start: datetime,
    end: datetime,
    level: str = "orange",
    regions: tuple[str, ...] = ("alacati",),
) -> AlertState:
    """The state row after run r1 announced the episode as new."""
    d = run_decide([ep(start, end, level, regions=regions)], [], "r1", ["r1"], T0)
    assert len(d.states) == 1
    return d.states[0]


def merged(states: list[AlertState], decision: Decision) -> list[AlertState]:
    """Apply a decision's upserts to a state table (what MERGE does)."""
    by_id = {s.event_id: s for s in states}
    by_id.update({s.event_id: s for s in decision.states})
    return [s for s in by_id.values() if s.status == "active"]


# --- new --------------------------------------------------------------------------------
def test_new_event_is_notified_once_with_deterministic_ids() -> None:
    e = ep(T0 + 3 * H, T0 + 8 * H)
    d = run_decide([e], [], "r1", ["r1"], T0)
    assert [n.status for n in d.notifications] == ["new"]
    note = d.notifications[0]
    assert note.change_reasons == ("first_detected",)
    assert note.revision == 1 and note.previous_level is None
    assert note.event_id == new_event_id("r1", e.episode_key)
    assert note.notification_id == notification_id(note.event_id, 1)
    st = d.states[0]
    assert (st.status, st.revision, st.notified_level, st.last_seen_run_id) == (
        "active",
        1,
        "orange",
        "r1",
    )
    # same inputs again (a replay before the state MERGE) -> identical output
    assert run_decide([e], [], "r1", ["r1"], T0) == d


def test_no_repeat_on_unchanged_next_run() -> None:
    st = first_event(T0 + 3 * H, T0 + 8 * H)
    d = run_decide([ep(T0 + 3 * H, T0 + 8 * H, run="r2")], [st], "r2", ["r1", "r2"], T0 + H)
    assert d.notifications == []
    assert d.states[0].last_seen_run_id == "r2"  # silent update only
    assert d.states[0].revision == 1


def test_replay_of_an_already_applied_run_is_a_noop() -> None:
    st = first_event(T0 + 3 * H, T0 + 8 * H)
    d = run_decide([ep(T0 + 3 * H, T0 + 8 * H)], [st], "r1", ["r1"], T0)
    assert d == Decision(states=[], notifications=[])


def test_below_notify_min_level_creates_nothing() -> None:
    cfg = replace(CFG, notify_min_level="orange")
    d = run_decide([ep(T0 + 3 * H, T0 + 8 * H, level="yellow")], [], "r1", ["r1"], T0, cfg)
    assert d == Decision(states=[], notifications=[])


def test_past_only_episode_is_never_new() -> None:
    d = run_decide([ep(T0 - 5 * H, T0 - 2 * H)], [], "r1", ["r1"], T0)
    assert d == Decision(states=[], notifications=[])


# --- updated ------------------------------------------------------------------------------
def test_level_up_is_notified_immediately() -> None:
    st = first_event(T0 + 3 * H, T0 + 8 * H, level="orange")
    d = run_decide(
        [ep(T0 + 3 * H, T0 + 8 * H, level="red", run="r2")], [st], "r2", ["r1", "r2"], T0 + H
    )
    (note,) = d.notifications
    assert (note.status, note.change_reasons, note.revision) == ("updated", ("level_up",), 2)
    assert (note.level, note.previous_level) == ("red", "orange")
    assert note.previous_start_utc == st.notified_start_utc
    assert d.states[0].notified_level == "red"


def test_level_down_needs_two_consecutive_runs() -> None:
    st = first_event(T0 + 5 * H, T0 + 10 * H, level="red")
    lower = [ep(T0 + 5 * H, T0 + 10 * H, level="orange", run="r2")]
    d2 = run_decide(lower, [st], "r2", ["r1", "r2"], T0 + H)
    assert d2.notifications == []
    assert d2.states[0].level == "orange" and d2.states[0].notified_level == "red"

    states = merged([st], d2)
    lower3 = [replace(lower[0], forecast_run_id="r3")]
    d3 = run_decide(lower3, states, "r3", ["r1", "r2", "r3"], T0 + 2 * H)
    (note,) = d3.notifications
    assert note.change_reasons == ("level_down",)
    assert (note.level, note.previous_level) == ("orange", "red")


def test_absent_run_resets_pending_downgrade() -> None:
    st = first_event(T0 + 5 * H, T0 + 10 * H, level="red")
    lower = ep(T0 + 5 * H, T0 + 10 * H, level="orange", run="r2")
    d2 = run_decide([lower], [st], "r2", ["r1", "r2"], T0 + H)
    states = merged([st], d2)
    d3 = run_decide([], states, "r3", ["r1", "r2", "r3"], T0 + 2 * H)  # absent
    assert d3.notifications == []
    states = merged(states, d3)
    lower4 = replace(lower, forecast_run_id="r4")
    d4 = run_decide([lower4], states, "r4", ["r1", "r2", "r3", "r4"], T0 + 3 * H)
    assert d4.notifications == []  # downgrade count restarted: not confirmed yet


def test_ended_carries_last_notified_level_not_unconfirmed_lower() -> None:
    cfg = replace(CFG, end_after_absent_runs=3)
    st = first_event(T0 + 5 * H, T0 + 10 * H, level="red")
    lower = ep(T0 + 5 * H, T0 + 10 * H, level="orange", run="r2")
    states = merged([st], run_decide([lower], [st], "r2", ["r1", "r2"], T0 + H, cfg))
    assert states[0].level == "orange"
    runs = ["r1", "r2", "r3", "r4", "r5"]
    for i in (3, 4):
        d = run_decide([], states, f"r{i}", runs[:i], T0 + i * H, cfg)
        assert d.notifications == []
        states = merged(states, d)
    d5 = run_decide([], states, "r5", runs, T0 + 5 * H, cfg)
    (note,) = d5.notifications
    assert note.status == "ended" and note.level == "red" and note.previous_level == "red"
    assert d5.states[0].notified_level == "red"


def test_level_down_then_back_up_is_silent() -> None:
    st = first_event(T0 + 5 * H, T0 + 10 * H, level="red")
    d2 = run_decide(
        [ep(T0 + 5 * H, T0 + 10 * H, level="orange", run="r2")], [st], "r2", ["r1", "r2"], T0 + H
    )
    states = merged([st], d2)
    d3 = run_decide(
        [ep(T0 + 5 * H, T0 + 10 * H, level="red", run="r3")],
        states,
        "r3",
        ["r1", "r2", "r3"],
        T0 + 2 * H,
    )
    assert d3.notifications == []


def test_creeping_start_of_ongoing_episode_is_not_a_window_shift() -> None:
    # Announced at T0 as starting now (T0) and lasting 12 h; the mart clips the start at
    # "now", so every later run shows start = current hour.
    st = first_event(T0, T0 + 12 * H)
    states = [st]
    runs = ["r1"]
    for k in range(1, 7):  # six hourly runs: start creeps 6 h, never notified
        run = f"r{k + 1}"
        runs.append(run)
        now = T0 + k * H + timedelta(minutes=3)
        d = run_decide([ep(T0 + k * H, T0 + 12 * H, run=run)], states, run, runs, now)
        assert d.notifications == [], f"run {run} notified a creeping start"
        states = merged(states, d)


def test_horizon_clipping_of_the_end_is_not_a_window_shift() -> None:
    # Ongoing beyond the 48 h horizon: end = horizon, moving forward every run.
    st = first_event(T0 + 2 * H, T0 + 48 * H)
    states, runs = [st], ["r1"]
    for k in range(1, 6):
        run = f"r{k + 1}"
        runs.append(run)
        start = max(T0 + 2 * H, T0 + k * H)  # the mart clips the start at "now" too
        d = run_decide([ep(start, T0 + (48 + k) * H, run=run)], states, run, runs, T0 + k * H)
        assert d.notifications == []
        states = merged(states, d)


def test_end_shrinking_from_horizon_is_notified() -> None:
    st = first_event(T0 + 2 * H, T0 + 48 * H)
    d = run_decide([ep(T0 + 2 * H, T0 + 20 * H, run="r2")], [st], "r2", ["r1", "r2"], T0 + H)
    assert [n.change_reasons for n in d.notifications] == [("window_shift",)]


def test_window_shift_of_future_start_over_3h_is_notified() -> None:
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d = run_decide([ep(T0 + 14 * H, T0 + 18 * H, run="r2")], [st], "r2", ["r1", "r2"], T0 + H)
    (note,) = d.notifications
    assert note.change_reasons == ("window_shift",)
    assert note.previous_start_utc == T0 + 10 * H and note.start_utc == T0 + 14 * H


def test_window_shift_of_3h_or_less_is_silent() -> None:
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d = run_decide([ep(T0 + 13 * H, T0 + 19 * H, run="r2")], [st], "r2", ["r1", "r2"], T0 + H)
    assert d.notifications == []
    assert d.states[0].start_utc == T0 + 13 * H
    assert d.states[0].notified_start_utc == T0 + 10 * H


def test_end_extension_inside_horizon_is_notified() -> None:
    st = first_event(T0 + 2 * H, T0 + 8 * H)
    d = run_decide([ep(T0 + 2 * H, T0 + 14 * H, run="r2")], [st], "r2", ["r1", "r2"], T0 + H)
    assert [n.change_reasons for n in d.notifications] == [("window_shift",)]


def test_region_added_is_notified_and_removal_is_silent() -> None:
    st = first_event(T0 + 3 * H, T0 + 8 * H, regions=("alacati",))
    d = run_decide(
        [ep(T0 + 3 * H, T0 + 8 * H, regions=("alacati", "ildir"), run="r2")],
        [st],
        "r2",
        ["r1", "r2"],
        T0 + H,
    )
    (note,) = d.notifications
    assert note.change_reasons == ("region_added",)
    states = merged([st], d)
    d3 = run_decide(
        [ep(T0 + 3 * H, T0 + 8 * H, regions=("ildir",), run="r3")],
        states,
        "r3",
        ["r1", "r2", "r3"],
        T0 + 2 * H,
    )
    assert d3.notifications == []
    assert d3.states[0].affected_regions == ("ildir",)
    assert d3.states[0].notified_regions == ("alacati", "ildir")


def test_unconfirmed_downgrade_is_reported_when_another_reason_notifies() -> None:
    st = first_event(T0 + 3 * H, T0 + 8 * H, level="red", regions=("alacati",))
    d = run_decide(
        [ep(T0 + 3 * H, T0 + 8 * H, level="orange", regions=("alacati", "ildir"), run="r2")],
        [st],
        "r2",
        ["r1", "r2"],
        T0 + H,
    )
    (note,) = d.notifications
    assert note.change_reasons == ("level_down", "region_added")
    assert note.level == "orange"


# --- ended -----------------------------------------------------------------------------------
def test_ended_by_window_passed() -> None:
    st = first_event(T0 + 1 * H, T0 + 4 * H)
    # run at T0+4h: the mart no longer has the episode (it is over)
    d = run_decide([], [st], "r2", ["r1", "r2"], T0 + 4 * H + timedelta(minutes=5))
    (note,) = d.notifications
    assert (note.status, note.change_reasons, note.revision) == ("ended", ("window_passed",), 2)
    assert d.states[0].status == "ended" and d.states[0].ended_at is not None


def test_ended_by_absence_after_two_complete_runs() -> None:
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d2 = run_decide([], [st], "r2", ["r1", "r2"], T0 + H)
    assert d2.notifications == [] and d2.states[0].absent_runs == 1
    states = merged([st], d2)
    d3 = run_decide([], states, "r3", ["r1", "r2", "r3"], T0 + 2 * H)
    (note,) = d3.notifications
    assert (note.status, note.change_reasons) == ("ended", ("forecast_cleared",))
    assert note.level == "orange" and note.previous_level == "orange"


def test_one_run_gap_does_not_end_and_resets_on_return() -> None:
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d2 = run_decide([], [st], "r2", ["r1", "r2"], T0 + H)
    states = merged([st], d2)
    d3 = run_decide(
        [ep(T0 + 10 * H, T0 + 16 * H, run="r3")], states, "r3", ["r1", "r2", "r3"], T0 + 2 * H
    )
    assert d3.notifications == []
    assert d3.states[0].absent_runs == 0 and d3.states[0].last_seen_run_id == "r3"


def test_absent_count_is_derived_so_reevaluation_does_not_double_count() -> None:
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d2 = run_decide([], [st], "r2", ["r1", "r2"], T0 + H)
    states = merged([st], d2)
    again = run_decide([], states, "r2", ["r1", "r2"], T0 + H)  # replay after state MERGE
    assert again == Decision(states=[], notifications=[])


def test_partial_runs_do_not_count_as_absent() -> None:
    # r2 was partial, so it is not in complete_runs: only r3 counts.
    st = first_event(T0 + 10 * H, T0 + 16 * H)
    d3 = run_decide([], [st], "r3", ["r1", "r3"], T0 + 2 * H)
    assert d3.notifications == [] and d3.states[0].absent_runs == 1


# --- matching ----------------------------------------------------------------------------------
def test_episode_continues_event_with_largest_overlap() -> None:
    a = first_event(T0 + 2 * H, T0 + 6 * H)
    b = replace(
        a,
        event_id="event-b",
        start_utc=T0 + 7 * H,
        end_utc=T0 + 12 * H,
        notified_start_utc=T0 + 7 * H,
        notified_end_utc=T0 + 12 * H,
        first_detected_at=T0 + timedelta(minutes=1),
    )
    # new episode 5..12: overlaps a by 1 h, b by 5 h -> continues b; a is absent
    d = run_decide([ep(T0 + 5 * H, T0 + 12 * H, run="r2")], [a, b], "r2", ["r1", "r2"], T0 + H)
    by_id = {s.event_id: s for s in d.states}
    assert by_id["event-b"].last_seen_run_id == "r2"
    assert by_id[a.event_id].absent_runs == 1
    assert d.notifications == []


def test_episode_within_tolerance_matches_but_far_one_is_new() -> None:
    st = first_event(T0 + 2 * H, T0 + 6 * H)
    near = ep(T0 + 7 * H, T0 + 9 * H, run="r2")  # 1 h gap < 2 h tolerance
    d = run_decide([near], [st], "r2", ["r1", "r2"], T0 + H)
    assert [n.status for n in d.notifications] == ["updated"]  # end moved > 3 h
    far = ep(T0 + 9 * H, T0 + 11 * H, run="r2")  # 3 h gap: separate event
    d2 = run_decide([far], [st], "r2", ["r1", "r2"], T0 + H)
    assert sorted(n.status for n in d2.notifications) == ["new"]


def test_other_hazard_never_matches() -> None:
    st = first_event(T0 + 2 * H, T0 + 6 * H)
    d = run_decide(
        [ep(T0 + 2 * H, T0 + 6 * H, hazard="high_waves", run="r2")],
        [st],
        "r2",
        ["r1", "r2"],
        T0 + H,
    )
    assert [(n.hazard, n.status) for n in d.notifications] == [("high_waves", "new")]


# --- config ----------------------------------------------------------------------------------
def test_config_comes_from_alerts_yaml() -> None:
    cfg = LifecycleConfig.from_alerts_yaml(load_yaml_config("alerts"))
    assert cfg.levels == ("yellow", "orange", "red")
    assert cfg.downgrade_confirm_runs in (1, 2)


def test_unsupported_downgrade_confirm_runs_is_rejected() -> None:
    alerts = load_yaml_config("alerts")
    alerts["episode"]["downgrade_confirm_runs"] = 3
    with pytest.raises(ValueError, match="downgrade_confirm_runs"):
        LifecycleConfig.from_alerts_yaml(alerts)


def test_complete_runs_must_end_with_the_run() -> None:
    with pytest.raises(ValueError):
        run_decide([], [], "r2", ["r1"], T0)
