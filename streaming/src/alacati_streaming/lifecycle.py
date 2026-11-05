"""Alert lifecycle (config/alerts.yaml steps 3-4) as a pure function.

Inputs: the episodes of the evaluated complete run (alacati_marts.mart_alert_events;
steps 1, 2 and 5 are done in dbt), the active rows of alacati_ops.alert_state, the
ordered list of complete runs, `now` and the config. Output: the alert_state rows to
upsert and the weather.alerts notifications to publish. No I/O, no clock, no randomness:
the same inputs always give the same outputs, which is what makes a replay after a crash
safe (same event_id, same revision, same notification_id).

Decisions worth knowing:
- Everything is compared with what was last NOTIFIED (notified_*), not with the
  previous run.
- mart_alert_events clips an ongoing episode at "now" (start_utc creeps forward every
  hour) and at now + horizon_hours (end_utc creeps forward). Neither is a window shift:
  the notified start is compared as max(notified_start, hour_floor(now)), and a later
  end is ignored while the notified end itself was clipped by the horizon.
- Downgrades need downgrade_confirm_runs consecutive lower runs. alert_state keeps the
  level of the last run (`level`) next to `notified_level`; that gives one run of
  history, so values 1 and 2 are supported (a larger value would need a counter column,
  i.e. a contract change). A run in which the event is absent breaks "consecutive": the
  stored `level` is reset to `notified_level` so the pending-downgrade count restarts.
- absent_runs is derived, not incremented: the number of complete runs after
  last_seen_run_id. Re-evaluating the same run gives the same count.
- A state row whose last_seen_run_id is the evaluated run was already processed (crash
  after the state MERGE, before the offset commit): it is left alone.
- The "ended" notification carries the last NOTIFIED level (notified_level), never an
  unconfirmed lower `level`.

Known rare window: if the process crashes after publishing a "new" notification but
before the alert_state MERGE, and the replay then runs against a NEWER dbt run whose
episode no longer yields the same episode_key, a second "new" (different event_id)
can be published for the same real-world event. A replay against the same run is
exactly idempotent.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

HOUR = timedelta(hours=1)


# --- data -------------------------------------------------------------------------------
@dataclass(frozen=True)
class RegionLevel:
    region_id: str
    level: str


@dataclass(frozen=True)
class Episode:
    """One row of mart_alert_events."""

    episode_key: str
    hazard: str
    level: str
    start_utc: datetime
    end_utc: datetime
    peak_value: float
    peak_unit: str
    peak_time_utc: datetime
    affected_regions: tuple[str, ...]
    region_levels: tuple[RegionLevel, ...]
    affected_locations: tuple[str, ...]
    forecast_run_id: str


@dataclass(frozen=True)
class AlertState:
    """One row of alacati_ops.alert_state (contracts/bigquery/ops/alert_state.json)."""

    event_id: str
    hazard: str
    status: str  # active | ended
    level: str
    start_utc: datetime
    end_utc: datetime
    peak_value: float
    peak_unit: str
    peak_time_utc: datetime
    affected_regions: tuple[str, ...]
    region_levels: tuple[RegionLevel, ...]
    affected_locations: tuple[str, ...]
    revision: int
    notified_level: str
    notified_start_utc: datetime
    notified_end_utc: datetime
    notified_regions: tuple[str, ...]
    last_notified_status: str
    last_notified_at: datetime
    first_detected_at: datetime
    last_updated_at: datetime
    last_seen_run_id: str
    absent_runs: int
    ended_at: datetime | None


@dataclass(frozen=True)
class Notification:
    """One weather.alerts v1 message (also one alacati_ops.alert_notifications row)."""

    notification_id: str
    event_id: str
    revision: int
    status: str  # new | updated | ended
    change_reasons: tuple[str, ...]
    hazard: str
    level: str
    previous_level: str | None
    start_utc: datetime
    end_utc: datetime
    previous_start_utc: datetime | None
    previous_end_utc: datetime | None
    peak_value: float
    peak_unit: str
    peak_time_utc: datetime
    affected_regions: tuple[str, ...]
    region_levels: tuple[RegionLevel, ...]
    affected_locations: tuple[str, ...]
    forecast_run_id: str
    issued_at: datetime


@dataclass(frozen=True)
class LifecycleConfig:
    levels: tuple[str, ...]
    notify_min_level: str
    horizon_hours: int
    match_tolerance_hours: float
    notify_window_shift_hours: float
    end_after_absent_runs: int
    downgrade_confirm_runs: int

    @classmethod
    def from_alerts_yaml(cls, alerts: dict[str, Any]) -> LifecycleConfig:
        episode = alerts["episode"]
        cfg = cls(
            levels=tuple(alerts["levels"]),
            notify_min_level=alerts["notify_min_level"],
            horizon_hours=int(alerts["horizon_hours"]),
            match_tolerance_hours=float(episode["match_tolerance_hours"]),
            notify_window_shift_hours=float(episode["notify_window_shift_hours"]),
            end_after_absent_runs=int(episode["end_after_absent_runs"]),
            downgrade_confirm_runs=int(episode["downgrade_confirm_runs"]),
        )
        if cfg.notify_min_level not in cfg.levels:
            raise ValueError(f"notify_min_level {cfg.notify_min_level!r} not in levels")
        if cfg.downgrade_confirm_runs not in (1, 2):
            raise ValueError(
                "downgrade_confirm_runs must be 1 or 2: alert_state keeps one run of level "
                "history (a larger value needs a contract change)"
            )
        if cfg.end_after_absent_runs < 1:
            raise ValueError("end_after_absent_runs must be >= 1")
        return cfg

    def rank(self, level: str) -> int:
        return self.levels.index(level)


@dataclass(frozen=True)
class Decision:
    states: list[AlertState]  # rows to upsert (MERGE on event_id); unchanged rows omitted
    notifications: list[Notification]


# --- ids ----------------------------------------------------------------------------------
def notification_id(event_id: str, revision: int) -> str:
    """contracts/kafka/weather_alert.v1: uuid5(NAMESPACE_URL, 'alacati-hava:<event>:<rev>')."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"alacati-hava:{event_id}:{revision}"))


def new_event_id(run_id: str, episode_key: str) -> str:
    """Deterministic, so a replay of the same run re-creates the same event."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"alacati-hava:event:{run_id}:{episode_key}"))


# --- helpers ------------------------------------------------------------------------------
def hour_floor(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> timedelta:
    """Overlap length; negative = the gap between two disjoint windows."""
    return min(a_end, b_end) - max(a_start, b_start)


def _horizon_clipped(end_utc: datetime, ref: datetime, cfg: LifecycleConfig) -> bool:
    """True if end_utc is at the forecast horizon seen from `ref` (true end unknown)."""
    return end_utc >= hour_floor(ref) + timedelta(hours=cfg.horizon_hours)


def _window_shifted(state: AlertState, ep: Episode, now: datetime, cfg: LifecycleConfig) -> bool:
    limit = timedelta(hours=cfg.notify_window_shift_hours)
    # Start: once the notified start is in the past the mart shows hour_floor(now)
    # (clipping), so compare against that, never against the original start.
    effective_start = max(state.notified_start_utc, hour_floor(now))
    if abs(ep.start_utc - effective_start) > limit:
        return True
    # End: an extension only counts if the notified end was a real end, not the horizon.
    if ep.end_utc < state.notified_end_utc:
        return state.notified_end_utc - ep.end_utc > limit
    if _horizon_clipped(state.notified_end_utc, state.last_notified_at, cfg):
        return False
    return ep.end_utc - state.notified_end_utc > limit


def _match(
    episodes: Sequence[Episode], states: Sequence[AlertState], cfg: LifecycleConfig
) -> dict[str, Episode]:
    """event_id -> episode. Each episode continues at most one event and vice versa.

    By design, an episode whose best event is claimed by a better-overlapping episode
    is left unmatched and therefore becomes a new event.
    """
    tolerance = timedelta(hours=cfg.match_tolerance_hours)
    # candidates: (episode, state, overlap)
    best_for_episode: dict[str, tuple[AlertState, timedelta]] = {}
    for ep in episodes:
        candidates = []
        for st in states:
            if st.hazard != ep.hazard:
                continue
            ov = overlap(ep.start_utc, ep.end_utc, st.start_utc, st.end_utc)
            if ov > timedelta(0) or -ov < tolerance:
                candidates.append((st, ov))
        if candidates:
            # largest overlap, then the oldest event, then event_id (stable)
            candidates.sort(key=lambda c: (-c[1], c[0].first_detected_at, c[0].event_id))
            best_for_episode[ep.episode_key] = candidates[0]

    # An event claimed by several episodes continues the one with the largest overlap
    # (then the earliest); the other episodes are unmatched (they may become new events).
    by_event: dict[str, list[tuple[Episode, timedelta]]] = {}
    for ep in episodes:
        if ep.episode_key in best_for_episode:
            st, ov = best_for_episode[ep.episode_key]
            by_event.setdefault(st.event_id, []).append((ep, ov))
    matched: dict[str, Episode] = {}
    for event_id, claims in by_event.items():
        claims.sort(key=lambda c: (-c[1], c[0].start_utc, c[0].episode_key))
        matched[event_id] = claims[0][0]
    return matched


def _notify(
    *,
    state_before: AlertState | None,
    event_id: str,
    revision: int,
    status: str,
    reasons: Sequence[str],
    hazard: str,
    level: str,
    start_utc: datetime,
    end_utc: datetime,
    peak_value: float,
    peak_unit: str,
    peak_time_utc: datetime,
    affected_regions: tuple[str, ...],
    region_levels: tuple[RegionLevel, ...],
    affected_locations: tuple[str, ...],
    run_id: str,
    now: datetime,
) -> Notification:
    return Notification(
        notification_id=notification_id(event_id, revision),
        event_id=event_id,
        revision=revision,
        status=status,
        change_reasons=tuple(reasons),
        hazard=hazard,
        level=level,
        previous_level=state_before.notified_level if state_before else None,
        start_utc=start_utc,
        end_utc=end_utc,
        previous_start_utc=state_before.notified_start_utc if state_before else None,
        previous_end_utc=state_before.notified_end_utc if state_before else None,
        peak_value=peak_value,
        peak_unit=peak_unit,
        peak_time_utc=peak_time_utc,
        affected_regions=affected_regions,
        region_levels=region_levels,
        affected_locations=affected_locations,
        forecast_run_id=run_id,
        issued_at=now,
    )


def _absent_runs(state: AlertState, complete_runs: Sequence[str], cfg: LifecycleConfig) -> int:
    """Complete runs after the one that last contained the event (capped at the limit)."""
    try:
        idx = list(complete_runs).index(state.last_seen_run_id)
    except ValueError:
        return cfg.end_after_absent_runs  # older than the run window: long gone
    return min(len(complete_runs) - 1 - idx, cfg.end_after_absent_runs)


# --- the decision -------------------------------------------------------------------------
def decide(
    episodes: Sequence[Episode],
    states: Sequence[AlertState],
    *,
    run_id: str,
    complete_runs: Sequence[str],
    now: datetime,
    config: LifecycleConfig,
) -> Decision:
    """Lifecycle for one evaluated complete run.

    `states`: active alert_state rows. `complete_runs`: run ids with status complete,
    oldest first, ending with `run_id`. `now`: evaluation time (UTC), also issued_at.
    """
    cfg = config
    if not complete_runs or complete_runs[-1] != run_id:
        raise ValueError("complete_runs must end with the evaluated run_id")
    active = [s for s in states if s.status == "active"]
    matched = _match(episodes, active, cfg)
    matched_keys = {ep.episode_key for ep in matched.values()}

    out_states: list[AlertState] = []
    notes: list[Notification] = []

    for st in sorted(active, key=lambda s: (s.first_detected_at, s.event_id)):
        if st.last_seen_run_id == run_id:
            continue  # this run was already applied to the row (replay)
        ep = matched.get(st.event_id)
        if ep is not None and ep.end_utc > now:
            row, note = _continue(st, ep, run_id, now, cfg)
        else:
            row, note = _absent_or_ended(st, ep, complete_runs, now, cfg)
        if row != st:
            out_states.append(row)
        if note is not None:
            notes.append(note)

    min_rank = cfg.rank(cfg.notify_min_level)
    for ep in sorted(episodes, key=lambda e: (e.hazard, e.start_utc, e.episode_key)):
        if ep.episode_key in matched_keys:
            continue
        if cfg.rank(ep.level) < min_rank or ep.end_utc <= now:
            continue  # below the notify level, or entirely in the past: never "new"
        event_id = new_event_id(run_id, ep.episode_key)
        notes.append(
            _notify(
                state_before=None,
                event_id=event_id,
                revision=1,
                status="new",
                reasons=["first_detected"],
                hazard=ep.hazard,
                level=ep.level,
                start_utc=ep.start_utc,
                end_utc=ep.end_utc,
                peak_value=ep.peak_value,
                peak_unit=ep.peak_unit,
                peak_time_utc=ep.peak_time_utc,
                affected_regions=ep.affected_regions,
                region_levels=ep.region_levels,
                affected_locations=ep.affected_locations,
                run_id=run_id,
                now=now,
            )
        )
        out_states.append(
            AlertState(
                event_id=event_id,
                hazard=ep.hazard,
                status="active",
                level=ep.level,
                start_utc=ep.start_utc,
                end_utc=ep.end_utc,
                peak_value=ep.peak_value,
                peak_unit=ep.peak_unit,
                peak_time_utc=ep.peak_time_utc,
                affected_regions=ep.affected_regions,
                region_levels=ep.region_levels,
                affected_locations=ep.affected_locations,
                revision=1,
                notified_level=ep.level,
                notified_start_utc=ep.start_utc,
                notified_end_utc=ep.end_utc,
                notified_regions=ep.affected_regions,
                last_notified_status="new",
                last_notified_at=now,
                first_detected_at=now,
                last_updated_at=now,
                last_seen_run_id=run_id,
                absent_runs=0,
                ended_at=None,
            )
        )
    return Decision(states=out_states, notifications=notes)


def _continue(
    st: AlertState, ep: Episode, run_id: str, now: datetime, cfg: LifecycleConfig
) -> tuple[AlertState, Notification | None]:
    """The event is in this run: maybe 'updated', otherwise a silent state update."""
    reasons: list[str] = []
    new_rank, notified_rank = cfg.rank(ep.level), cfg.rank(st.notified_level)
    lower = new_rank < notified_rank
    if new_rank > notified_rank:
        reasons.append("level_up")
    elif lower:
        # `st.level` is the previous run's level: lower there too = one confirmed run.
        earlier_lower_runs = 1 if cfg.rank(st.level) < notified_rank else 0
        if earlier_lower_runs + 1 >= cfg.downgrade_confirm_runs:
            reasons.append("level_down")
    if _window_shifted(st, ep, now, cfg):
        reasons.append("window_shift")
    if set(ep.affected_regions) - set(st.notified_regions):
        reasons.append("region_added")
    if reasons and lower and "level_down" not in reasons:
        # The message carries the current (lower) level and region_levels, so it must
        # say so, even though the downgrade alone would not have triggered it yet.
        reasons.insert(0, "level_down")

    current = replace(
        st,
        level=ep.level,
        start_utc=ep.start_utc,
        end_utc=ep.end_utc,
        peak_value=ep.peak_value,
        peak_unit=ep.peak_unit,
        peak_time_utc=ep.peak_time_utc,
        affected_regions=ep.affected_regions,
        region_levels=ep.region_levels,
        affected_locations=ep.affected_locations,
        last_seen_run_id=run_id,
        absent_runs=0,
    )
    if not reasons:
        changed = _content(current) != _content(st)
        return replace(current, last_updated_at=now if changed else st.last_updated_at), None

    revision = st.revision + 1
    note = _notify(
        state_before=st,
        event_id=st.event_id,
        revision=revision,
        status="updated",
        reasons=reasons,
        hazard=st.hazard,
        level=ep.level,
        start_utc=ep.start_utc,
        end_utc=ep.end_utc,
        peak_value=ep.peak_value,
        peak_unit=ep.peak_unit,
        peak_time_utc=ep.peak_time_utc,
        affected_regions=ep.affected_regions,
        region_levels=ep.region_levels,
        affected_locations=ep.affected_locations,
        run_id=run_id,
        now=now,
    )
    row = replace(
        current,
        revision=revision,
        notified_level=ep.level,
        notified_start_utc=ep.start_utc,
        notified_end_utc=ep.end_utc,
        notified_regions=ep.affected_regions,
        last_notified_status="updated",
        last_notified_at=now,
        last_updated_at=now,
    )
    return row, note


def _absent_or_ended(
    st: AlertState,
    ep: Episode | None,
    complete_runs: Sequence[str],
    now: datetime,
    cfg: LifecycleConfig,
) -> tuple[AlertState, Notification | None]:
    """The event is not (usefully) in this run: absent, window passed, or cleared."""
    absent = 0 if ep is not None else _absent_runs(st, complete_runs, cfg)
    end_utc = ep.end_utc if ep is not None else st.end_utc
    reason: str | None = None
    if end_utc <= now:
        reason = "window_passed"
    elif absent >= cfg.end_after_absent_runs:
        reason = "forecast_cleared"
    if reason is None:
        # An absent run breaks "consecutive" lower runs: restart the downgrade count.
        level = st.level if ep is not None else st.notified_level
        if absent == st.absent_runs and level == st.level:
            return st, None
        return replace(st, absent_runs=absent, level=level, last_updated_at=now), None

    revision = st.revision + 1
    note = _notify(
        state_before=st,
        event_id=st.event_id,
        revision=revision,
        status="ended",
        reasons=[reason],
        hazard=st.hazard,
        level=st.notified_level,
        start_utc=st.start_utc,
        end_utc=st.end_utc,
        peak_value=st.peak_value,
        peak_unit=st.peak_unit,
        peak_time_utc=st.peak_time_utc,
        affected_regions=st.affected_regions,
        region_levels=st.region_levels,
        affected_locations=st.affected_locations,
        run_id=complete_runs[-1],
        now=now,
    )
    row = replace(
        st,
        status="ended",
        absent_runs=absent,
        revision=revision,
        notified_level=st.notified_level,
        notified_start_utc=st.start_utc,
        notified_end_utc=st.end_utc,
        notified_regions=st.affected_regions,
        last_notified_status="ended",
        last_notified_at=now,
        last_updated_at=now,
        ended_at=now,
    )
    return row, note


def _content(st: AlertState) -> tuple[Any, ...]:
    return (
        st.level,
        st.start_utc,
        st.end_utc,
        st.peak_value,
        st.peak_time_utc,
        st.affected_regions,
        st.region_levels,
        st.affected_locations,
        st.absent_runs,
    )
