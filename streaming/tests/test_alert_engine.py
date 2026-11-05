"""alert-engine orchestration: readiness gating, write order, crash/replay safety."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fakes import FakeClock, FakeConsumer, FakeProducer, new_run_id, raw_message
from test_lifecycle import ep

from alacati_streaming.alert_engine import AlertEngineApp, alert_message
from alacati_streaming.alert_store import MartRun, RunInfo
from alacati_streaming.lifecycle import AlertState, Episode, LifecycleConfig, Notification
from alacati_streaming.settings import ALERTS_TOPIC, load_yaml_config

H = timedelta(hours=1)
STARTED = datetime(2026, 11, 5, 10, 5, tzinfo=UTC)
FINISHED = STARTED + timedelta(seconds=20)
BUILT = STARTED + timedelta(minutes=3)


def cfg() -> LifecycleConfig:
    return LifecycleConfig.from_alerts_yaml(load_yaml_config("alerts"))


class FakeStore:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.runs: dict[str, RunInfo] = {}
        self.mart: MartRun | None = None
        self.modified: datetime | None = None
        self.eps: list[Episode] = []
        self.states: dict[str, AlertState] = {}
        self.notifications: dict[str, Notification] = {}
        self.complete: list[str] = []
        self.fail_state_merge = False

    def run_info(self, run_id: str) -> RunInfo | None:
        return self.runs.get(run_id)

    def mart_run(self) -> MartRun | None:
        return self.mart

    def table_modified(self, table: str) -> datetime | None:
        return self.modified

    def episodes(self) -> list[Episode]:
        return list(self.eps)

    def active_states(self) -> list[AlertState]:
        return [s for s in self.states.values() if s.status == "active"]

    def complete_runs(self, up_to: datetime) -> list[str]:
        return list(self.complete)

    def merge_notifications(self, notes: list[Notification]) -> None:
        self.calls.append("merge_notifications")
        for n in notes:
            self.notifications.setdefault(n.notification_id, n)

    def merge_states(self, states: list[AlertState]) -> None:
        self.calls.append("merge_states")
        if self.fail_state_merge:
            raise RuntimeError("BigQuery unavailable")
        self.states.update({s.event_id: s for s in states})

    def build(self, run_id: str, episodes: list[Episode]) -> None:
        """Simulate dbt building the marts from a complete run."""
        info = self.runs[run_id]
        self.mart = MartRun(info, as_of=info.started_at)
        self.modified = info.started_at + timedelta(minutes=3)
        self.eps = episodes
        self.complete.append(run_id)


class Harness:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.store = FakeStore(self.calls)
        self.producer = FakeProducer(self.calls)
        self.consumer = FakeConsumer()
        self.clock = FakeClock()
        self.app = self.new_app()

    def new_app(self) -> AlertEngineApp:
        """A fresh engine process (in-memory state lost), same stores and topic."""
        return AlertEngineApp(
            self.consumer,
            self.store,
            self.producer,
            cfg,
            flush_timeout_s=120,
            mart_poll_interval_s=30,
            mart_wait_timeout_s=600,
            retry_backoff_s=10,
            clock=self.clock,
        )

    def publish_run(self, run_id: str, n: int = 2, status: str = "complete") -> None:
        for i in range(n):
            self.consumer.push(raw_message(run_id, n, f"p{i}"), partition=i % 3)
        self.store.runs[run_id] = RunInfo(run_id, status, STARTED, FINISHED)

    def step_all(self) -> None:
        while self.consumer.queue:
            self.app.step(0)
        self.app.step(0)


def storm(run_id: str) -> Episode:
    base = BUILT.replace(minute=0, second=0)
    return ep(base + 4 * H, base + 9 * H, level="orange", run=run_id)


def test_waits_for_dbt_then_publishes_in_a_crash_safe_order() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.step_all()
    assert h.producer.published == [] and h.consumer.committed == {}  # dbt not done yet
    h.store.build(run, [storm(run)])
    h.clock.advance(31)
    h.step_all()
    assert h.calls == ["merge_notifications", "flush", "merge_states"]
    ((topic, key, message),) = h.producer.published
    assert topic == ALERTS_TOPIC and key == message["event_id"]
    assert message["status"] == "new" and message["forecast_run_id"] == run
    assert message["issued_at"] == "2026-11-05T10:08:00Z"  # dbt build time
    assert h.consumer.committed == {0: 1, 1: 1}  # committed only after everything


def test_partial_run_is_skipped_and_committed_without_evaluation() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run, status="partial")
    h.step_all()
    assert h.calls == [] and h.consumer.committed == {0: 1, 1: 1}


def test_publish_failure_keeps_offsets_and_retry_reuses_the_notification_id() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.store.build(run, [storm(run)])
    h.producer.fail_flush = True
    h.step_all()
    assert "merge_states" not in h.calls and h.consumer.committed == {}
    (first_id,) = h.store.notifications
    h.producer.fail_flush = False
    h.clock.advance(11)
    h.step_all()
    ((_, _, message),) = h.producer.published
    assert message["notification_id"] == first_id
    assert len(h.store.notifications) == 1 and len(h.store.states) == 1
    assert h.consumer.committed == {0: 1, 1: 1}


def test_crash_after_state_merge_replay_sends_nothing_new() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.store.build(run, [storm(run)])
    h.step_all()
    assert len(h.producer.published) == 1
    # Crash before the commit: a new process re-reads the same records.
    h.app = h.new_app()
    h.consumer.commits.clear()
    h.publish_run(run)  # same run replayed
    h.step_all()
    assert len(h.producer.published) == 1
    assert len(h.store.states) == 1


def test_crash_after_publish_before_state_merge_republishes_same_id() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.store.build(run, [storm(run)])
    h.store.fail_state_merge = True
    h.step_all()
    assert h.consumer.committed == {}
    h.app = h.new_app()
    h.store.fail_state_merge = False
    h.publish_run(run)
    h.clock.advance(11)
    h.step_all()
    ids = [m["notification_id"] for _, _, m in h.producer.published]
    assert len(ids) >= 2 and len(set(ids)) == 1  # duplicate publish, same id: bot dedupes
    assert len(h.store.notifications) == 1


def test_gives_up_on_a_run_that_dbt_never_builds() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.step_all()
    h.clock.advance(601)
    h.step_all()
    assert h.producer.published == [] and h.consumer.committed == {0: 1, 1: 1}


def test_older_run_evaluated_against_newer_mart_only_once() -> None:
    h = Harness()
    r1, r2 = new_run_id(), new_run_id()
    h.publish_run(r1)
    h.publish_run(r2)
    h.store.runs[r2] = RunInfo(r2, "complete", STARTED + H, FINISHED + H)
    h.store.complete.append(r1)
    h.store.build(r2, [storm(r2)])
    h.step_all()
    assert h.calls.count("merge_states") == 1
    assert len(h.producer.published) == 1


def test_alert_message_matches_the_contract_for_every_status() -> None:
    h = Harness()
    run = new_run_id()
    h.publish_run(run)
    h.store.build(run, [storm(run)])
    h.step_all()
    (state,) = h.store.states.values()
    # next complete run without the episode, twice -> ended (forecast_cleared)
    for _ in range(2):
        nxt = new_run_id()
        h.publish_run(nxt)
        h.store.runs[nxt] = RunInfo(nxt, "complete", STARTED + H, FINISHED + H)
        h.store.build(nxt, [])
        h.clock.advance(31)
        h.step_all()
    statuses = [m["status"] for _, _, m in h.producer.published]
    assert statuses == ["new", "ended"]
    ended = h.producer.published[-1][2]
    assert ended["change_reasons"] == ["forecast_cleared"] and ended["revision"] == 2
    assert ended["event_id"] == state.event_id
    for note in h.store.notifications.values():
        alert_message(note)  # raises on a contract violation
