from __future__ import annotations

import json

import httpx
import pytest
import respx
from conftest import NOW, FakeStore, make_payload

from alacati_ingestion import cli
from alacati_ingestion.kafka_sink import DeliveryCallback, KafkaSinkStore
from alacati_ingestion.open_meteo import ApiConfig, OpenMeteoClient, Point, RateLimiter
from alacati_ingestion.runner import RunResult, execute_run


class FakeProducer:
    def __init__(self, fail_keys: tuple[str, ...] = (), unflushed: tuple[str, ...] = ()) -> None:
        self.fail_keys = fail_keys
        self.unflushed = unflushed
        self.sent: list[tuple[str, str, bytes]] = []
        self._pending: list[tuple[str, DeliveryCallback]] = []
        self.flush_timeout: float | None = None

    def produce(self, topic: str, key: str, value: bytes, on_delivery: DeliveryCallback) -> None:
        self.sent.append((topic, key, value))
        self._pending.append((key, on_delivery))

    def flush(self, timeout: float) -> int:
        self.flush_timeout = timeout
        left = 0
        for key, cb in self._pending:
            if key in self.unflushed:
                left += 1
            elif key in self.fail_keys:
                cb(RuntimeError("broker said no"))
            else:
                cb(None)
        return left


def _run(config: ApiConfig, points: list[Point], store: KafkaSinkStore) -> RunResult:
    respx.get(config.forecast_url).respond(json=make_payload(config, "forecast"))
    respx.get(config.marine_url).respond(json=make_payload(config, "marine"))
    with httpx.Client() as http:
        client = OpenMeteoClient(config, http, limiter=RateLimiter(10_000), sleep=lambda _s: None)
        return execute_run(
            config=config,
            points=points,
            client=client,
            store=store,
            cli_trigger="schedule",
            now=lambda: NOW,
        )


@respx.mock
def test_messages_published_with_key_topic_value(config: ApiConfig, points: list[Point]) -> None:
    producer, ops = FakeProducer(), FakeStore()
    result = _run(config, points, KafkaSinkStore(producer, ops, flush_timeout_s=7))
    assert result.summary.status == "complete"
    assert producer.flush_timeout == 7
    assert len(producer.sent) == len(points)
    assert {t for t, _, _ in producer.sent} == {"weather.raw"}
    for _topic, key, value in producer.sent:
        msg = json.loads(value.decode("utf-8"))
        assert key == f"{msg['source']}:{msg['point_id']}"
        assert msg["run_id"] == result.summary.run_id
    assert ops.raw_loaded == []  # nothing goes to BigQuery raw
    assert ops.ops is not None
    assert ops.ops[1].succeeded_messages == len(points)


@respx.mock
def test_failed_delivery_is_partial(config: ApiConfig, points: list[Point]) -> None:
    bad = next(p for p in points if p.source == "marine")
    producer = FakeProducer(fail_keys=(f"marine:{bad.point_id}",))
    ops = FakeStore()
    result = _run(config, points, KafkaSinkStore(producer, ops))
    s = result.summary
    assert (s.status, s.failed_messages) == ("partial", 1)
    assert s.succeeded_messages == len(points) - 1
    assert ops.ops is not None
    assert ops.ops[1].status == "partial"


@respx.mock
def test_undelivered_after_flush_timeout_not_counted(
    config: ApiConfig, points: list[Point]
) -> None:
    keys = tuple(f"{p.source}:{p.point_id}" for p in points)
    result = _run(config, points, KafkaSinkStore(FakeProducer(unflushed=keys), FakeStore()))
    assert result.summary.status == "failed"


def test_run_id_is_last_stdout_line(capsys: pytest.CaptureFixture[str]) -> None:
    from conftest import NOW as T

    from alacati_ingestion.runner import RunSummary

    result = RunResult(RunSummary("abc-123", "manual", T, 3, 1, T, 1, 0, "complete"))
    cli._print_summary(result, dry_run=True)
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[-1] == "RUN_ID=abc-123"


def test_default_sink_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_SINK", raising=False)
    assert cli.default_sink() == "bigquery"
    monkeypatch.setenv("INGEST_SINK", "kafka")
    assert cli.default_sink() == "kafka"
