"""Telemetry gates Cloud events and writes D6b lines to stdout."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping

import pytest

from laravel_cloud_queues.observability import NullSink, Telemetry


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []
        self.timeouts: list[float | None] = []

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        self.events.append(dict(event))
        self.timeouts.append(lock_timeout)
        return True

    def close(self) -> None:
        return None


class RaisingSink:
    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        raise RuntimeError("sink down")

    def close(self) -> None:
        return None


def test_emit_is_noop_outside_managed_mode() -> None:
    """D12: sqs/redis do not emit lifecycle or failed_job events."""

    sink = RecordingSink()
    telemetry = Telemetry(sink=sink, emits_cloud_events=False)
    telemetry.emit({"_cloud_event": "queue", "type": "processed"}, lock_timeout=0.1)
    assert sink.events == []


def test_emit_forwards_lock_timeout_in_managed_mode() -> None:
    sink = RecordingSink()
    telemetry = Telemetry(sink=sink, emits_cloud_events=True)
    event: dict[str, object] = {"_cloud_event": "queue", "type": "started"}
    telemetry.emit(event, lock_timeout=0.25)
    assert sink.events == [event]
    assert sink.timeouts == [0.25]


def test_emit_swallows_sink_errors() -> None:
    telemetry = Telemetry(sink=RaisingSink(), emits_cloud_events=True)
    telemetry.emit({"_cloud_event": "queue"})


def test_log_line_writes_one_compact_json_line(capsys: pytest.CaptureFixture[str]) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    telemetry.log_line(
        {"laravel_cloud_queues": "failed_job", "queue": "emails", "n": 1.0, "path": "a/b"}
    )
    captured = capsys.readouterr().out
    assert captured.count("\n") == 1
    assert captured.startswith('{"laravel_cloud_queues":"failed_job","queue":"emails"')
    assert "1.0" in captured
    assert "a/b" in captured
    assert "\\/" not in captured
    assert json.loads(captured)["queue"] == "emails"


def test_log_line_is_not_gated_on_cloud_events(capsys: pytest.CaptureFixture[str]) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=True)
    telemetry.log_line({"ok": True})
    assert json.loads(capsys.readouterr().out) == {"ok": True}


def test_log_line_swallows_encoding_and_write_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)

    class Odd:
        pass

    telemetry.log_line({"bad": Odd()})

    class Closed:
        def write(self, data: str) -> None:
            raise OSError("closed")

        def flush(self) -> None:
            raise OSError("closed")

    monkeypatch.setattr(sys, "stdout", Closed())
    telemetry.log_line({"queue": "emails"})


def test_log_line_logging_reentry_does_not_recurse_or_log_the_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    logger = logging.getLogger("laravel_cloud_queues.observability")
    messages: list[str] = []

    class Handler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def emit(self, record: logging.LogRecord) -> None:
            self.calls += 1
            messages.append(record.getMessage())
            telemetry.log_line({"secret": "super-secret-payload"})

    class Closed:
        def write(self, data: str) -> None:
            raise OSError("closed")

        def flush(self) -> None:
            return None

    handler = Handler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    monkeypatch.setattr(sys, "stdout", Closed())
    try:
        telemetry.log_line({"secret": "super-secret-payload"})
    finally:
        logger.removeHandler(handler)
    assert handler.calls == 1
    assert messages
    assert all("super-secret-payload" not in message for message in messages)
