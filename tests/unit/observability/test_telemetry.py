"""Telemetry gates Cloud events and logs D6b lines through laravel-cloud-logging."""

from __future__ import annotations

import json
import logging
import sys
import threading
from collections.abc import Mapping

import anyio
import anyio.to_thread
import pytest
from laravel_cloud_logging import CloudHandler, LineFormatter, MonologFormatter

from laravel_cloud_queues.observability import NullSink, Telemetry


@pytest.fixture(autouse=True)
def cloud_logging(monkeypatch: pytest.MonkeyPatch) -> CloudHandler:
    """Install the handler and formatter that ``laravel_cloud_logging.configure()`` sets up."""
    handler = CloudHandler()
    handler.setFormatter(MonologFormatter())
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [handler])
    monkeypatch.setattr(root, "level", logging.INFO)
    return handler


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


def test_aemit_never_leaves_the_loop_outside_managed_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_thread(*args: object, **kwargs: object) -> None:
        raise AssertionError("aemit must not hop to a thread")

    monkeypatch.setattr(anyio.to_thread, "run_sync", no_thread)
    sink = RecordingSink()
    anyio.run(Telemetry(sink=sink, emits_cloud_events=False).aemit, {"_cloud_event": "queue"})
    assert sink.events == []


def test_aemit_emits_from_a_worker_thread_in_managed_mode() -> None:
    threads: list[int] = []

    class ThreadSink(RecordingSink):
        def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
            threads.append(threading.get_ident())
            return super().emit(event, lock_timeout=lock_timeout)

    sink = ThreadSink()
    event: dict[str, object] = {"_cloud_event": "queue", "type": "queued"}
    anyio.run(Telemetry(sink=sink, emits_cloud_events=True).aemit, event)
    assert sink.events == [event]
    assert sink.timeouts == [None]
    assert threads[0] != threading.get_ident()


def test_aemit_never_raises() -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=True)

    def broken(event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        raise RuntimeError("emit broke")

    telemetry.emit = broken  # type: ignore[method-assign]
    anyio.run(telemetry.aemit, {"_cloud_event": "queue"})


def _raise(exc: Exception) -> Exception:
    try:
        raise exc
    except Exception as caught:
        return caught


def test_log_line_writes_one_monolog_line(capfd: pytest.CaptureFixture[str]) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    telemetry.log_line(
        {"laravel_cloud_queues": "failed_job", "queue": "emails", "n": 1.0, "bad": "A\ud800"},
        message="Job failed on emails.",
        level=logging.ERROR,
        exception=_raise(RuntimeError("boom")),
    )
    captured = capfd.readouterr().out
    assert captured.count("\n") == 1
    line = json.loads(captured)
    assert list(line) == [
        "message",
        "context",
        "level",
        "level_name",
        "channel",
        "datetime",
        "extra",
    ]
    assert line["message"] == "Job failed on emails."
    assert (line["level"], line["level_name"]) == (400, "ERROR")
    assert line["extra"] == {"logger": "laravel_cloud_queues.worker"}
    context = line["context"]
    assert context["laravel_cloud_queues"] == "failed_job"
    assert context["queue"] == "emails"
    assert context["n"] == 1.0
    assert context["bad"] == "A\ufffd"
    assert context["exception"]["class"] == "RuntimeError"
    assert context["exception"]["message"] == "boom"
    assert context["exception"]["trace"]


def test_log_line_goes_through_the_worker_logger() -> None:
    records: list[logging.LogRecord] = []

    class Handler(logging.Handler):  # skipcq: PY-A6006 - test-only capture handler
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("laravel_cloud_queues.worker")
    handler = Handler()
    logger.addHandler(handler)
    try:
        Telemetry(sink=NullSink(), emits_cloud_events=False).log_line(
            {"queue": "emails"}, message="App handlers see it.", level=logging.WARNING
        )
    finally:
        logger.removeHandler(handler)
    [record] = records
    assert record.getMessage() == "App handlers see it."
    assert record.levelno == logging.WARNING
    assert record.__dict__["queue"] == "emails"


def test_log_line_is_not_gated_on_cloud_events(capfd: pytest.CaptureFixture[str]) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=True)
    telemetry.log_line({"ok": True}, message="ok")
    line = json.loads(capfd.readouterr().out)
    assert line["context"] == {"ok": True}
    assert line["level_name"] == "INFO"


def test_log_line_swallows_record_and_write_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    telemetry.log_line({"bad": {1: "non-string key"}}, message="bad")

    class Closed:
        @property
        def buffer(self) -> Closed:
            return self

        def write(self, data: bytes) -> None:
            raise OSError("closed")

        def flush(self) -> None:
            raise OSError("closed")

    monkeypatch.setattr(sys, "__stdout__", Closed())
    telemetry.log_line({"queue": "emails"}, message="closed")


def test_log_line_logging_reentry_does_not_recurse_or_log_the_record() -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    logger = logging.getLogger("laravel_cloud_queues.observability")
    messages: list[str] = []

    class Handler(logging.Handler):  # skipcq: PY-A6006 - test-only capture handler
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def emit(self, record: logging.LogRecord) -> None:
            self.calls += 1
            messages.append(record.getMessage())
            telemetry.log_line({"secret": {1: "super-secret-payload"}}, message="again")

    handler = Handler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        telemetry.log_line({"secret": {1: "super-secret-payload"}}, message="first")
    finally:
        logger.removeHandler(handler)
    assert handler.calls == 1
    assert messages
    assert all("super-secret-payload" not in message for message in messages)


def test_log_line_alarm_lock_timeout_never_uses_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    from laravel_cloud_queues.observability import _telemetry

    def fail(_: str) -> None:
        raise AssertionError("alarm fallback must not acquire a logging lock")

    monkeypatch.setattr(_telemetry, "log_failure", fail)
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
    telemetry._lock.acquire()
    try:
        started = time.monotonic()
        telemetry.log_line({"status": "failed"}, message="failed", lock_timeout=0.01)
        assert time.monotonic() - started < 0.5
    finally:
        telemetry._lock.release()
    telemetry.log_line({"invalid": {1: "key"}}, message="invalid", lock_timeout=0.01)


def test_null_sink_accepts_and_discards_events() -> None:
    sink = NullSink()
    assert sink.emit({"_cloud_event": "queue"}) is True
    assert sink.emit({"_cloud_event": "queue"}, lock_timeout=0.1) is True
    assert sink.close() is None


def test_log_line_alarm_path_writes_directly_to_the_descriptor(
    capfd: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)

    def no_handler(record: logging.LogRecord) -> bool:
        raise AssertionError("the alarm path must not take the handler lock")

    monkeypatch.setattr(CloudHandler, "handle", no_handler)
    telemetry.log_line(
        {"status": "failed", "path": "a/b"},
        message="Job failed on emails.",
        level=logging.ERROR,
        exception=_raise(RuntimeError("timed out")),
        lock_timeout=0.05,
    )
    out = capfd.readouterr().out
    assert out.endswith("}\n")
    assert out.count("\n") == 1
    line = json.loads(out)
    assert line["level_name"] == "ERROR"
    assert line["context"]["path"] == "a/b"
    assert line["context"]["exception"]["message"] == "timed out"


def test_log_line_alarm_path_uses_the_configured_formatter(
    capfd: pytest.CaptureFixture[str], cloud_logging: CloudHandler
) -> None:
    cloud_logging.setFormatter(LineFormatter(color=False))
    Telemetry(sink=NullSink(), emits_cloud_events=False).log_line(
        {"queue": "emails"}, message="Job failed.", level=logging.ERROR, lock_timeout=0.05
    )
    out = capfd.readouterr().out
    assert "ERROR" in out
    assert "Job failed.  queue=emails" in out
    assert not out.startswith("{")


def test_log_line_alarm_path_falls_back_to_json_without_a_cloud_handler(
    capfd: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(logging.getLogger(), "handlers", [logging.NullHandler()])
    Telemetry(sink=NullSink(), emits_cloud_events=False).log_line(
        {"queue": "emails"}, message="Job failed.", lock_timeout=0.05
    )
    assert json.loads(capfd.readouterr().out)["context"] == {"queue": "emails"}
