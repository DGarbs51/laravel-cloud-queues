from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from harness.log_collector import (
    LogCollector,
    validate_failed_job_event,
    validate_lifecycle_event,
    validate_sequence,
)

pytestmark = pytest.mark.socket


def connect(collector: LogCollector) -> socket.socket:
    stream = socket.socket(socket.AF_UNIX)
    stream.settimeout(1)
    stream.connect(collector.socket_path)
    return stream


def lifecycle(kind: str = "started") -> dict[str, object]:
    event: dict[str, object] = {
        "_cloud_event": "queue",
        "type": kind,
        "queue": "emails",
        "timestamp": "2026-09-27 12:00:00.123456",
    }
    if kind in {"processed", "released", "failed"}:
        event["duration_ms"] = 0
    return event


def failed() -> dict[str, object]:
    return {
        "_cloud_event": "failed_job",
        "id": "01958e54-3400-7000-8000-000000000001",
        "queue": "emails",
        "started_at": "2026-09-27 12:00:00.123456",
        "attempts": 1,
        "payload": '{"displayName":"send"}',
        "job_name": "send",
        "exception_preview": "Error: message in file:1",
        "exception": "full traceback",
    }


def test_multiple_persistent_connections_and_partial_reads() -> None:
    with LogCollector() as collector, connect(collector) as a, connect(collector) as b:
        first = json.dumps(lifecycle()).encode() + b"\n"
        second = json.dumps(lifecycle("processed")).encode() + b"\n"
        a.sendall(first[:10])
        b.sendall(second)
        collector.wait_for(lambda events: len(events) == 1)
        assert collector.raw_lines == [second]
        a.sendall(first[10:] + first)
        collector.wait_for(lambda events: len(events) == 3)
        assert collector.raw_lines == [second, first, first]
        assert not collector.errors
        with pytest.raises(TimeoutError):
            collector.wait_for(lambda events: len(events) == 4, timeout=0.01)


def test_invalid_json_and_partial_eof_are_flagged() -> None:
    with LogCollector() as collector:
        with connect(collector) as stream:
            stream.sendall(b'{bad}\n\xff\nNaN\n"x"\n{"partial":')
        collector.wait_for(lambda _: len(collector.errors) == 5)
        # stop joins handlers, making EOF capture deterministic.
        collector.stop()
        assert collector.raw_lines == [b"{bad}\n", b"\xff\n", b"NaN\n", b'"x"\n', b'{"partial":']
        assert len(collector.errors) == 5
        assert "partial" in collector.errors[-1]
        assert collector.events == ["x"]


def test_outage_restart_refuse_and_close_clients() -> None:
    with LogCollector() as collector:
        path = collector.socket_path
        with connect(collector) as stream:
            stream.sendall(b"{}\n")
            collector.wait_for(lambda events: len(events) == 1)
            collector.close_clients()
            assert stream.recv(1) == b""
        collector.stop()
        assert not Path(path).exists()
        with socket.socket(socket.AF_UNIX) as stream, pytest.raises(OSError):
            stream.connect(path)
        collector.start()
        assert collector.socket_path == path
        with connect(collector) as stream:
            stream.sendall(b"{}\n")
            collector.wait_for(lambda events: len(events) == 2)
            collector.refuse = True
            assert stream.recv(1) == b""
        collector.start()
        assert not Path(path).exists()
        collector.refuse = False
        collector.start()
        assert Path(path).exists()
    assert not Path(path).parent.exists()


@pytest.mark.parametrize("kind", ["queued", "started", "processed", "released", "failed"])
def test_lifecycle_validator_accepts(kind: str) -> None:
    assert validate_lifecycle_event(lifecycle(kind)) == []


@pytest.mark.parametrize(
    "change",
    [
        {"_cloud_event": "wrong"},
        {"type": "done"},
        {"type": []},
        {"queue": 1},
        {"timestamp": "2026-09-27T12:00:00.123456Z"},
        {"timestamp": "2026-09-27 12:00:00.123"},
        {"duration_ms": -1},
        {"duration_ms": True},
        {"duration_ms": 0.1},
    ],
)
def test_lifecycle_validator_rejects(change: dict[str, object]) -> None:
    assert validate_lifecycle_event(lifecycle("processed") | change)


def test_validator_missing_duration_and_start_duration() -> None:
    assert validate_lifecycle_event(lifecycle() | {"duration_ms": 0})
    event = lifecycle("processed")
    del event["duration_ms"]
    assert validate_lifecycle_event(event)
    assert validate_lifecycle_event([])


def test_failed_event_and_sequence() -> None:
    event = failed()
    assert not validate_failed_job_event(event)
    assert not validate_failed_job_event(
        event | {"exception_preview": "é" * 1001, "replayable": False}
    )
    events = [lifecycle(), event, lifecycle("failed")]
    assert not validate_sequence(events, ["started", "failed_job", "failed"])
    assert validate_sequence(events, ["started", "failed", "failed_job"])
    assert validate_sequence([None], [])
    assert validate_failed_job_event([])
    for key in event:
        incomplete = event.copy()
        del incomplete[key]
        assert validate_failed_job_event(incomplete), key


@pytest.mark.parametrize(
    "change",
    [
        {"id": "bad"},
        {"id": "01958e54-3400-4000-8000-000000000001"},
        {"payload": {}},
        {"attempts": True},
        {"attempts": 0},
        {"started_at": "today"},
        {"exception_preview": "é" * 1002},
        {"replayable": True},
    ],
)
def test_failed_validator_rejects(change: dict[str, object]) -> None:
    assert validate_failed_job_event(failed() | change)


@pytest.mark.parametrize("length", [16384, 16385])
def test_collector_line_limit_includes_newline_and_counts_bytes(length):
    line = b'{"payload":"' + "é".encode() * 8000
    line += b"x" * (length - len(line) - 3) + b'"}\n'
    assert len(line) == length
    with LogCollector() as collector, connect(collector) as stream:
        stream.sendall(line)
        collector.wait_for(lambda _: len(collector.raw_lines) == 1)
        assert collector.raw_lines == [line]
        assert bool(collector.errors) == (length > 16384)


@pytest.mark.parametrize("value", ["2026-13-45 99:00:00.123456", "2026-02-29 12:00:00.123456"])
def test_validators_reject_noncalendar_timestamps(value):
    assert validate_lifecycle_event(lifecycle() | {"timestamp": value})
    assert validate_failed_job_event(failed() | {"started_at": value})


def test_validators_reject_unknown_keys():
    assert validate_lifecycle_event(lifecycle() | {"unexpected": True})
    assert validate_failed_job_event(failed() | {"unexpected": True})
