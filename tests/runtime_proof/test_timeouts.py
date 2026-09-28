"""Subprocess proofs of D2: exit 124, lifecycle event, redelivery, terminal failure."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import pytest
from broker import Broker
from support import explain, note, read_events, spawn, wait_until_visible

pytestmark = pytest.mark.runtime_proof

TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}")
PROMPT_TIMEOUT = 0.6
NATIVE_TIMEOUT = 0.45


def _setup(tmp_path: Path) -> tuple[Broker, Path, str]:
    broker = Broker(str(tmp_path / "broker.sqlite"))
    broker.init()
    events = tmp_path / "events.ndjson"
    message_id = broker.publish(json.dumps({"job": "proof"}))
    return broker, events, message_id


def _args(
    broker: Broker,
    events: Path,
    *,
    handler: str,
    timeout: float,
    tries: int,
    visibility: float,
    work: float,
    collector_path: str = "",
    fail_on_timeout: bool = False,
    lease: float = 0.0,
    iterations: int = 0,
    linger: float = 0.0,
    role: str = "owner",
    wait: float = 0.0,
) -> list[str]:
    args = [
        "--broker",
        broker.path,
        "--events",
        str(events),
        "--handler",
        handler,
        "--timeout",
        str(timeout),
        "--tries",
        str(tries),
        "--visibility",
        str(visibility),
        "--work-seconds",
        str(work),
        "--lease",
        str(lease),
        "--linger",
        str(linger),
        "--role",
        role,
        "--wait",
        str(wait),
        "--queue",
        "proof",
    ]
    if collector_path:
        args.extend(["--socket", collector_path])
    if fail_on_timeout:
        args.append("--fail-on-timeout")
    if iterations:
        args.extend(["--native-iterations", str(iterations)])
    return args


def _lifecycle(events: Path) -> dict[str, object]:
    rows = [row for row in read_events(events) if row.get("_cloud_event") == "queue"]
    assert len(rows) == 1, rows
    return rows[0]


def _assert_event(event: dict[str, object], type_: str) -> int:
    assert event["type"] == type_
    assert event["queue"] == "proof"
    assert isinstance(event["duration_ms"], int)
    assert TIMESTAMP.fullmatch(str(event["timestamp"]))
    return int(event["duration_ms"])


def _assert_socket(collector_lines: list[str], event: dict[str, object]) -> None:
    assert collector_lines, "alarm handler did not complete the socket write"
    received = json.loads(collector_lines[0])
    assert received == event


@pytest.mark.parametrize("handler", ["async", "sync", "native"])
def test_retryable_timeout_redelivers(
    tmp_path: Path,
    collector,
    handler: str,
    native_iterations: int,
) -> None:
    broker, events, message_id = _setup(tmp_path)
    timeout = NATIVE_TIMEOUT if handler == "native" else PROMPT_TIMEOUT
    visibility = 2.5 if handler == "native" else 1.15
    proc = spawn(
        _args(
            broker,
            events,
            handler=handler,
            timeout=timeout,
            tries=3,
            visibility=visibility,
            work=6,
            collector_path=collector.path,
            iterations=native_iterations if handler == "native" else 0,
        ),
        timeout=20,
    )
    assert proc.returncode == 124, explain(proc)
    event = _lifecycle(events)
    duration_ms = _assert_event(event, "released")
    _assert_socket(collector.lines, event)
    assert [row for row in read_events(events) if row.get("kind") == "failure"] == []
    row = broker.get(message_id)
    assert int(row["deleted"]) == 0
    assert int(row["receive_count"]) == 1
    if handler == "native":
        assert duration_ms >= 900, duration_ms
    else:
        assert 450 <= duration_ms <= 1200, duration_ms

    wait_until_visible(broker, message_id)
    second = spawn(
        _args(
            broker,
            events,
            handler="noop",
            timeout=5,
            tries=3,
            visibility=2,
            work=0,
        ),
        timeout=10,
    )
    assert second.returncode == 0, explain(second)
    done = broker.get(message_id)
    assert int(done["deleted"]) == 1
    assert int(done["receive_count"]) == 2
    receives = [op for op in broker.operations(message_id) if op["op"] == "receive"]
    assert [op["detail"] for op in receives] == ["1", "2"]
    note(
        "retryable",
        handler=handler,
        duration_ms=duration_ms,
        overrun_ms=duration_ms - int(timeout * 1000),
        iterations=native_iterations if handler == "native" else 0,
    )


@pytest.mark.parametrize("handler", ["async", "sync", "native"])
def test_terminal_timeout_on_last_attempt(
    tmp_path: Path,
    collector,
    handler: str,
    native_iterations: int,
) -> None:
    _assert_terminal(
        tmp_path,
        collector,
        handler,
        native_iterations,
        tries=1,
        fail_on_timeout=False,
        scenario="last_attempt",
    )


@pytest.mark.parametrize("handler", ["async", "sync", "native"])
def test_fail_on_timeout_first_attempt(
    tmp_path: Path,
    collector,
    handler: str,
    native_iterations: int,
) -> None:
    _assert_terminal(
        tmp_path,
        collector,
        handler,
        native_iterations,
        tries=4,
        fail_on_timeout=True,
        scenario="fail_on_timeout",
    )


def _assert_terminal(
    tmp_path: Path,
    collector,
    handler: str,
    native_iterations: int,
    *,
    tries: int,
    fail_on_timeout: bool,
    scenario: str,
) -> None:
    broker, events, message_id = _setup(tmp_path)
    timeout = NATIVE_TIMEOUT if handler == "native" else PROMPT_TIMEOUT
    proc = spawn(
        _args(
            broker,
            events,
            handler=handler,
            timeout=timeout,
            tries=tries,
            visibility=5,
            work=6,
            collector_path=collector.path,
            fail_on_timeout=fail_on_timeout,
            iterations=native_iterations if handler == "native" else 0,
        ),
        timeout=20,
    )
    assert proc.returncode == 124, explain(proc)
    rows = read_events(events)
    failures = [row for row in rows if row.get("kind") == "failure"]
    assert len(failures) == 1
    failure = failures[0]
    assert failure["reason"] == "TimeoutExceeded"
    assert failure["attempt"] == 1
    assert failure["tries"] == tries
    assert failure["fail_on_timeout"] is fail_on_timeout
    assert failure["payload"] == json.dumps({"job": "proof"})
    failure_at = rows.index(failure)
    event = _lifecycle(events)
    assert rows.index(event) > failure_at
    duration_ms = _assert_event(event, "failed")
    _assert_socket(collector.lines, event)
    done = broker.get(message_id)
    assert int(done["deleted"]) == 1
    assert int(done["receive_count"]) == 1
    assert [op["op"] for op in broker.operations(message_id) if op["op"] == "delete"] == ["delete"]
    if handler == "native":
        assert duration_ms >= 900, duration_ms
    else:
        assert 450 <= duration_ms <= 1200, duration_ms
    note(
        scenario,
        handler=handler,
        duration_ms=duration_ms,
        overrun_ms=duration_ms - int(timeout * 1000),
        iterations=native_iterations if handler == "native" else 0,
    )


def test_sleep_is_interruptible(tmp_path: Path, collector) -> None:
    broker, events, message_id = _setup(tmp_path)
    timeout = 0.45
    proc = spawn(
        _args(
            broker,
            events,
            handler="sleep",
            timeout=timeout,
            tries=2,
            visibility=3,
            work=3,
            collector_path=collector.path,
        ),
        timeout=10,
    )
    assert proc.returncode == 124, explain(proc)
    duration_ms = _assert_event(_lifecycle(events), "released")
    assert 300 <= duration_ms <= 800, duration_ms
    assert int(broker.get(message_id)["deleted"]) == 0
    note(
        "sleep_interruptible", duration_ms=duration_ms, overrun_ms=duration_ms - int(timeout * 1000)
    )


def test_timer_disarmed_after_success(tmp_path: Path) -> None:
    broker, events, message_id = _setup(tmp_path)
    started = time.perf_counter()
    proc = spawn(
        _args(
            broker,
            events,
            handler="noop",
            timeout=0.4,
            tries=1,
            visibility=2,
            work=0,
            linger=0.85,
        ),
        timeout=10,
    )
    elapsed = time.perf_counter() - started
    assert proc.returncode == 0, explain(proc)
    assert elapsed >= 0.75, elapsed
    rows = read_events(events)
    assert [row for row in rows if row.get("_cloud_event") == "queue"] == []
    assert [row for row in rows if row.get("kind") == "success"]
    assert int(broker.get(message_id)["deleted"]) == 1
    note("timer_disarmed", elapsed_s=round(elapsed, 3))


def test_retryable_timeout_with_watchdog_still_redelivers(tmp_path: Path, collector) -> None:
    broker, events, message_id = _setup(tmp_path)
    timeout = 0.8
    lease = 0.6
    proc = spawn(
        _args(
            broker,
            events,
            handler="sync",
            timeout=timeout,
            tries=4,
            visibility=lease,
            work=5,
            lease=lease,
            collector_path=collector.path,
        ),
        timeout=15,
    )
    assert proc.returncode == 124, explain(proc)
    event = _lifecycle(events)
    duration_ms = _assert_event(event, "released")
    assert 650 <= duration_ms <= 1400, duration_ms
    _assert_socket(collector.lines, event)
    assert [row for row in read_events(events) if row.get("kind") == "failure"] == []
    row = broker.get(message_id)
    assert int(row["deleted"]) == 0
    assert float(row["visibility_until"]) > time.time() - 0.05
    renews = [op for op in broker.operations(message_id) if op["op"] == "renew"]
    assert len(renews) >= 2, renews
    wait_until_visible(broker, message_id)
    second = spawn(
        _args(broker, events, handler="noop", timeout=5, tries=4, visibility=2, work=0),
        timeout=10,
    )
    assert second.returncode == 0, explain(second)
    assert int(broker.get(message_id)["receive_count"]) == 2
    note("timeout_with_watchdog", duration_ms=duration_ms, renews=len(renews))
