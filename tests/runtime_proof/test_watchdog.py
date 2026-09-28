"""Subprocess proofs of D7: renewal while the loop is blocked, and GIL starvation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from broker import Broker
from support import explain, note, popen, read_events, wait_for_kind

pytestmark = pytest.mark.runtime_proof


def _broker(tmp_path: Path) -> tuple[Broker, Path, str]:
    broker = Broker(str(tmp_path / "broker.sqlite"))
    broker.init()
    events = tmp_path / "owner.ndjson"
    message_id = broker.publish(json.dumps({"job": "proof"}))
    return broker, events, message_id


def _owner_args(
    broker: Broker,
    events: Path,
    *,
    handler: str,
    work: float,
    lease: float,
    timeout: float,
    iterations: int = 0,
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
        "3",
        "--visibility",
        str(lease),
        "--lease",
        str(lease),
        "--work-seconds",
        str(work),
        "--role",
        "owner",
    ]
    if iterations:
        args.extend(["--native-iterations", str(iterations)])
    return args


def _thief_args(broker: Broker, events: Path, wait: float) -> list[str]:
    return [
        "--broker",
        broker.path,
        "--events",
        str(events),
        "--handler",
        "noop",
        "--timeout",
        "0",
        "--tries",
        "3",
        "--visibility",
        "5",
        "--role",
        "thief",
        "--wait",
        str(wait),
    ]


def test_watchdog_renews_while_sync_handler_blocks_the_loop(tmp_path: Path) -> None:
    broker, events, message_id = _broker(tmp_path)
    thief_events = tmp_path / "thief.ndjson"
    lease = 1.0
    work = 2.5
    owner = popen(_owner_args(broker, events, handler="sync", work=work, lease=lease, timeout=8))
    try:
        wait_for_kind(events, "started")
        thief = popen(_thief_args(broker, thief_events, wait=2.3))
        try:
            thief.wait(timeout=15)
            owner.wait(timeout=15)
        finally:
            if thief.poll() is None:
                thief.kill()
    finally:
        if owner.poll() is None:
            owner.kill()
    owner_out, owner_err = owner.communicate()
    thief_out, thief_err = thief.communicate()
    assert owner.returncode == 0, f"stdout={owner_out}\nstderr={owner_err}"
    assert thief.returncode == 2, f"stdout={thief_out}\nstderr={thief_err}"
    done = broker.get(message_id)
    assert int(done["deleted"]) == 1
    assert int(done["receive_count"]) == 1
    ops = broker.operations(message_id)
    renews = [op for op in ops if op["op"] == "renew"]
    assert len(renews) >= 4, ops
    gaps = [float(renews[i]["at"]) - float(renews[i - 1]["at"]) for i in range(1, len(renews))]
    assert gaps
    assert max(gaps) < 0.95, gaps
    assert not [op for op in ops if op["op"] == "renew_failed"]
    assert [row for row in read_events(events) if row.get("kind") == "success"]
    note(
        "watchdog_sync",
        renews=len(renews),
        max_gap_s=round(max(gaps), 3),
        mean_gap_s=round(sum(gaps) / len(gaps), 3),
    )


def test_gil_holding_native_call_starves_renewal(tmp_path: Path, native_iterations: int) -> None:
    broker, events, message_id = _broker(tmp_path)
    thief_events = tmp_path / "thief.ndjson"
    lease = 0.36
    owner = popen(
        _owner_args(
            broker,
            events,
            handler="native",
            work=0,
            lease=lease,
            timeout=0,
            iterations=native_iterations,
        )
    )
    try:
        started = wait_for_kind(events, "started")
        thief = popen(_thief_args(broker, thief_events, wait=3.5))
        try:
            owner.wait(timeout=20)
            thief.wait(timeout=10)
        finally:
            if thief.poll() is None:
                thief.kill()
    finally:
        if owner.poll() is None:
            owner.kill()
    owner_out, owner_err = owner.communicate()
    thief_out, thief_err = thief.communicate()
    assert owner.returncode == 1, f"stdout={owner_out}\nstderr={owner_err}\nevents={read_events(events)}"
    assert thief.returncode == 0, f"stdout={thief_out}\nstderr={thief_err}"
    window = next(row for row in read_events(events) if row.get("kind") == "native_window")
    ops = broker.operations(message_id)
    renews_during = [
        op
        for op in ops
        if op["op"] == "renew" and float(op["at"]) < float(window["end"]) - 0.01
    ]
    failed = [op for op in ops if op["op"] == "renew_failed"]
    assert renews_during == [], ops
    assert failed, ops
    assert float(failed[0]["at"]) >= float(window["end"]) - 0.02
    assert int(broker.get(message_id)["receive_count"]) == 2
    assert int(broker.get(message_id)["deleted"]) == 1
    stolen = read_events(thief_events)
    assert stolen and stolen[0]["kind"] == "stolen"
    assert stolen[0]["receipt"] != started["receipt"]
    assert [row for row in read_events(events) if row.get("kind") == "lease_lost"]
    assert [row for row in read_events(events) if row.get("kind") == "success"] == []
    note(
        "watchdog_gil_starved",
        native_s=round(float(window["end"]) - float(window["start"]), 3),
        renews_during=len(renews_during),
        renew_failed=len(failed),
        renew_failed_after_s=round(float(failed[0]["at"]) - float(window["end"]), 3),
        iterations=native_iterations,
        lease_s=lease,
    )
