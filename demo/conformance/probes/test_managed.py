"""Managed-mode end-to-end probes against the public agent protocol.

Laravel v13.33.0: Foundation/Cloud/Queue.php:242-358,473-551;
Queue/Worker.php:319-356,701-740; Foundation/Cloud/FailedJobProvider.php:57-92.
"""

from __future__ import annotations

import json
import signal
import time

import pytest

from harness.agent_emulator import status
from harness.log_collector import (
    validate_failed_job_event,
    validate_lifecycle_event,
    validate_sequence,
)

from .support import capture, envelope, managed_env, rows, until, worker

pytestmark = [pytest.mark.agent, pytest.mark.subprocess]


@pytest.mark.conformance("worker.two_sequential_jobs_isolated", tier="emulated")
def test_sequential_jobs(run_process, artifacts, agent_emulator, log_collector, evidence):
    agent_emulator.poll_wait = 0.02
    traces = ["1" * 32, "2" * 32]
    ids = [
        agent_emulator.enqueue(envelope(job, trace_id=trace))
        for job, trace in zip(("demo.sync", "demo.async"), traces, strict=True)
    ]
    process = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "2",
    )
    result = process.wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    handlers = rows(artifacts, "handler")
    assert [row["message_id"] for row in handlers] == ids
    assert [row["attempt"] for row in handlers] == [1, 1]
    assert [row["trace_id"] for row in handlers] == traces
    assert all(row["main_thread"] for row in handlers)
    assert handlers[0]["dependency_id"] != handlers[1]["dependency_id"]
    assert handlers[0]["resource"] == handlers[1]["resource"]
    kinds = [row["kind"] for row in rows(artifacts)]
    assert kinds == [
        "lifespan_start",
        "dependency_start",
        "handler",
        "dependency_stop",
        "dependency_start",
        "handler",
        "dependency_stop",
        "lifespan_stop",
    ]
    events = log_collector.wait_for(lambda events: len(events) == 4)
    assert not validate_sequence(events, ["started", "processed", "started", "processed"])
    for event in events:
        assert not validate_lifecycle_event(event)
    evidence.record("worker_pid", process.process.pid)


@pytest.mark.parametrize(
    ("job", "policy", "kwargs", "attempts", "outcomes"),
    [
        (
            "demo.retry",
            {"tries": 3, "backoff": [1, 0]},
            {"label": "retry"},
            [1, 2],
            ["released", "processed"],
        ),
        (
            "demo.retry",
            {"tries": 3, "backoff": 0},
            {"label": "exhaust", "succeed_on": 9},
            [1, 2, 3],
            ["released", "released", "processed"],
        ),
        ("demo.release", {"tries": 3}, {"label": "release"}, [1, 2], ["released", "processed"]),
        ("demo.fail", {"tries": 5}, {"label": "fail"}, [1], ["processed"]),
        ("demo.default_failure", {}, {"label": "default"}, [1], ["processed"]),
        # The deployed decorator says 3 tries. The queued policy still wins (D4).
        ("demo.retry", {"tries": 1}, {"label": "old-policy", "succeed_on": 9}, [1], ["processed"]),
    ],
    ids=["retry", "exhaustion", "release", "fail", "default", "policy-deploy"],
)
@pytest.mark.conformance("retry.explicit_release", tier="emulated")
def test_outcomes(
    run_process,
    artifacts,
    agent_emulator,
    log_collector,
    evidence,
    job,
    policy,
    kwargs,
    attempts,
    outcomes,
    monkeypatch,
):
    agent_emulator.poll_wait = 0.02
    message_id = agent_emulator.enqueue(envelope(job, kwargs=kwargs, policy=policy))
    cleanup_at_ack = []
    apply_outcome = agent_emulator._apply

    def inspect_cleanup(body):
        cleanup_at_ack.append(len(rows(artifacts, "dependency_stop")))
        return apply_outcome(body)

    monkeypatch.setattr(agent_emulator, "_apply", inspect_cleanup)
    process = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        str(len(attempts)),
    )
    result = process.wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    handlers = rows(artifacts, "handler")
    assert [row["attempt"] for row in handlers] == attempts
    assert {row["message_id"] for row in handlers} == {message_id}
    results = [r.body for r in agent_emulator.results]
    assert [r["status"] for r in results] == outcomes
    assert all(r["messageId"] == message_id for r in results)
    assert agent_emulator.message(message_id).status == "processed"
    failed = kwargs["label"] in {"exhaust", "fail", "default", "old-policy"}
    events = log_collector.wait_for(lambda events: len(events) == len(attempts) * 2 + int(failed))
    expected = [event for outcome in outcomes[:-1] for event in ("started", "released")]
    expected += ["started", "failed_job", "failed"] if failed else ["started", "processed"]
    assert not validate_sequence(events, expected)
    if failed:
        assert not validate_failed_job_event(events[-2])
        assert events[-2]["attempts"] == attempts[-1]
    if kwargs["label"] in {"retry", "release"}:
        assert results[0]["delay"] == 1
        assert handlers[1]["time"] - handlers[0]["time"] >= 0.9
    evidence.record("original_message_id", message_id)
    evidence.record("retried_message_ids", [row["message_id"] for row in handlers[1:]])
    if job in {"demo.retry", "demo.release", "demo.fail"}:
        assert cleanup_at_ack == list(range(1, len(attempts) + 1))
    evidence.record("cleanup_count_at_each_ack", cleanup_at_ack)
    evidence.record("attempts", attempts)


@pytest.mark.parametrize("defect", ["unknown", "schema", "json", "array", "version", "pointer"])
@pytest.mark.conformance("worker.undecodable_message", tier="emulated")
def test_poison(run_process, artifacts, agent_emulator, log_collector, evidence, defect):
    body = envelope(
        "demo.missing" if defect == "unknown" else "demo.sync",
        kwargs={"label": 123} if defect == "schema" else None,
        policy={"tries": 99},
    )
    if defect in {"json", "array", "pointer"}:
        body = {"json": "not json", "array": "[]", "pointer": '{"@pointer":"unsupported"}'}[defect]
    elif defect == "version":
        value = json.loads(body)
        value["laravel_cloud_queues"]["version"] = 99
        body = json.dumps(value)
    message_id = agent_emulator.enqueue(body)
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "1",
    ).wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    assert not rows(artifacts, "handler")
    assert agent_emulator.message(message_id).status == "processed"
    assert agent_emulator.message(message_id).receive_count == 1
    events = log_collector.wait_for(lambda events: len(events) == 3)
    assert not validate_sequence(events, ["started", "failed_job", "failed"])
    assert events[1]["payload"] == body
    if defect in {"json", "array", "pointer"}:
        assert events[1]["job_name"] == ""


@pytest.mark.conformance("retry.dashboard_replay", tier="emulated")
def test_replay(run_process, artifacts, agent_emulator, log_collector, evidence):
    body = envelope("demo.default_failure")
    env = managed_env(artifacts, agent_emulator, log_collector)
    original = agent_emulator.enqueue(body)
    assert worker(run_process, artifacts, env, "--max-jobs", "1").wait(15).returncode == 0
    events = log_collector.wait_for(lambda events: len(events) == 3)
    replay = agent_emulator.enqueue(events[1]["payload"])
    assert worker(run_process, artifacts, env, "--max-jobs", "1").wait(15).returncode == 0
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert replay != original
    assert [row["attempt"] for row in rows(artifacts, "handler")] == [1, 1]
    assert [row["uuid"] for row in rows(artifacts, "handler")] == [json.loads(body)["uuid"]] * 2
    assert agent_emulator.message(replay).status == "processed"


@pytest.mark.parametrize(
    ("job", "style", "policy", "terminal"),
    [
        ("demo.timeout", "python", {"tries": 2, "timeout": 0.25}, False),
        ("demo.async_timeout", None, {"tries": 2, "timeout": 0.25}, False),
        ("demo.timeout", "sleep", {"tries": 2, "timeout": 0.25}, False),
        ("demo.fail_timeout", None, {"tries": 3, "timeout": 0.25, "fail_on_timeout": True}, True),
        ("demo.timeout", "python", {"tries": 1, "timeout": 0.25}, True),
    ],
    ids=["python", "async", "sleep", "fail-on-timeout", "last-attempt"],
)
@pytest.mark.conformance("timeout.release_exit_124", tier="emulated")
def test_timeout(
    run_process, artifacts, agent_emulator, log_collector, evidence, job, style, policy, terminal
):
    agent_emulator.visibility_timeout = 1
    kwargs = {"label": "timeout"}
    if style:
        kwargs["style"] = style
    message_id = agent_emulator.enqueue(envelope(job, kwargs=kwargs, policy=policy))
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "1",
    ).wait(15)
    evidence.record("exit_code", result.returncode)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 124, result.stderr
    events = log_collector.wait_for(lambda events: len(events) == (3 if terminal else 2))
    assert not validate_sequence(
        events, ["started", "failed_job", "failed"] if terminal else ["started", "released"]
    )
    if terminal:
        assert agent_emulator.message(message_id).status == "processed"
    else:
        assert not agent_emulator.results  # Timeout must not release/apply backoff.
        until(lambda: agent_emulator.message(message_id).status == "pending")
        second = worker(
            run_process,
            artifacts,
            managed_env(artifacts, agent_emulator, log_collector),
            "--max-jobs",
            "1",
        )
        assert second.wait(15).returncode == 124
        assert agent_emulator.message(message_id).receive_count == 2
        evidence.record("redelivery_count", 2)
    capture(evidence, artifacts, log_collector, agent_emulator)


@pytest.mark.parametrize("code", [422, 503])
@pytest.mark.conformance("agent.result_4xx_nonfatal", tier="emulated")
def test_result_failure(run_process, artifacts, agent_emulator, log_collector, evidence, code):
    agent_emulator.poll_wait = 0.02
    first = agent_emulator.enqueue(envelope())
    second = agent_emulator.enqueue(envelope())
    agent_emulator.inject("result", status(code))
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "2",
    ).wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    assert [r.body["messageId"] for r in agent_emulator.results].count(first) == 1
    assert agent_emulator.message(second).receive_count == (1 if code == 422 else 0)
    assert agent_emulator.message(first).status != "processed"
    assert log_collector.wait_for(lambda events: len(events) >= 2)[1]["type"] == "processed"


@pytest.mark.conformance("worker.graceful_shutdown", tier="emulated")
def test_graceful_shutdown(run_process, artifacts, agent_emulator, log_collector, evidence):
    first = agent_emulator.enqueue(envelope("demo.slow", kwargs={"label": "slow", "seconds": 1}))
    second = agent_emulator.enqueue(envelope())
    process = worker(run_process, artifacts, managed_env(artifacts, agent_emulator, log_collector))
    until(lambda: bool(rows(artifacts, "handler")))
    process.send_signal(signal.SIGTERM)
    time.sleep(0.05)
    process.send_signal(signal.SIGTERM)
    result = process.wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    assert agent_emulator.message(first).status == "processed"
    assert agent_emulator.message(second).receive_count == 0
    assert rows(artifacts, "handler_finished")
    assert rows(artifacts, "lifespan_stop")


@pytest.mark.conformance("observability.outage_nonfatal", tier="emulated")
def test_socket_outage(run_process, artifacts, agent_emulator, log_collector, evidence):
    env = managed_env(artifacts, agent_emulator, log_collector)
    log_collector.stop()
    message_id = agent_emulator.enqueue(envelope())
    result = worker(run_process, artifacts, env, "--max-jobs", "1").wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 0, result.stderr
    assert agent_emulator.message(message_id).status == "processed"
    assert not log_collector.events
    evidence.observed("Unavailable collector did not change successful acknowledgement")


@pytest.mark.conformance("agent.receive_selection", tier="emulated")
def test_agent_queue_assignment(run_process, artifacts, agent_emulator, log_collector, evidence):
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--queue",
        "wrong",
        "--max-jobs",
        "1",
    ).wait(10)
    assert result.returncode == 2, result.stderr
    assert not agent_emulator.results
    evidence.record("conflicting_queue_exit", result.returncode)


@pytest.mark.conformance("worker.ambiguous_ack_stops", tier="emulated")
def test_ambiguous_ack(run_process, artifacts, agent_emulator, log_collector, evidence):
    first = agent_emulator.enqueue(envelope())
    second = agent_emulator.enqueue(envelope())
    agent_emulator.inject("result", "apply_then_disconnect")
    # Exhaust the two connection retries too. A subsequent 404 instead exercises
    # the deliberately nonfatal acknowledgement-rejection path.
    agent_emulator.inject("result", "disconnect", times=2)
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "2",
    ).wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    # Agent lost-response parity is AgentUnavailableError/exit 0; direct brokers use exit 1.
    assert result.returncode == 0, result.stderr
    assert agent_emulator.message(first).status == "processed"
    assert agent_emulator.message(second).receive_count == 0
    assert len(agent_emulator.results) == 3
    assert all(r.body["messageId"] == first for r in agent_emulator.results)
    assert all(r.body["status"] == "processed" for r in agent_emulator.results)
    assert all(r.response_code is None for r in agent_emulator.results)
    assert sum(r.applied_code == 200 for r in agent_emulator.results) == 1
    evidence.observed("Agent lost acknowledgement stops fetching without a contradictory report")


@pytest.mark.parametrize(
    "options",
    [
        ["--stop-when-empty"],
        ["--stop-when-empty-for", "0.1"],
        ["--max-time", "0.1"],
    ],
)
@pytest.mark.conformance("worker.lifecycle_options", tier="emulated")
def test_lifecycle_options(
    run_process, artifacts, agent_emulator, log_collector, evidence, options
):
    agent_emulator.poll_wait = 0.02
    result = worker(
        run_process, artifacts, managed_env(artifacts, agent_emulator, log_collector), *options
    ).wait(10)
    assert result.returncode == 0, result.stderr
    assert rows(artifacts, "lifespan_stop")
    capture(evidence, artifacts, log_collector, agent_emulator)


@pytest.mark.conformance("timeout.native_blocking_limitation", tier="emulated")
def test_native_timeout(run_process, artifacts, agent_emulator, log_collector, evidence):
    started = time.monotonic()
    sum(range(3000000))
    elapsed = max(time.monotonic() - started, 0.001)
    iterations = int(3000000 * 1.5 / elapsed)
    env = managed_env(artifacts, agent_emulator, log_collector)
    env["LCQ_DEMO_NATIVE_ITERATIONS"] = str(iterations)
    agent_emulator.enqueue(
        envelope(
            "demo.timeout",
            kwargs={"label": "native", "style": "native"},
            policy={"tries": 2, "timeout": 0.25},
        )
    )
    process = worker(run_process, artifacts, env, "--max-jobs", "1")
    result = process.wait(15)
    capture(evidence, artifacts, log_collector, agent_emulator)
    assert result.returncode == 124, result.stderr
    events = log_collector.wait_for(lambda events: len(events) == 2)
    assert events[1]["type"] == "released"
    duration = events[1]["duration_ms"]
    evidence.record("configured_timeout_ms", 250)
    evidence.record("observed_duration_ms", duration)
    evidence.observed(
        "Native call delays Python signal handling until it returns to the interpreter"
    )
    assert duration > 250


@pytest.mark.conformance("worker.terminal_failure_order", tier="emulated")
def test_failure_order(
    run_process, artifacts, agent_emulator, log_collector, evidence, monkeypatch
):
    """Job.php:190-216 deletes before FailedJobProvider.php:70-89 writes failed_job/failed."""
    message_id = agent_emulator.enqueue(envelope("demo.fail", policy={"tries": 5}))
    observations = []
    capture_line = log_collector._capture
    apply_outcome = agent_emulator._apply

    def capture_with_state(line, *, partial=False):
        event = json.loads(line)
        observations.append(
            (
                event.get("_cloud_event"),
                event.get("type"),
                agent_emulator.message(message_id).status,
            )
        )
        capture_line(line, partial=partial)

    def apply_after_cleanup(body):
        observations.append(("ack", bool(rows(artifacts, "dependency_stop")), None))
        return apply_outcome(body)

    monkeypatch.setattr(log_collector, "_capture", capture_with_state)
    monkeypatch.setattr(agent_emulator, "_apply", apply_after_cleanup)
    result = worker(
        run_process,
        artifacts,
        managed_env(artifacts, agent_emulator, log_collector),
        "--max-jobs",
        "1",
    ).wait(15)
    assert result.returncode == 0, result.stderr
    log_collector.wait_for(lambda events: len(events) == 3)
    assert ("ack", True, None) in observations
    assert ("failed_job", None, "processed") in observations
    assert observations[-1] == ("queue", "failed", "processed")
    evidence.record("ordering", observations)
    capture(evidence, artifacts, log_collector, agent_emulator)
