"""Real worker subprocesses in managed mode through the agent emulator, with lifecycle and
``failed_job`` events captured by the log collector.

Managed mode refuses ``_SQS_ENDPOINT``, so messages go straight into the emulator: the body
is a real envelope from the dispatch pipeline (``prepare_dispatch``). Event order follows
D13.1 (complete, then ``failed_job``, then ``failed``; ``Foundation/Cloud/FailedJobProvider.php``)
and D2 (timeout: ``released`` or ``failed_job`` + ``failed``, then exit 124). Agent 5xx on
``/result`` stops the worker with exit 0 (``Queue/Worker.php:419-432``).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from harness.agent_emulator import AgentEmulator, status
from harness.log_collector import LogCollector, validate_failed_job_event, validate_sequence
from laravel_cloud_queues.config import load_config
from laravel_cloud_queues.jobs.dispatch import prepare_dispatch
from laravel_cloud_queues.registry import Registry
from tests.integration.worker.apps.basic import build
from tests.integration.worker.support import Workers, clean_env

pytestmark = [
    pytest.mark.subprocess,
    pytest.mark.agent,
    pytest.mark.socket,
    pytest.mark.timeout(180),
]

PREFIX = "https://sqs.us-east-1.amazonaws.com/123456789012"


def managed_config(socket: str) -> dict[str, Any]:
    return {
        "driver": "cloud",
        "queue": "default",
        "queues": ["default"],
        "connection": {
            "driver": "sqs",
            "prefix": PREFIX,
            "suffix": "",
            "queue": "default",
            "region": "us-east-1",
            "credentials": "ecs",
        },
        "agent": {"enabled": True, "socket": socket},
    }


@dataclass
class Managed:
    emulator: AgentEmulator
    collector: LogCollector
    registry: Registry
    workers: Workers

    def enqueue(self, job: str, **kwargs: Any) -> str:
        target = self.registry.get(job)
        body = prepare_dispatch(target, (), kwargs, target.dispatch_options).message.body
        return self.emulator.enqueue(body, queue_url=f"{PREFIX}/default")

    def events(self) -> list[dict[str, Any]]:
        return [e for e in self.collector.events if isinstance(e, dict)]

    def types(self) -> list[str]:
        return [str(e.get("type", e.get("_cloud_event"))) for e in self.events()]

    def results(self) -> list[tuple[str, str]]:
        return [
            (r.body["messageId"], r.body["status"])
            for r in self.emulator.results
            if isinstance(r.body, dict) and r.completed
        ]


@pytest.fixture
def managed(run_process: Any, log_collector: LogCollector, tmp_path: Path) -> Iterator[Managed]:
    with AgentEmulator(poll_wait=0.2, visibility_timeout=2) as emulator:
        config = managed_config(emulator.socket_path)
        record = tmp_path / "records.jsonl"
        env = clean_env(
            {
                "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG": json.dumps(config),
                "LARAVEL_CLOUD_LOG_SOCKET": log_collector.socket_path,
                "LCQ_TEST_RECORD": str(record),
            }
        )
        registry = build(config=load_config(env={}, managed_config=config))
        yield Managed(emulator, log_collector, registry, Workers(run_process, env, record))


def test_success_reports_processed_and_emits_lifecycle(managed: Managed) -> None:
    message_id = managed.enqueue("ok", value=1)
    run = managed.workers.run("--max-jobs", "1", "--sleep", "0.1")
    assert run.code == 0, run.describe()
    assert managed.results() == [(message_id, "processed")]
    assert managed.types() == ["started", "processed"]
    assert validate_sequence(managed.events(), ["started", "processed"]) == []
    assert managed.events()[1]["queue"] == "default"
    assert run.failure_records == []


def test_retry_then_terminal_failure_events(managed: Managed) -> None:
    message_id = managed.enqueue("always_fails")
    run = managed.workers.run("--max-jobs", "3", "--sleep", "0.1")
    assert run.code == 0, run.describe()
    assert [r["attempt"] for r in managed.workers.records()] == [1, 2, 3]
    assert managed.results() == [
        (message_id, "released"),
        (message_id, "released"),
        (message_id, "processed"),
    ]
    expected = ["started", "released", "started", "released", "started", "failed_job", "failed"]
    assert managed.types() == expected
    assert validate_sequence(managed.events(), expected) == []
    failed_job, failed = managed.events()[-2:]
    assert validate_failed_job_event(failed_job) == []
    assert failed_job["job_name"] == "always_fails"
    assert failed_job["attempts"] == 3
    assert json.loads(failed_job["payload"])["displayName"] == "always_fails"
    assert failed["timestamp"] >= failed_job["started_at"]
    assert run.failure_records == []


def test_timeout_releases_then_redelivers_with_next_attempt(managed: Managed) -> None:
    message_id = managed.enqueue("slow_async")
    first = managed.workers.run("--max-jobs", "1", "--sleep", "0.1")
    assert first.code == 124, first.describe()
    assert managed.types() == ["started", "released"]
    assert managed.results() == []  # no release on timeout; visibility returns it
    second = managed.workers.run("--max-jobs", "1", "--sleep", "0.1")
    assert second.code == 124, second.describe()
    assert [(r["message_id"], r["attempt"]) for r in managed.workers.records()] == [
        (message_id, 1),
        (message_id, 2),
    ]


def test_terminal_timeout_emits_failed_job_then_failed_and_completes(managed: Managed) -> None:
    message_id = managed.enqueue("slow_terminal")
    run = managed.workers.run("--max-jobs", "1", "--sleep", "0.1")
    assert run.code == 124, run.describe()
    assert managed.results() == [(message_id, "processed")]
    managed.collector.wait_for(lambda events: len(events) == 3)
    assert managed.types() == ["started", "failed_job", "failed"]
    assert "timed out" in managed.events()[1]["exception_preview"]


def test_agent_5xx_on_result_exits_0_without_fetching_again(managed: Managed) -> None:
    managed.enqueue("ok")
    managed.emulator.inject("result", status(503, "unhealthy"), times=10)
    process = managed.workers.start("--sleep", "0.1")
    managed.workers.wait_for_record(lambda r: r["job"] == "ok")
    time.sleep(1)  # the /result retries run
    untouched = managed.enqueue("ok")
    run = managed.workers.wait(process, timeout=30)
    assert run.code == 0, run.describe()
    assert managed.emulator.message(untouched).receive_count == 0
    assert managed.types() == ["started", "processed"]


def test_sigterm_while_idle_exits_promptly(managed: Managed) -> None:
    """An idle agent worker never aborts its in-flight ``GET /next`` (Laravel parity); it
    exits once that poll returns. The emulator's short poll keeps the bound small here; on
    Cloud the bound is the 65 s poll timeout, within Flex's 90 s."""
    process = managed.workers.start("--sleep", "3")
    managed.workers.wait_until_idle(process)
    signalled = managed.workers.terminate(process)
    run = managed.workers.wait(process, timeout=30)
    assert run.code == 0, run.describe()
    assert time.monotonic() - signalled < 5 + managed.emulator.poll_wait


def test_conflicting_queue_is_a_startup_error(managed: Managed) -> None:
    run = managed.workers.run("--queue", "other")
    assert run.code == 2, run.describe()
    assert "conflicts" in run.result.stderr
