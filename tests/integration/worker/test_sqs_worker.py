"""Real worker subprocesses against SQS (moto locally, LocalStack in CI), ``sqs`` mode.

Timeouts follow D2 (Laravel ``Queue/Worker.php:319-356`` and
``Foundation/Cloud/QueueConnector.php:101-134``: no release on timeout, exit 124, the message
returns through visibility with an incremented receive count). Retries follow
``Queue/Worker.php:648-687`` (release the same message) and the pre-run check ``:701-718``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from laravel_cloud_queues.config import load_config
from laravel_cloud_queues.registry import Registry
from tests.harness.log_collector import LogCollector
from tests.harness.sqs import SQSEndpoint
from tests.integration.worker.apps.basic import build
from tests.integration.worker.support import Workers, clean_env, native_iterations

pytestmark = [pytest.mark.subprocess, pytest.mark.sqs, pytest.mark.timeout(180)]


@dataclass
class Sqs:
    endpoint: SQSEndpoint
    queue_url: str
    queue: str
    registry: Registry
    workers: Workers
    collector: LogCollector

    def dispatch(self, job: str, **kwargs: Any) -> str:
        return self.registry.get(job).dispatch(**kwargs).message_id

    def depth(self) -> int:
        attributes = self.endpoint.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        return sum(int(value) for value in attributes.values())


@pytest.fixture
def sqs(
    sqs_endpoint: SQSEndpoint, run_process: Any, log_collector: LogCollector, tmp_path: Path
) -> Iterator[Sqs]:
    queue_url = sqs_endpoint.create_queue()
    prefix, queue = queue_url.rsplit("/", 1)
    settings = {
        "LARAVEL_CLOUD_QUEUES_BACKEND": "sqs",
        "LARAVEL_CLOUD_QUEUES_SQS_PREFIX": prefix,
        "LARAVEL_CLOUD_QUEUES_SQS_QUEUE": queue,
        "LARAVEL_CLOUD_QUEUES_SQS_REGION": sqs_endpoint.region,
        "LARAVEL_CLOUD_QUEUES_SQS_KEY": sqs_endpoint.access_key,
        "LARAVEL_CLOUD_QUEUES_SQS_SECRET": sqs_endpoint.secret_key,
        "LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT": sqs_endpoint.url,
    }
    record = tmp_path / "records.jsonl"
    env = clean_env(
        {
            **settings,
            "LARAVEL_CLOUD_LOG_SOCKET": log_collector.socket_path,
            "LCQ_TEST_RECORD": str(record),
        }
    )
    registry = build(config=load_config(env=settings))
    try:
        yield Sqs(
            sqs_endpoint,
            queue_url,
            queue,
            registry,
            Workers(run_process, env, record),
            log_collector,
        )
    finally:
        registry.backend.producer.close()


def test_success_is_deleted_and_logged_without_socket_events(sqs: Sqs) -> None:
    message_id = sqs.dispatch("ok", value=5)
    run = sqs.workers.run("--max-jobs", "1")
    assert run.code == 0, run.describe()
    [record] = sqs.workers.records()
    assert record["message_id"] == message_id
    assert record["attempt"] == 1
    assert record["value"] == 5
    [line] = run.job_lines
    assert line["status"] == "processed"
    assert line["job"] == "ok"
    assert line["queue"] == sqs.queue
    assert line["message_id"] == message_id
    assert sqs.depth() == 0
    assert sqs.collector.raw_lines == []


def test_retry_releases_the_same_message_until_exhausted(sqs: Sqs) -> None:
    message_id = sqs.dispatch("always_fails")
    run = sqs.workers.run("--max-jobs", "3")
    assert run.code == 0, run.describe()
    records = sqs.workers.records()
    assert [r["message_id"] for r in records] == [message_id] * 3
    assert [r["attempt"] for r in records] == [1, 2, 3]
    assert run.statuses == ["released", "released", "failed"]
    [failure] = run.failure_records
    assert "always fails" in str(failure)
    assert sqs.depth() == 0
    assert sqs.collector.raw_lines == []


def test_default_tries_is_one(sqs: Sqs) -> None:
    sqs.dispatch("fails_default")
    run = sqs.workers.run("--max-jobs", "1")
    assert run.statuses == ["failed"]
    assert len(run.failure_records) == 1
    assert sqs.depth() == 0


def test_two_sequential_jobs_get_isolated_contexts(sqs: Sqs) -> None:
    first, second = sqs.dispatch("ok", value=1), sqs.dispatch("sync_ok")
    run = sqs.workers.run("--max-jobs", "2")
    assert run.code == 0, run.describe()
    records = sqs.workers.records()
    assert {r["message_id"] for r in records} == {first, second}
    assert all(r["current_is_context"] for r in records)
    assert len({r["pid"] for r in records}) == 1


@pytest.mark.parametrize("job", ["slow_async", "slow_loop", "slow_native"])
def test_retryable_timeout_exits_124_and_redelivers_with_next_attempt(sqs: Sqs, job: str) -> None:
    kwargs = {"n": native_iterations(1.5)} if job == "slow_native" else {}
    message_id = sqs.dispatch(job, **kwargs)
    first = sqs.workers.run("--max-jobs", "1", lease=2)
    assert first.code == 124, first.describe()
    assert first.statuses == ["released"]
    assert first.failure_records == []
    if job == "slow_native":
        # Native code overruns the 0.5 s timeout until control returns.
        assert first.job_lines[0]["duration_ms"] >= 1000
    else:
        assert first.job_lines[0]["duration_ms"] < 1500
    second = sqs.workers.run("--max-jobs", "1", lease=2)
    assert second.code == 124, second.describe()
    attempts = [(r["message_id"], r["attempt"]) for r in sqs.workers.records() if "phase" not in r]
    assert attempts == [(message_id, 1), (message_id, 2)]
    assert sqs.collector.raw_lines == []


def test_timeout_on_last_attempt_fails_and_deletes(sqs: Sqs) -> None:
    sqs.dispatch("slow_terminal")
    run = sqs.workers.run("--max-jobs", "1")
    assert run.code == 124, run.describe()
    assert run.statuses == ["failed"]
    [failure] = run.failure_records
    assert "timed out" in str(failure)
    assert sqs.depth() == 0


def test_fail_on_timeout_fails_with_attempts_left(sqs: Sqs) -> None:
    sqs.dispatch("slow_fail_on_timeout")
    run = sqs.workers.run("--max-jobs", "1")
    assert run.code == 124, run.describe()
    assert run.statuses == ["failed"]
    assert sqs.depth() == 0


def test_sigterm_mid_job_finishes_reports_and_exits_0(sqs: Sqs) -> None:
    sqs.dispatch("sleepy", seconds=2)
    process = sqs.workers.start()
    sqs.workers.wait_for_record(lambda r: r.get("phase") == "start")
    sqs.workers.terminate(process)
    run = sqs.workers.wait(process, timeout=30)
    assert run.code == 0, run.describe()
    assert [r["phase"] for r in sqs.workers.records()] == ["start", "end"]
    assert run.statuses == ["processed"]
    assert sqs.depth() == 0


def test_sigterm_while_idle_waits_at_most_for_the_current_long_poll(sqs: Sqs) -> None:
    """An in-flight ReceiveMessage is not aborted (it may dequeue a message): <= 20 s poll."""
    process = sqs.workers.start()
    sqs.workers.wait_until_idle(process)
    signalled = sqs.workers.terminate(process)
    run = sqs.workers.wait(process, timeout=40)
    assert run.code == 0, run.describe()
    assert time.monotonic() - signalled <= 25


def test_lost_lease_reports_nothing_and_exits_1(sqs: Sqs) -> None:
    """FINDINGS.md: a GIL-holding call starves renewal; another consumer takes the message."""
    message_id = sqs.dispatch("native_block", n=native_iterations(4))
    process = sqs.workers.start("--max-jobs", "1", lease=1)
    sqs.workers.wait_for_record(lambda r: r.get("phase") == "start")
    taken = None
    deadline = time.monotonic() + 3.5
    while taken is None and time.monotonic() < deadline:
        messages = sqs.endpoint.client.receive_message(
            QueueUrl=sqs.queue_url, VisibilityTimeout=30, WaitTimeSeconds=1
        ).get("Messages", [])
        taken = messages[0] if messages else None
    assert taken is not None and taken["MessageId"] == message_id
    # Another worker finished it: the original lease is gone for good.
    sqs.endpoint.client.delete_message(QueueUrl=sqs.queue_url, ReceiptHandle=taken["ReceiptHandle"])
    run = sqs.workers.wait(process, timeout=30)
    assert run.code == 1, run.describe()
    assert run.statuses == []
    assert [r["phase"] for r in sqs.workers.records()] == ["start", "end"]
