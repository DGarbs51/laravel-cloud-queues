"""Separate producer/worker proofs using the same moto/LocalStack and Valkey fixtures.

Laravel: Queue/SqsQueue.php:551-635,644-657; Queue/Jobs/SqsJob.php:66-86,132-135;
Queue/RedisQueue.php:315-329; Queue/LuaScripts.php:75-106,123-165 (v13.33.0).
"""

from __future__ import annotations

import json
import sys
import time

import pytest

from tests.conformance import ROOT

from .support import capture, environment, produce, rows, sqs_env, until, worker

pytestmark = pytest.mark.subprocess


@pytest.mark.sqs
@pytest.mark.parametrize("job", ["demo.sync", "demo.async", "demo.retry", "demo.default_failure"])
@pytest.mark.conformance("dispatch.standard", tier="emulated")
def test_sqs_processes(run_process, artifacts, sqs_endpoint, log_collector, evidence, job):
    url = sqs_endpoint.create_queue()
    env = sqs_env(artifacts, sqs_endpoint, url)
    # Object-storage credentials/endpoints must never select or configure SQS (D6a).
    env.update(
        AWS_ACCESS_KEY_ID="wrong-storage-key",
        AWS_SECRET_ACCESS_KEY="wrong-storage-secret",
        AWS_ENDPOINT_URL="http://127.0.0.1:1",
        AWS_ENDPOINT_URL_SQS="http://127.0.0.1:1",
        AWS_REGION="wrong-region",
        LARAVEL_CLOUD_LOG_SOCKET=log_collector.socket_path,
    )
    sent = produce(run_process, artifacts, env, job)
    count = 2 if job == "demo.retry" else 1
    result = worker(run_process, artifacts, env, "--max-jobs", str(count)).wait(25)
    capture(evidence, artifacts, log_collector)
    assert result.returncode == 0, result.stderr
    handlers = rows(artifacts, "handler")
    assert [row["attempt"] for row in handlers] == list(range(1, count + 1))
    assert {row["message_id"] for row in handlers} == {sent["message_id"]}
    assert {row["pid"] for row in handlers} != {sent["producer_pid"]}
    assert not log_collector.events
    assert not sqs_endpoint.client.receive_message(QueueUrl=url).get("Messages")
    if job == "demo.default_failure":
        records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        assert any(
            "exception" in record["context"] and "payload" in record["context"]
            for record in records
        )
    evidence.record("producer_pid", sent["producer_pid"])
    evidence.record("original_message_id", sent["message_id"])
    evidence.record("retried_message_ids", [row["message_id"] for row in handlers[1:]])


@pytest.mark.sqs
@pytest.mark.parametrize("kind", ["named", "override", "delay", "fifo", "fifo-explicit", "fair"])
@pytest.mark.conformance("dispatch.named_queue", tier="emulated")
def test_sqs_options(run_process, artifacts, sqs_endpoint, evidence, kind):
    url = sqs_endpoint.create_queue(fifo=kind.startswith("fifo"))
    env = sqs_env(artifacts, sqs_endpoint, url)
    job = "demo.named" if kind in {"named", "override"} else "demo.sync"
    options = {}
    if kind == "named":
        # Suffix the declared logical name to this fixture's unique physical queue.
        # A full URL override is tested separately, so use a named logical queue here.
        env["LARAVEL_CLOUD_QUEUES_SQS_PREFIX"] = url.rsplit("/", 1)[0]
        # harness owns cleanup: its generated queue is used as the named job default
        # in a separate producer process via the documented demo setting.
        env["LCQ_DEMO_NAMED_QUEUE"] = url.rsplit("/", 1)[1]
    elif kind == "override":
        options["queue"] = url
    elif kind == "delay":
        options["delay"] = 2
    elif kind == "fifo-explicit":
        options.update(group="tenant-1", deduplication_id="dedup-1")
    elif kind == "fair":
        options["message_group"] = "tenant-1"
    before = time.monotonic()
    sent = produce(run_process, artifacts, env, job, options=options)
    if kind == "delay":
        assert not sqs_endpoint.client.receive_message(QueueUrl=url).get("Messages")
        time.sleep(max(0, before + 2.2 - time.monotonic()))
    received = sqs_endpoint.client.receive_message(
        QueueUrl=url, AttributeNames=["All"], WaitTimeSeconds=2
    )["Messages"][0]
    assert received["MessageId"] == sent["message_id"]
    attributes = received["Attributes"]
    if kind.startswith("fifo"):
        expected_group = "tenant-1" if kind == "fifo-explicit" else url.rsplit("/", 1)[1]
        assert attributes["MessageGroupId"] == expected_group
        assert attributes["MessageDeduplicationId"]
        if kind == "fifo-explicit":
            assert attributes["MessageDeduplicationId"] == "dedup-1"
    elif kind == "fair":
        assert attributes["MessageGroupId"] == "tenant-1"
    evidence.record("message_id", received["MessageId"])
    evidence.record("attributes", attributes)
    evidence.record("queue", sent["queue"])
    evidence.observed("Emulated SQS attributes; no claim of live server-side fairness or timing")


@pytest.mark.redis
@pytest.mark.parametrize("job", ["demo.sync", "demo.retry", "demo.default_failure", "demo.timeout"])
@pytest.mark.conformance("redis.transport", tier="emulated")
def test_redis_processes(
    run_process, artifacts, redis_url, redis_prefix, log_collector, evidence, job
):
    env = environment(artifacts)
    env.update(
        LARAVEL_CLOUD_QUEUES_BACKEND="redis",
        LARAVEL_CLOUD_QUEUES_REDIS_URL=redis_url,
        LARAVEL_CLOUD_QUEUES_REDIS_PREFIX=redis_prefix,
        LARAVEL_CLOUD_LOG_SOCKET=log_collector.socket_path,
    )
    sent = produce(run_process, artifacts, env, job, options={"delay": 1})
    count = 2 if job == "demo.retry" else 1
    result = worker(run_process, artifacts, env, "--max-jobs", str(count)).wait(20)
    capture(evidence, artifacts, log_collector)
    assert result.returncode == (124 if job == "demo.timeout" else 0), result.stderr
    handlers = rows(artifacts, "handler")
    assert [row["attempt"] for row in handlers] == list(range(1, count + 1))
    assert {row["message_id"] for row in handlers} == {sent["message_id"]}
    assert handlers[0]["pid"] != sent["producer_pid"]
    assert not log_collector.events
    if job == "demo.default_failure":
        assert any(
            "exception" in json.loads(line)["context"]
            for line in result.stdout.splitlines()
            if line.startswith("{")
        )
    evidence.record("original_message_id", sent["message_id"])
    evidence.record("exit_code", result.returncode)


@pytest.mark.sqs
@pytest.mark.conformance("worker.multi_queue_priority", tier="emulated")
def test_priority(run_process, artifacts, sqs_endpoint, evidence):
    high, low = sqs_endpoint.create_queue(), sqs_endpoint.create_queue()
    env = sqs_env(artifacts, sqs_endpoint, high)
    low_sent = produce(run_process, artifacts, env, options={"queue": low})
    high_sent = produce(run_process, artifacts, env, options={"queue": high})
    result = worker(
        run_process, artifacts, env, "--queue", f"{high},{low}", "--max-jobs", "2"
    ).wait(20)
    capture(evidence, artifacts)
    assert result.returncode == 0, result.stderr
    assert [row["message_id"] for row in rows(artifacts, "handler")] == [
        high_sent["message_id"],
        low_sent["message_id"],
    ]


@pytest.mark.sqs
@pytest.mark.conformance("sqs.visibility_renewal", tier="emulated")
def test_visibility_renewal(run_process, artifacts, sqs_endpoint, evidence):
    url = sqs_endpoint.create_queue()
    env = sqs_env(artifacts, sqs_endpoint, url)
    sent = produce(
        run_process, artifacts, env, "demo.slow", kwargs={"label": "lease", "seconds": 3}
    )
    # Public WorkerOptions exposes a short lease for a bounded process proof.
    code = (
        "from laravel_cloud_queues.worker import Worker, WorkerOptions, resolve_target; "
        "raise SystemExit(Worker(resolve_target('tests.conformance.app:app'), "
        "WorkerOptions(max_jobs=1, lease_seconds=1)).run())"
    )
    process = run_process([sys.executable, "-c", code], env=env, cwd=ROOT, output_dir=artifacts)
    until(lambda: bool(rows(artifacts, "handler")))
    time.sleep(1.5)
    assert not sqs_endpoint.client.receive_message(QueueUrl=url).get("Messages")
    result = process.wait(10)
    capture(evidence, artifacts)
    assert result.returncode == 0, result.stderr
    assert rows(artifacts, "handler")[0]["message_id"] == sent["message_id"]


@pytest.mark.sqs
@pytest.mark.conformance("worker.ambiguous_ack_stops", tier="emulated")
def test_ambiguous_direct_ack(run_process, artifacts, sqs_endpoint, evidence):
    url = sqs_endpoint.create_queue(fifo=True)
    env = sqs_env(artifacts, sqs_endpoint, url)
    first = produce(run_process, artifacts, env)
    second = produce(run_process, artifacts, env)
    process = run_process(
        [sys.executable, "-m", "tests.conformance.fault_worker"],
        env=env,
        cwd=ROOT,
        output_dir=artifacts,
    )
    result = process.wait(15)
    capture(evidence, artifacts)
    assert result.returncode == 1, result.stderr
    assert [row["message_id"] for row in rows(artifacts, "handler")] == [first["message_id"]]
    remaining = sqs_endpoint.client.receive_message(QueueUrl=url)["Messages"]
    assert [message["MessageId"] for message in remaining] == [second["message_id"]]
    evidence.record("exit_code", result.returncode)
    evidence.observed(
        "Injected lost response after real DeleteMessage; second message was not fetched"
    )
