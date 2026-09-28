"""Real worker subprocesses against local Valkey/Redis, ``redis`` mode (Laravel
``RedisQueue`` semantics, D6; reservation expiry = lease, D13.8)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from harness.log_collector import LogCollector
from harness.redis import connect
from laravel_cloud_queues.config import load_config
from laravel_cloud_queues.registry import Registry
from tests.integration.worker.apps.basic import build
from tests.integration.worker.support import Workers, clean_env

pytestmark = [pytest.mark.subprocess, pytest.mark.redis, pytest.mark.timeout(180)]


@dataclass
class Valkey:
    url: str
    prefix: str
    registry: Registry
    workers: Workers
    collector: LogCollector

    def dispatch(self, job: str, **kwargs: Any) -> str:
        return self.registry.get(job).dispatch(**kwargs).message_id

    def keys(self) -> list[bytes]:
        client = connect(self.url)
        try:
            return [
                key
                for key in client.scan_iter(match=self.prefix + "*")
                if client.type(key) != b"none" and _size(client, key) > 0
            ]
        finally:
            client.close()


def _size(client: Any, key: bytes) -> int:
    kind = client.type(key)
    if kind == b"list":
        return int(client.llen(key))
    if kind == b"zset":
        return int(client.zcard(key))
    return 1


@pytest.fixture
def valkey(
    redis_url: str,
    redis_prefix: str,
    run_process: Any,
    log_collector: LogCollector,
    tmp_path: Path,
) -> Iterator[Valkey]:
    settings = {
        "LARAVEL_CLOUD_QUEUES_BACKEND": "redis",
        "LARAVEL_CLOUD_QUEUES_REDIS_URL": redis_url,
        "LARAVEL_CLOUD_QUEUES_REDIS_PREFIX": redis_prefix,
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
        yield Valkey(
            redis_url, redis_prefix, registry, Workers(run_process, env, record), log_collector
        )
    finally:
        registry.backend.producer.close()


def test_success(valkey: Valkey) -> None:
    message_id = valkey.dispatch("ok", value=3)
    run = valkey.workers.run("--max-jobs", "1", "--sleep", "1")
    assert run.code == 0, run.describe()
    [record] = valkey.workers.records()
    assert (record["message_id"], record["attempt"], record["value"]) == (message_id, 1, 3)
    assert run.statuses == ["processed"]
    assert valkey.keys() == []
    assert valkey.collector.raw_lines == []


def test_retry_releases_the_same_job_until_exhausted(valkey: Valkey) -> None:
    message_id = valkey.dispatch("always_fails")
    run = valkey.workers.run("--max-jobs", "3", "--sleep", "1")
    assert run.code == 0, run.describe()
    records = valkey.workers.records()
    assert [(r["message_id"], r["attempt"]) for r in records] == [
        (message_id, 1),
        (message_id, 2),
        (message_id, 3),
    ]
    assert run.statuses == ["released", "released", "failed"]
    assert len(run.failure_records) == 1
    assert valkey.keys() == []
    assert valkey.collector.raw_lines == []


def test_default_tries_is_one(valkey: Valkey) -> None:
    valkey.dispatch("fails_default")
    run = valkey.workers.run("--max-jobs", "1", "--sleep", "1")
    assert run.statuses == ["failed"]
    assert len(run.failure_records) == 1


def test_retryable_timeout_redelivers_after_reservation_expiry(valkey: Valkey) -> None:
    message_id = valkey.dispatch("slow_loop")
    first = valkey.workers.run("--max-jobs", "1", "--sleep", "1", lease=2)
    assert first.code == 124, first.describe()
    assert first.statuses == ["released"]
    second = valkey.workers.run("--max-jobs", "1", "--sleep", "1", lease=2)
    assert second.code == 124, second.describe()
    assert [(r["message_id"], r["attempt"]) for r in valkey.workers.records()] == [
        (message_id, 1),
        (message_id, 2),
    ]


def test_timeout_on_last_attempt_fails(valkey: Valkey) -> None:
    valkey.dispatch("slow_terminal")
    run = valkey.workers.run("--max-jobs", "1", "--sleep", "1")
    assert run.code == 124, run.describe()
    assert run.statuses == ["failed"]
    assert len(run.failure_records) == 1
    assert valkey.keys() == []


def test_sigterm_mid_job_finishes_and_reports(valkey: Valkey) -> None:
    valkey.dispatch("sleepy", seconds=2)
    process = valkey.workers.start("--sleep", "1")
    valkey.workers.wait_for_record(lambda r: r.get("phase") == "start")
    valkey.workers.terminate(process)
    run = valkey.workers.wait(process, timeout=30)
    assert run.code == 0, run.describe()
    assert run.statuses == ["processed"]
    assert valkey.keys() == []


def test_sigterm_while_idle_exits_promptly(valkey: Valkey) -> None:
    process = valkey.workers.start("--sleep", "3")
    valkey.workers.wait_until_idle(process)
    signalled = valkey.workers.terminate(process)
    run = valkey.workers.wait(process, timeout=30)
    assert run.code == 0, run.describe()
    assert time.monotonic() - signalled < 5
