"""Narrow doubles for worker unit tests.

The worker is exercised end to end (``Worker.run()`` on the main thread, real signals and
timers) while core execution, policy math and event builders are replaced by small fakes
that follow their contracts. A message body is a JSON script: ``{"do": ..., "policy": ...}``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import anyio
import pytest

import laravel_cloud_queues.worker as worker_module
from laravel_cloud_queues.config import (
    AgentConfig,
    ManagedQueuesConfig,
    QueueConfig,
    RedisConfig,
    SqsConnectionConfig,
)
from laravel_cloud_queues.errors import MalformedEnvelopeError
from laravel_cloud_queues.jobs.execution import HandlerResult
from laravel_cloud_queues.jobs.policy import WorkerDefaults
from laravel_cloud_queues.transports import Backend, Delivery
from laravel_cloud_queues.worker import Worker, WorkerOptions

SQS = SqsConnectionConfig(prefix="http://sqs.test/1", region="us-east-1", credentials="default")


def config_for(mode: str, *, agent: bool = False, assigned: str = "assigned") -> QueueConfig:
    if mode == "redis":
        return QueueConfig(mode="redis", redis=RedisConfig(url="redis://127.0.0.1/15"))
    if mode == "managed":
        managed = ManagedQueuesConfig(
            connection=SQS, agent=AgentConfig(enabled=agent), queue=assigned
        )
        return QueueConfig(mode="managed", sqs=SQS, managed=managed)
    return QueueConfig(mode="sqs", sqs=SQS)


def body(do: str = "succeed", **policy: object) -> str:
    return json.dumps({"do": do, "policy": {k: v for k, v in policy.items() if v is not None}})


_ids = iter(range(1, 1_000_000))


def delivery(
    do: str = "succeed", *, attempt: int = 1, queue: str = "default", **policy: object
) -> Delivery:
    n = next(_ids)
    return Delivery(
        message_id=f"m{n}",
        queue=queue,
        body=body(do, **policy),
        attempt=attempt,
        receipt=f"secret-receipt-{n}",
        received_at=time.monotonic(),
    )


# --- core doubles --------------------------------------------------------------------------


@dataclass(frozen=True)
class FakePolicy:
    """ResolvedPolicy per its contract docstrings."""

    tries: int
    backoff: tuple[float, ...]
    timeout: float
    fail_on_timeout: bool

    def exceeded_before_run(self, attempt: int) -> bool:
        return self.tries > 0 and attempt > self.tries

    def is_last_attempt(self, attempt: int) -> bool:
        return self.tries > 0 and attempt >= self.tries

    def retry_delay(self, attempt: int) -> int:
        value = self.backoff[min(attempt, len(self.backoff)) - 1]
        return min(43_200, math.ceil(value))


@dataclass(frozen=True)
class FakePrepared:
    script: Mapping[str, Any]
    envelope: SimpleNamespace

    def policy(self, defaults: WorkerDefaults) -> FakePolicy:
        declared = self.script["policy"]
        backoff = declared.get("backoff", defaults.backoff)
        return FakePolicy(
            tries=declared.get("tries", defaults.tries),
            backoff=tuple(backoff)
            if isinstance(backoff, list)
            else (backoff,)
            if isinstance(backoff, (int, float))
            else backoff,
            timeout=declared.get("timeout", defaults.timeout),
            fail_on_timeout=declared.get("fail_on_timeout", defaults.fail_on_timeout),
        )


class FakeContext:
    def __init__(self, **fields: object) -> None:
        self.fields = fields


@dataclass
class Env:
    """Shared journal of transport calls and records, in order."""

    journal: list[tuple[str, Any]] = field(default_factory=list)
    ran: list[str] = field(default_factory=list)
    contexts: list[FakeContext] = field(default_factory=list)
    lifespan: list[str] = field(default_factory=list)

    def names(self) -> list[str]:
        return [name for name, _ in self.journal]

    def of(self, name: str) -> list[Any]:
        return [value for n, value in self.journal if n == name]


def install_core(monkeypatch: pytest.MonkeyPatch, env: Env) -> None:
    def prepare_execution(registry: object, raw: str) -> FakePrepared:
        script = json.loads(raw)
        if script["do"] == "defect":
            raise MalformedEnvelopeError("The message body is not a valid envelope.")
        return FakePrepared(script, SimpleNamespace(job="demo.job", uuid="uuid-1"))

    async def run_prepared(prepared: FakePrepared, context: FakeContext) -> HandlerResult:
        action, _, arg = prepared.script["do"].partition(":")
        env.ran.append(action)
        env.contexts.append(context)
        if action == "raise":
            return HandlerResult("error", exception=RuntimeError("boom"))
        if action == "release":
            return HandlerResult("release", delay=int(arg))
        if action == "fail":
            return HandlerResult("fail", exception=ValueError(arg) if arg else None)
        if action == "sleep":  # sync, blocks the loop on the main thread
            time.sleep(float(arg))  # noqa: ASYNC251
        if action == "spin":
            deadline = time.monotonic() + float(arg)
            while time.monotonic() < deadline:
                pass
        if action == "async-sleep":
            await anyio.sleep(float(arg))
        if action == "sigterm":
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)  # noqa: ASYNC251
        return HandlerResult("success")

    def lifecycle_event(
        type_: str, queue: str, *, timestamp: datetime, duration_ms: int | None = None
    ) -> dict[str, object]:
        event: dict[str, object] = {"_cloud_event": "queue", "type": type_, "queue": queue}
        event["timestamp"] = timestamp
        if duration_ms is not None:
            event["duration_ms"] = duration_ms
        return event

    def failed_job_event(**fields: Any) -> dict[str, object]:
        return {"_cloud_event": "failed_job", **fields}

    def failure_log_record(**fields: Any) -> dict[str, object]:
        return {"laravel_cloud_queues": "failed_job", **fields}

    monkeypatch.setattr(worker_module, "prepare_execution", prepare_execution)
    monkeypatch.setattr(worker_module, "run_prepared", run_prepared)
    monkeypatch.setattr(worker_module, "JobContext", FakeContext)
    monkeypatch.setattr(worker_module, "lifecycle_event", lifecycle_event)
    monkeypatch.setattr(worker_module, "failed_job_event", failed_job_event)
    monkeypatch.setattr(worker_module, "failure_log_record", failure_log_record)


# --- transport / telemetry / registry doubles ----------------------------------------------


class FakeConsumer:
    def __init__(
        self,
        env: Env,
        items: Sequence[Delivery | BaseException | Callable[[], Delivery | None] | None],
        *,
        renewal: bool = False,
        complete_error: BaseException | None = None,
        release_error: BaseException | None = None,
        renew_error: BaseException | None = None,
    ) -> None:
        self.env = env
        self.items = list(items)
        self.supports_renewal = renewal
        self.complete_error = complete_error
        self.release_error = release_error
        self.renew_error = renew_error
        self.receives: list[tuple[tuple[str, ...], float]] = []
        self.interrupted = 0
        self.closed = False

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        self.receives.append((tuple(queues), wait_seconds))
        if not self.items:
            return None
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return item()
        return item

    def complete(self, delivery: Delivery) -> None:
        self.env.journal.append(("complete", delivery.message_id))
        if self.complete_error is not None:
            raise self.complete_error

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        self.env.journal.append(("release", (delivery.message_id, delay_seconds)))
        if self.release_error is not None:
            raise self.release_error

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        self.env.journal.append(("renew", (delivery.message_id, lease_seconds)))
        if self.renew_error is not None:
            raise self.renew_error

    def interrupt(self) -> None:
        self.interrupted += 1

    def close(self) -> None:
        self.closed = True


class FakeTelemetry:
    """Telemetry per its contract: ``emit`` is a no-op unless ``emits_cloud_events``."""

    def __init__(self, env: Env, emits_cloud_events: bool) -> None:
        self.env = env
        self.emits = emits_cloud_events
        self.lock_timeouts: list[float | None] = []

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        self.lock_timeouts.append(lock_timeout)
        if self.emits:
            self.env.journal.append(("event", dict(event)))

    def log_line(
        self,
        record: Mapping[str, object],
        *,
        message: str,
        level: int = logging.INFO,
        exception: BaseException | None = None,
        lock_timeout: float | None = None,
    ) -> None:
        self.lock_timeouts.append(lock_timeout)
        self.env.journal.append(("line", dict(record)))


class FakeRegistry:
    def __init__(
        self, config: QueueConfig | Exception, telemetry: FakeTelemetry, consumer: FakeConsumer
    ) -> None:
        self._config = config
        self.telemetry = telemetry
        self.loaded = 0
        self.leases: list[int] = []

        def factory(*, lease_seconds: int = 60) -> FakeConsumer:
            self.leases.append(lease_seconds)
            return consumer

        self.backend = Backend(mode="sqs", producer=None, consumer_factory=factory)  # type: ignore[arg-type]

    @property
    def registry(self) -> FakeRegistry:
        return self

    @property
    def config(self) -> QueueConfig:
        if isinstance(self._config, Exception):
            raise self._config
        return self._config

    def load(self) -> None:
        self.loaded += 1


class FakeTarget:
    def __init__(self, registry: FakeRegistry, env: Env) -> None:
        self.registry = registry
        self.env = env

    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[None]:
        self.env.lifespan.append("enter")
        try:
            yield
        finally:
            self.env.lifespan.append("exit")


@dataclass
class Harness:
    env: Env
    consumer: FakeConsumer
    telemetry: FakeTelemetry
    registry: FakeRegistry
    worker: Worker

    def run(self) -> int:
        return self.worker.run()

    def events(self) -> list[dict[str, Any]]:
        return self.env.of("event")

    def lines(self) -> list[dict[str, Any]]:
        return self.env.of("line")

    def job_lines(self) -> list[dict[str, Any]]:
        return [line for line in self.lines() if line.get("laravel_cloud_queues") == "job"]


class Exited(BaseException):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


@pytest.fixture
def make(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., Harness]]:
    env = Env()
    install_core(monkeypatch, env)

    def fake_exit(code: int) -> None:
        raise Exited(code)

    monkeypatch.setattr(os, "_exit", fake_exit)

    def build(
        items: Sequence[Any] = (),
        *,
        mode: str = "sqs",
        agent: bool = False,
        config: QueueConfig | Exception | None = None,
        renewal: bool = False,
        complete_error: BaseException | None = None,
        release_error: BaseException | None = None,
        renew_error: BaseException | None = None,
        **options: Any,
    ) -> Harness:
        resolved = config if config is not None else config_for(mode, agent=agent)
        emits = not isinstance(resolved, Exception) and resolved.emits_cloud_events
        consumer = FakeConsumer(
            env,
            items,
            renewal=renewal,
            complete_error=complete_error,
            release_error=release_error,
            renew_error=renew_error,
        )
        telemetry = FakeTelemetry(env, emits)
        registry = FakeRegistry(resolved, telemetry, consumer)
        options.setdefault("stop_when_empty", True)
        worker = Worker(FakeTarget(registry, env), WorkerOptions(**options))  # type: ignore[arg-type]
        return Harness(env, consumer, telemetry, registry, worker)

    yield build
    signal.setitimer(signal.ITIMER_REAL, 0)
