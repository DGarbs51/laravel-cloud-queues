"""``registry.testing()``: recorder and eager execution (PROJECT_SCOPE.md §20)."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

import anyio
import anyio.lowlevel
import pytest
from tests.unit.jobs.fakes import make_registry

from laravel_cloud_queues import JobContext, Registry, current_job
from laravel_cloud_queues import registry as registry_module
from laravel_cloud_queues.config import QueueConfig, RedisConfig
from laravel_cloud_queues.errors import (
    ArgumentError,
    InvalidQueueOptionError,
    JobFailedError,
    PayloadTooLargeError,
)
from laravel_cloud_queues.testing import DispatchRecorder, RecordedDispatch


@pytest.fixture
def registry(monkeypatch: pytest.MonkeyPatch) -> Registry:
    """A registry without configuration: testing must never load config or build a backend."""

    def forbidden(*_: object, **__: object) -> Any:
        raise AssertionError("testing mode touched configuration or the backend")

    monkeypatch.setattr(registry_module, "load_config", forbidden)
    monkeypatch.setattr(registry_module, "create_backend", forbidden)
    return Registry()


def test_records_every_dispatch_without_running(registry: Registry) -> None:
    calls: list[int] = []

    @registry.job(name="emails.send")
    def send(user_id: int, template: str = "welcome") -> None:
        calls.append(user_id)

    other = registry.job(name="other", queue="low")(lambda: None)

    with registry.testing(eager=False) as recorder:
        receipt = send.options(delay=1.5).dispatch(1, template="reset")
        other.dispatch()

    assert calls == []
    assert isinstance(recorder, DispatchRecorder)
    first, second = recorder.dispatched
    assert first == RecordedDispatch(
        job_name="emails.send",
        queue="default",
        body=first.body,
        args=(1,),
        kwargs={"template": "reset"},
        delay_seconds=2,
    )
    body = json.loads(first.body)
    assert body["uuid"] == receipt.uuid
    assert body["displayName"] == "emails.send"
    assert receipt.queue == "default"
    assert receipt.message_id
    assert second.queue == "low"
    assert recorder.for_job("other") == [second]


def test_records_fifo_and_fair_options(registry: Registry) -> None:
    job = registry.job(name="j")(lambda: None)
    with registry.testing(eager=False) as recorder:
        job.options(queue="orders.fifo", group="g", deduplication_id="d").dispatch()
        job.options(queue="emails", message_group="tenant").dispatch()
    fifo, fair = recorder.dispatched
    assert (fifo.fifo_group, fifo.deduplication_id, fifo.message_group) == ("g", "d", None)
    assert (fair.fifo_group, fair.deduplication_id, fair.message_group) == (None, None, "tenant")


def test_dispatch_validation_still_applies(registry: Registry) -> None:
    job = registry.job(name="j")(lambda x: None)
    with registry.testing() as recorder:
        with pytest.raises(InvalidQueueOptionError):
            job.options(queue="q.fifo", delay=5).dispatch(1)
        with pytest.raises(ArgumentError):
            job.dispatch()
        with pytest.raises(PayloadTooLargeError):  # SQS rules unless configured otherwise
            job.dispatch("x" * 1_100_000)
    assert recorder.dispatched == []


def test_follows_configured_redis_mode() -> None:
    registry = Registry(
        config=QueueConfig(mode="redis", redis=RedisConfig(url="redis://x", queue="rq"))
    )
    job = registry.job(name="j")(lambda x: None)
    with registry.testing(eager=False) as recorder:
        with pytest.raises(InvalidQueueOptionError):
            job.options(group="g").dispatch(1)
        job.dispatch("x" * 1_100_000)
    assert recorder.dispatched[0].queue == "rq"


def test_uses_given_backend_capabilities() -> None:
    registry, producer, telemetry = make_registry(mode="redis")
    with registry.testing(eager=False) as recorder, pytest.raises(InvalidQueueOptionError):
        registry.job(name="j")(lambda: None).options(message_group="t").dispatch()
    assert recorder.dispatched == []
    assert producer.sent == []
    assert telemetry.events == []


def test_session_is_restored_on_exit() -> None:
    registry, producer, _ = make_registry()
    job = registry.job(name="j")(lambda: None)
    with registry.testing(eager=False) as outer:
        with registry.testing(eager=False) as inner:
            job.dispatch()
        job.dispatch()
    job.dispatch()
    assert (len(inner.dispatched), len(outer.dispatched), len(producer.sent)) == (1, 1, 1)


# --- eager ----------------------------------------------------------------------------


def test_eager_runs_sync_handler_outside_a_loop(registry: Registry) -> None:
    seen: list[object] = []

    @registry.job(name="j", tries=3)
    def handler(x: int, context: JobContext) -> None:
        seen.extend(
            [x, context.attempt, context.max_tries, context.queue, current_job() is context]
        )

    with registry.testing() as recorder:
        receipt = handler.dispatch(5)
    assert seen == [5, 1, 3, "default", True]
    assert len(recorder.dispatched) == 1
    assert receipt.uuid == json.loads(recorder.dispatched[0].body)["uuid"]


def test_eager_runs_async_handler_outside_a_loop(registry: Registry) -> None:
    seen: list[object] = []

    @registry.job(name="j")
    async def handler(x: int, context: JobContext) -> None:
        await anyio.lowlevel.checkpoint()
        seen.extend([x, context.max_tries])

    with registry.testing():
        handler.dispatch(9)
    assert seen == [9, 1]


def test_eager_arguments_round_trip_through_the_payload(registry: Registry) -> None:
    seen: list[object] = []

    @registry.job(name="j")
    def handler(items: tuple[int, ...]) -> None:
        seen.append(items)

    with registry.testing():
        handler.dispatch((1, 2))
    assert seen == [(1, 2)]


def test_eager_reraises_handler_exceptions(registry: Registry) -> None:
    @registry.job(name="j", tries=5)
    def handler() -> None:
        raise ValueError("boom")

    with registry.testing() as recorder, pytest.raises(ValueError, match="boom"):
        handler.dispatch()
    assert len(recorder.dispatched) == 1


def test_eager_explicit_fail_raises(registry: Registry) -> None:
    reason = LookupError("gone")
    fail_plain = registry.job(name="a")(lambda: current_job().fail())
    fail_exc = registry.job(name="b")(lambda: current_job().fail(reason))

    with registry.testing():
        with pytest.raises(JobFailedError, match=r"Job failed explicitly\."):
            fail_plain.dispatch()
        with pytest.raises(LookupError) as info:
            fail_exc.dispatch()
    assert info.value is reason


def test_eager_release_is_recorded_not_rerun(registry: Registry) -> None:
    calls: list[int] = []

    @registry.job(name="j")
    def handler(context: JobContext) -> None:
        calls.append(context.attempt)
        context.release(30)

    with registry.testing() as recorder:
        handler.dispatch()
    assert calls == [1]
    assert len(recorder.dispatched) == 1


def test_eager_dispatch_async_awaits_in_the_running_loop(registry: Registry) -> None:
    seen: list[object] = []

    @registry.job(name="j")
    async def handler(x: int) -> None:
        seen.extend([x, asyncio.get_running_loop(), threading.get_ident()])

    async def main() -> None:
        with registry.testing():
            await handler.dispatch_async(1)
        assert seen == [1, asyncio.get_running_loop(), threading.get_ident()]

    anyio.run(main)


def test_eager_dispatch_async_reraises(registry: Registry) -> None:
    @registry.job(name="j")
    async def handler() -> None:
        raise RuntimeError("async boom")

    async def main() -> None:
        with registry.testing(), pytest.raises(RuntimeError, match="async boom"):
            await handler.dispatch_async()

    anyio.run(main)


def test_eager_sync_dispatch_inside_a_running_loop_uses_a_helper_thread(
    registry: Registry,
) -> None:
    seen: list[object] = []

    @registry.job(name="j")
    async def handler(x: int) -> None:
        await anyio.lowlevel.checkpoint()
        seen.extend([x, asyncio.get_running_loop(), threading.get_ident()])

    async def main() -> None:
        with registry.testing():
            handler.dispatch(2)  # no nested loop: runs to completion on a helper thread
        assert seen[0] == 2
        assert seen[1] is not asyncio.get_running_loop()
        assert seen[2] != threading.get_ident()

    anyio.run(main)


def test_eager_sync_dispatch_inside_a_running_loop_reraises(registry: Registry) -> None:
    job = registry.job(name="j")(lambda: 1 / 0)

    async def main() -> None:
        with registry.testing(), pytest.raises(ZeroDivisionError):
            job.dispatch()

    anyio.run(main)


def test_eager_nested_dispatch(registry: Registry) -> None:
    order: list[str] = []

    @registry.job(name="child")
    def child(n: int) -> None:
        order.append(f"child {n}")

    @registry.job(name="parent")
    async def parent() -> None:
        order.append("parent start")
        child.dispatch(1)
        await child.dispatch_async(2)
        order.append("parent end")

    with registry.testing() as recorder:
        parent.dispatch()
    assert order == ["parent start", "child 1", "child 2", "parent end"]
    assert [d.job_name for d in recorder.dispatched] == ["parent", "child", "child"]
