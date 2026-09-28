"""Shared execution path: prepare_execution defects and run_prepared outcome mapping
(PROJECT_SCOPE.md §8 trust boundary, §12, §16; docs/contract/worker.md step 5)."""

from __future__ import annotations

import asyncio
import json
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import anyio
import anyio.lowlevel
import pytest

from laravel_cloud_queues import JobContext, Registry, current_job
from laravel_cloud_queues.errors import (
    ArgumentMismatchError,
    CodecError,
    JobDefectError,
    JobFailedError,
    UnknownJobError,
)
from laravel_cloud_queues.jobs import execution as execution_module
from laravel_cloud_queues.jobs.context import JobControl
from laravel_cloud_queues.jobs.dispatch import prepare_dispatch
from laravel_cloud_queues.jobs.envelope import ENVELOPE_KEY
from laravel_cloud_queues.jobs.execution import (
    HandlerResult,
    PreparedExecution,
    prepare_execution,
    run_prepared,
)
from laravel_cloud_queues.jobs.job import AnyJob
from laravel_cloud_queues.jobs.policy import ResolvedPolicy, WorkerDefaults
from tests.unit.jobs.fakes import make_registry


@dataclass(frozen=True)
class Address:
    city: str
    zip_code: str


def body_for(job: AnyJob, *args: object, **kwargs: object) -> str:
    return prepare_dispatch(job, args, kwargs, job.dispatch_options).message.body


def make_context(job_name: str = "j", attempt: int = 1) -> JobContext:
    return JobContext(
        job_name=job_name, uuid="u", message_id="m", queue="q", attempt=attempt, max_tries=3
    )


def run(prepared: PreparedExecution, context: JobContext | None = None) -> HandlerResult:
    return anyio.run(run_prepared, prepared, context or make_context())


# --- prepare_execution ----------------------------------------------------------------


def test_prepare_execution_decodes_against_the_signature() -> None:
    registry, _, _ = make_registry()

    @registry.job(name="ship", tries=4)
    def ship(
        order: uuid.UUID, address: Address, context: JobContext, *, rush: bool = False
    ) -> None: ...

    order = uuid.uuid4()
    prepared = prepare_execution(
        registry, body_for(ship, order, Address("Oslo", "0150"), rush=True)
    )
    assert prepared.job is ship
    assert prepared.args == (order, Address("Oslo", "0150"))
    assert prepared.kwargs == {"rush": True}
    assert prepared.envelope.job == "ship"
    assert prepared.policy(WorkerDefaults()) == ResolvedPolicy(
        tries=4, backoff=(0,), timeout=60.0, fail_on_timeout=False
    )


def test_policy_comes_from_the_message_not_the_current_declaration() -> None:
    """D4: a deploy never changes the rules for jobs already queued."""
    old, _, _ = make_registry()
    body = body_for(old.job(name="j", tries=5, backoff=[3])(lambda: None))
    new, _, _ = make_registry()
    new.job(name="j", tries=1)(lambda: None)
    policy = prepare_execution(new, body).policy(WorkerDefaults(timeout=10))
    assert (policy.tries, policy.backoff, policy.timeout) == (5, (3,), 10)


@pytest.mark.parametrize(
    "body",
    ["", "not json", "[]", '{"uuid": "u"}', json.dumps({"@pointer": "laravel:sqs-payloads:x"})],
)
def test_malformed_bodies_are_job_defects(body: str) -> None:
    registry, _, _ = make_registry()
    with pytest.raises(JobDefectError):
        prepare_execution(registry, body)


def test_unknown_job_is_a_defect_and_imports_nothing() -> None:
    source, _, _ = make_registry()
    body = body_for(source.job(name="os.system")(lambda: None))
    target, _, _ = make_registry()
    with pytest.raises(UnknownJobError):
        prepare_execution(target, body)


def test_argument_mismatch_is_a_defect() -> None:
    registry, _, _ = make_registry()
    job = registry.job(name="j")(lambda x: None)
    raw = json.loads(body_for(job, 1))
    raw[ENVELOPE_KEY]["kwargs"] = {"unexpected": 1}
    with pytest.raises((ArgumentMismatchError, CodecError)):
        prepare_execution(registry, json.dumps(raw))


def test_injected_parameter_from_payload_is_a_defect() -> None:
    registry, _, _ = make_registry()

    @registry.job(name="j")
    def handler(x: int, context: JobContext) -> None: ...

    raw = json.loads(body_for(handler, 1))
    raw[ENVELOPE_KEY]["kwargs"] = {"context": {"attempt": 99}}
    with pytest.raises(JobDefectError):
        prepare_execution(registry, json.dumps(raw))


def test_codec_mismatch_is_a_defect() -> None:
    registry, _, _ = make_registry()

    @registry.job(name="j")
    def handler(x: int) -> None: ...

    raw = json.loads(body_for(handler, 1))
    raw[ENVELOPE_KEY]["args"] = ["not an int"]
    with pytest.raises(CodecError):
        prepare_execution(registry, json.dumps(raw))


def test_unexpected_decode_failures_become_codec_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """A custom codec raising e.g. ValueError is still a deterministic defect."""
    registry, _, _ = make_registry()
    body = body_for(registry.job(name="j")(lambda: None))

    def broken(*_: object) -> Any:
        raise ValueError("bad custom codec")

    monkeypatch.setattr(execution_module, "decode_arguments", broken)
    with pytest.raises(CodecError) as info:
        prepare_execution(registry, body)
    assert isinstance(info.value.__cause__, ValueError)


# --- run_prepared ---------------------------------------------------------------------


def prepared_for(
    registry: Registry, handler: Callable[..., Any], *args: object
) -> PreparedExecution:
    job = registry.job(name="j")(handler)
    return prepare_execution(registry, body_for(job, *args))


def test_success_passes_arguments_and_ignores_return_value() -> None:
    registry, _, _ = make_registry()
    seen: list[object] = []

    def handler(x: int, context: JobContext) -> str:
        seen.extend([x, context, current_job()])
        return "ignored"

    context = make_context()
    assert run(prepared_for(registry, handler, 7), context) == HandlerResult("success")
    assert seen == [7, context, context]


def test_async_handler() -> None:
    registry, _, _ = make_registry()
    seen: list[int] = []

    async def handler(x: int) -> None:
        await anyio.lowlevel.checkpoint()
        seen.append(x)

    assert run(prepared_for(registry, handler, 3)).kind == "success"
    assert seen == [3]


def test_sync_handler_runs_on_the_calling_thread() -> None:
    registry, _, _ = make_registry()
    threads: list[int] = []
    run(prepared_for(registry, lambda: threads.append(threading.get_ident())))
    assert threads == [threading.get_ident()]


def test_exception_maps_to_error() -> None:
    registry, _, _ = make_registry()
    error = RuntimeError("boom")

    def handler() -> None:
        raise error

    result = run(prepared_for(registry, handler))
    assert result == HandlerResult("error", exception=error)


def test_release_and_fail() -> None:
    registry, _, _ = make_registry()

    def release(context: JobContext) -> None:
        context.release(2.5)

    def fail(context: JobContext) -> None:
        context.fail("nope")

    assert run(prepared_for(registry, release)) == HandlerResult("release", delay=3)
    other, _, _ = make_registry()
    result = run(prepared_for(other, fail))
    assert result.kind == "fail"
    assert isinstance(result.exception, JobFailedError)
    assert str(result.exception) == "nope"


def test_recorded_outcome_wins_over_swallowed_control_and_success() -> None:
    registry, _, _ = make_registry()

    def swallow() -> None:
        try:
            current_job().release(10)
        except JobControl:
            return  # swallowed: the handler "returns normally"

    assert run(prepared_for(registry, swallow)) == HandlerResult("release", delay=10)


def test_recorded_outcome_wins_over_a_later_exception() -> None:
    registry, _, _ = make_registry()
    reason = LookupError("gone")

    def handler(context: JobContext) -> None:
        try:
            context.fail(reason)
        except Exception:
            raise ValueError("cleanup failed") from None

    assert run(prepared_for(registry, handler)) == HandlerResult("fail", exception=reason)


def test_raising_job_control_without_an_outcome_is_an_error() -> None:
    registry, _, _ = make_registry()

    def handler() -> None:
        raise JobControl("user raised")

    result = run(prepared_for(registry, handler))
    assert result.kind == "error"
    assert isinstance(result.exception, JobControl)


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(3)])
def test_base_exceptions_propagate(error: BaseException) -> None:
    registry, _, _ = make_registry()

    def handler() -> None:
        raise error

    with pytest.raises(type(error)):
        run(prepared_for(registry, handler))
    with pytest.raises(RuntimeError):
        current_job()


def test_cancellation_propagates() -> None:
    registry, _, _ = make_registry()

    async def handler() -> None:
        await anyio.sleep(10)

    prepared = prepared_for(registry, handler)

    async def main() -> None:
        with anyio.move_on_after(0.01) as scope:
            await run_prepared(prepared, make_context())
        assert scope.cancelled_caught

    anyio.run(main)


def test_current_job_is_reset_after_the_handler() -> None:
    registry, _, _ = make_registry()
    prepared = prepared_for(registry, lambda: None)

    async def main() -> None:
        await run_prepared(prepared, make_context())
        with pytest.raises(RuntimeError):
            current_job()

    anyio.run(main)


def test_trace_context_is_activated_around_the_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[object] = []

    @contextmanager
    def fake_activate(carrier: Mapping[str, str]) -> Iterator[None]:
        events.append(("enter", dict(carrier)))
        yield
        events.append("exit")

    monkeypatch.setattr(execution_module, "activate_trace_context", fake_activate)
    registry, _, _ = make_registry()
    prepared = prepared_for(registry, lambda: events.append("handler"))
    run(prepared)
    assert events == [("enter", dict(prepared.envelope.context)), "handler", "exit"]


def test_concurrent_runs_do_not_share_current_job() -> None:
    registry, _, _ = make_registry()
    seen: dict[str, str] = {}

    async def handler(tag: str) -> None:
        await asyncio.sleep(0.01)
        seen[tag] = current_job().message_id

    job = registry.job(name="j")(handler)

    async def main() -> None:
        async with anyio.create_task_group() as group:
            for tag in ("a", "b"):
                prepared = prepare_execution(registry, body_for(job, tag))
                context = JobContext(
                    job_name="j", uuid="u", message_id=tag, queue="q", attempt=1, max_tries=1
                )
                group.start_soon(run_prepared, prepared, context)

    anyio.run(main)
    assert seen == {"a": "a", "b": "b"}
