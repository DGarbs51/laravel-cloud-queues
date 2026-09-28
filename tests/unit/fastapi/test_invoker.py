"""Per-job dependency scope: injection, teardown, overrides, and the calling thread."""

from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import anyio
import pytest
from fastapi import Depends, FastAPI
from starlette.requests import Request

from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.fastapi._depends import parameter_is_injected
from laravel_cloud_queues.fastapi._invoker import TEARDOWN_DEADLINE_SECONDS, FastAPIInvoker
from laravel_cloud_queues.jobs.context import JobContext, JobControl, current_job
from laravel_cloud_queues.jobs.signature import inspect_handler


class _Job:
    """A job the invoker can call. The signature is inspected the same way ``Job`` does."""

    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func
        self._signature = inspect_handler(func, is_injected=parameter_is_injected)


class _Context:
    def __init__(self, name: str = "emails.send") -> None:
        self._name = name

    @property
    def job_name(self) -> str:
        return self._name


def _run(func: Callable[..., Any], *args: Any) -> Any:
    return anyio.run(func, *args, backend="asyncio")


def test_sync_and_async_jobs_receive_serialized_args_and_depends() -> None:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    seen: list[str] = []

    def get_mailer() -> str:
        return "smtp"

    def sync_capture(user_id: int, mailer: str = Depends(get_mailer)) -> None:
        seen.append(f"sync:{user_id}:{mailer}")

    async def async_capture(user_id: int, mailer: str = Depends(get_mailer)) -> None:
        seen.append(f"async:{user_id}:{mailer}")

    _run(invoker.invoke, _Job(sync_capture), (7,), {}, _Context())
    _run(invoker.invoke, _Job(async_capture), (), {"user_id": 8}, _Context())
    assert seen == ["sync:7:smtp", "async:8:smtp"]


def test_sync_handler_runs_on_the_calling_thread_and_sync_deps_use_the_threadpool() -> None:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    seen: dict[str, threading.Thread] = {}

    def get_mailer() -> str:
        seen["dep"] = threading.current_thread()
        return "smtp"

    def send(mailer: str = Depends(get_mailer)) -> None:
        seen["handler"] = threading.current_thread()
        assert mailer == "smtp"

    _run(invoker.invoke, _Job(send), (), {}, _Context())
    assert seen["handler"] is threading.main_thread()
    # FastAPI resolves sync dependencies with run_in_threadpool. Timeouts do not
    # depend on that thread; they depend on the handler staying on the main thread.
    assert seen["dep"] is not threading.main_thread()


def test_yield_teardown_order_on_success_failure_and_release() -> None:
    order: list[str] = []

    def outer() -> Iterator[str]:
        order.append("outer-enter")
        yield "outer"
        order.append("outer-exit")

    def inner() -> Iterator[str]:
        order.append("inner-enter")
        yield "inner"
        order.append("inner-exit")

    app = FastAPI()
    invoker = FastAPIInvoker(app)

    def send(label: str, left: str = Depends(outer), right: str = Depends(inner)) -> None:
        order.append(f"run:{label}:{left}:{right}")
        if label == "boom":
            raise RuntimeError("handler")
        if label == "release":
            raise JobControl("release")

    _run(invoker.invoke, _Job(send), ("ok",), {}, _Context())
    assert order == [
        "outer-enter",
        "inner-enter",
        "run:ok:outer:inner",
        "inner-exit",
        "outer-exit",
    ]

    order.clear()
    with pytest.raises(RuntimeError, match="handler"):
        _run(invoker.invoke, _Job(send), ("boom",), {}, _Context())
    assert order == [
        "outer-enter",
        "inner-enter",
        "run:boom:outer:inner",
        "inner-exit",
        "outer-exit",
    ]

    order.clear()
    with pytest.raises(JobControl, match="release"):
        _run(invoker.invoke, _Job(send), ("release",), {}, _Context())
    assert order[-2:] == ["inner-exit", "outer-exit"]


def test_teardown_exception_after_success_propagates() -> None:
    def resource() -> Iterator[int]:
        yield 1
        raise RuntimeError("teardown boom")

    app = FastAPI()

    def send(value: int = Depends(resource)) -> None:
        assert value == 1

    with pytest.raises(RuntimeError, match="teardown boom"):
        _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())


def test_teardown_exception_after_job_control_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def resource() -> Iterator[int]:
        yield 1
        raise RuntimeError("teardown boom")

    app = FastAPI()

    def send(token: str, value: int = Depends(resource)) -> None:
        assert value == 1
        raise JobControl("fail")

    monkeypatch.setattr(
        "laravel_cloud_queues.fastapi._invoker.TEARDOWN_DEADLINE_SECONDS",
        TEARDOWN_DEADLINE_SECONDS,
    )
    with caplog.at_level(logging.ERROR), pytest.raises(JobControl, match="fail"):
        _run(FastAPIInvoker(app).invoke, _Job(send), ("secret-token",), {}, _Context())
    assert "teardown boom" in caplog.text
    assert "secret-token" not in caplog.text
    assert "emails.send" in caplog.text


def test_teardown_exception_after_handler_error_keeps_the_handler_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def resource() -> Iterator[int]:
        yield 1
        raise RuntimeError("teardown boom")

    app = FastAPI()

    def send(value: int = Depends(resource)) -> None:
        assert value == 1
        raise RuntimeError("handler boom")

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="handler boom"):
        _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())
    assert "teardown boom" in caplog.text


def test_release_teardown_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "laravel_cloud_queues.fastapi._invoker.TEARDOWN_DEADLINE_SECONDS",
        0.2,
    )
    events: list[str] = []

    async def resource() -> AsyncIterator[int]:
        try:
            yield 1
        finally:
            events.append("teardown-start")
            await anyio.sleep(30)
            events.append("teardown-done")

    app = FastAPI()

    def send(value: int = Depends(resource)) -> None:
        assert value == 1
        raise JobControl("release")

    with pytest.raises(JobControl, match="release"):
        _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())
    assert events == ["teardown-start"]


def test_dependency_overrides_and_per_job_cache() -> None:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    created: list[object] = []

    def token() -> object:
        value = object()
        created.append(value)
        return value

    def bundle(
        left: object = Depends(token),
        right: object = Depends(token),
    ) -> tuple[object, object]:
        return (left, right)

    def send(
        user_id: int,
        packed: tuple[object, object] = Depends(bundle),
        again: object = Depends(token),
    ) -> None:
        left, right = packed
        assert left is right is again
        assert user_id == 1

    _run(invoker.invoke, _Job(send), (1,), {}, _Context())
    _run(invoker.invoke, _Job(send), (1,), {}, _Context())
    assert len(created) == 2
    assert created[0] is not created[1]

    app.dependency_overrides[token] = lambda: "fixed"

    def overridden(value: object = Depends(token)) -> None:
        assert value == "fixed"

    _run(invoker.invoke, _Job(overridden), (), {}, _Context())
    assert current_job not in app.dependency_overrides


def test_use_cache_false_builds_distinct_instances_within_one_job() -> None:
    app = FastAPI()
    seen: list[object] = []

    def token() -> object:
        return object()

    def send(
        left: object = Depends(token, use_cache=False),
        right: object = Depends(token, use_cache=False),
    ) -> None:
        seen.extend((left, right))

    _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())
    assert seen[0] is not seen[1]


def test_job_context_by_annotation_and_by_current_job() -> None:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    first = _Context("one")
    second = _Context("two")
    seen: list[tuple[str, str]] = []

    def send(
        user_id: int,
        job: JobContext,
        via: JobContext = Depends(current_job),  # noqa: B008
    ) -> None:
        seen.append((job.job_name, via.job_name))
        assert user_id == 3

    _run(invoker.invoke, _Job(send), (3,), {}, first)
    _run(invoker.invoke, _Job(send), (3,), {}, second)
    assert seen == [("one", "one"), ("two", "two")]


def test_positional_only_context_keeps_the_payload_shape() -> None:
    app = FastAPI()
    seen: list[tuple[int, str, str]] = []

    def send(user_id: int, job: JobContext, /, label: str) -> None:
        seen.append((user_id, job.job_name, label))

    _run(FastAPIInvoker(app).invoke, _Job(send), (3, "hi"), {}, _Context("shaped"))
    assert seen == [(3, "shaped", "hi")]


def test_request_dependency_fails_at_invoke() -> None:
    app = FastAPI()

    def send(request: Request) -> None:
        assert request

    with pytest.raises(ConfigurationError, match="Request"):
        _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())


def test_sync_and_async_yield_dependencies_tear_down_lifo() -> None:
    order: list[str] = []

    def sync_dep() -> Iterator[str]:
        order.append("sync-enter")
        yield "sync"
        order.append("sync-exit")

    async def async_dep() -> AsyncIterator[str]:
        order.append("async-enter")
        yield "async"
        order.append("async-exit")

    app = FastAPI()

    async def send(left: str = Depends(sync_dep), right: str = Depends(async_dep)) -> None:
        order.append(f"run:{left}:{right}")

    _run(FastAPIInvoker(app).invoke, _Job(send), (), {}, _Context())
    assert order == [
        "sync-enter",
        "async-enter",
        "run:sync:async",
        "async-exit",
        "sync-exit",
    ]
