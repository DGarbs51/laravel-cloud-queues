"""Application lifespan is entered once per worker enter and is visible on app.state."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import anyio
import pytest
from fastapi import APIRouter, Depends, FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues
from laravel_cloud_queues.fastapi._depends import parameter_is_injected
from laravel_cloud_queues.fastapi._invoker import FastAPIInvoker
from laravel_cloud_queues.jobs.signature import inspect_handler
from tests.unit.jobs.fakes import make_registry


class _Registry:
    def job(self, func: Callable[..., Any] | None = None, /, **_kwargs: Any) -> Any:
        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn

        if func is not None:
            return decorate(func)
        return decorate


class _Job:
    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func
        self.signature = inspect_handler(func, is_injected=parameter_is_injected)

    def call_bound(self, bound: inspect.BoundArguments) -> Any:
        return self.func(*bound.args, **bound.kwargs)


def _run(func: Callable[..., Any], *args: Any) -> Any:
    return anyio.run(func, *args, backend="asyncio")


def test_lifespan_runs_once_across_jobs_and_exits_on_stop() -> None:
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        events.append("start")
        _app.state.db = "ready"
        # Only identifier keys are copied onto app.state; the binding key is left alone.
        yield {"label": "yielded", "not an identifier": 1, "laravel_cloud_queues": None}
        events.append("stop")

    app = FastAPI(lifespan=lifespan)
    queues = LaravelCloudQueues(app, registry=_Registry())  # type: ignore[arg-type]
    invoker = FastAPIInvoker(app)

    def get_db() -> str:
        return f"{app.state.db}:{app.state.label}"

    def send(n: int, db: str = Depends(get_db)) -> None:
        events.append(f"job:{n}:{db}")

    async def scenario() -> None:
        async with queues.lifespan():
            assert events == ["start"]
            assert app.state.laravel_cloud_queues is queues
            assert not hasattr(app.state, "not an identifier")
            await invoker.invoke(_Job(send), (1,), {}, None)  # type: ignore[arg-type]
            await invoker.invoke(_Job(send), (2,), {}, None)  # type: ignore[arg-type]
            assert events.count("start") == 1

    _run(scenario)
    assert events == [
        "start",
        "job:1:ready:yielded",
        "job:2:ready:yielded",
        "stop",
    ]


def test_on_startup_and_on_shutdown_run_without_a_lifespan_argument() -> None:
    events: list[str] = []

    def startup() -> None:
        events.append("startup")

    async def shutdown() -> None:
        events.append("shutdown")

    app = FastAPI(on_startup=[startup], on_shutdown=[shutdown])
    queues = LaravelCloudQueues(app, registry=_Registry())  # type: ignore[arg-type]

    async def scenario() -> None:
        async with queues.lifespan():
            events.append("jobs")

    _run(scenario)
    assert events == ["startup", "jobs", "shutdown"]


def test_shutdown_runs_when_the_body_raises() -> None:
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        events.append("start")
        yield
        events.append("stop")

    app = FastAPI(lifespan=lifespan)
    queues = LaravelCloudQueues(app, registry=_Registry())  # type: ignore[arg-type]

    async def scenario() -> None:
        async with queues.lifespan():
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _run(scenario)
    assert events == ["start", "stop"]


def test_included_router_lifespan_is_entered() -> None:
    events: list[str] = []

    @asynccontextmanager
    async def router_lifespan(_app: FastAPI) -> AsyncIterator[None]:
        events.append("router-start")
        yield
        events.append("router-stop")

    app = FastAPI()
    router = APIRouter(lifespan=router_lifespan)
    app.include_router(router)
    queues = LaravelCloudQueues(app, registry=_Registry())  # type: ignore[arg-type]

    async def scenario() -> None:
        async with queues.lifespan():
            events.append("jobs")

    _run(scenario)
    assert events == ["router-start", "jobs", "router-stop"]


@pytest.mark.parametrize("shutdown_fails", [False, True])
def test_async_producer_is_closed_after_the_app_shutdown(shutdown_fails: bool) -> None:
    registry, producer, _ = make_registry(native_async=True)
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        events.append(f"stop, closed={producer.opened[0].closed}")
        if shutdown_fails:
            raise RuntimeError("shutdown failed")

    queues = LaravelCloudQueues(FastAPI(lifespan=lifespan), registry=registry)

    async def scenario() -> None:
        async with queues.lifespan():
            await registry.async_producer()

    if shutdown_fails:
        with pytest.raises(RuntimeError, match="shutdown failed"):
            _run(scenario)
    else:
        _run(scenario)
    assert events == ["stop, closed=0"]
    assert producer.opened[0].closed == 1
