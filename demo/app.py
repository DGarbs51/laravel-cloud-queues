"""Executable FastAPI producer/worker fixture. All payload values are synthetic."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from laravel_cloud_queues.fastapi import LaravelCloudQueues, current_job


def record(kind: str, **fields: object) -> None:
    row = {"kind": kind, "pid": os.getpid(), "time": time.monotonic(), **fields}
    destination = os.environ.get("LCQ_DEMO_EVENTS")
    if destination:
        with Path(destination).open("a") as stream:
            stream.write(json.dumps(row) + "\n")


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    application.state.resource = uuid4().hex
    record("lifespan_start", resource=application.state.resource)
    try:
        yield
    finally:
        record("lifespan_stop", resource=application.state.resource)


app = FastAPI(title="Laravel Cloud Queues conformance demo", lifespan=lifespan)
queues = LaravelCloudQueues(app)


def resource() -> Iterator[str]:
    token = uuid4().hex
    record("dependency_start", dependency_id=token)
    try:
        yield token
    finally:
        record("dependency_stop", dependency_id=token)


def observe(label: str, token: str = "") -> None:
    context = current_job()
    trace_id = ""
    try:
        from opentelemetry.trace import get_current_span

        trace_id = format(get_current_span().get_span_context().trace_id, "032x")
    except ImportError:
        pass
    record(
        "handler",
        label=label,
        dependency_id=token,
        resource=app.state.resource,
        message_id=context.message_id,
        attempt=context.attempt,
        queue=context.queue,
        uuid=context.uuid,
        main_thread=threading.current_thread() is threading.main_thread(),
        trace_id=trace_id,
    )


@queues.job(name="demo.sync")
def sync_job(label: str, token: str = Depends(resource)) -> None:
    observe(label, token)


@queues.job(name="demo.async")
async def async_job(label: str, token: str = Depends(resource)) -> None:
    await asyncio.sleep(0)
    observe(label, token)


@queues.job(name="demo.named", queue=os.environ.get("LCQ_DEMO_NAMED_QUEUE", "named"))
def named_job(label: str) -> None:
    observe(label)


@queues.job(name="demo.retry", tries=3, backoff=[1, 0])
def retry_job(label: str, succeed_on: int = 2, token: str = Depends(resource)) -> None:
    observe(label, token)
    if current_job().attempt < succeed_on:
        raise RuntimeError("synthetic retry failure")


@queues.job(name="demo.release", tries=3)
def release_job(label: str, token: str = Depends(resource)) -> None:
    observe(label, token)
    if current_job().attempt == 1:
        current_job().release(1)


@queues.job(name="demo.fail", tries=5)
def fail_job(label: str, token: str = Depends(resource)) -> None:
    observe(label, token)
    current_job().fail("synthetic explicit failure")


@queues.job(name="demo.default_failure")
def default_failure(label: str) -> None:
    observe(label)
    raise RuntimeError("synthetic default-policy failure")


@queues.job(name="demo.timeout", tries=2, timeout=0.25)
def timeout_job(label: str, style: str = "python") -> None:
    observe(label)
    if style == "native":
        # ponytail: calibrated native loop; this intentionally exposes the CPython signal ceiling.
        sum(range(int(os.environ.get("LCQ_DEMO_NATIVE_ITERATIONS", "100000000"))))
    elif style == "sleep":
        time.sleep(10)
    else:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            pass


@queues.job(name="demo.async_timeout", tries=2, timeout=0.25)
async def async_timeout_job(label: str) -> None:
    observe(label)
    await asyncio.sleep(10)


@queues.job(name="demo.fail_timeout", tries=3, timeout=0.25, fail_on_timeout=True)
def fail_timeout_job(label: str) -> None:
    observe(label)
    time.sleep(10)


@queues.job(name="demo.slow", timeout=0)
async def slow_job(label: str, seconds: float = 1) -> None:
    observe(label)
    await asyncio.sleep(seconds)
    record("handler_finished", label=label)


@queues.job(name="demo.collisions")
def collisions(queue: str, delay: int, timeout: int) -> str:
    return f"{queue}:{delay}:{timeout}"


@queues.job(name="demo.payload")
def payload_job(value: str) -> None:
    record("payload_length", length=len(value))


class DispatchRequest(BaseModel):
    kwargs: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)


@app.post("/dispatch/{name}")
async def dispatch(name: str, body: DispatchRequest) -> dict[str, object]:
    # Registry lookup only; this endpoint never imports a payload-specified module.
    if name not in queues.registry.jobs():
        raise HTTPException(404, "Unknown demo job")
    receipt = await queues.registry.get(name).options(**body.options).dispatch_async(**body.kwargs)
    return {**asdict(receipt), "producer_pid": os.getpid()}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
