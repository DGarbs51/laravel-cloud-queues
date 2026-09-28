"""Jobs for the worker subprocess tests.

Every run appends one JSON line to ``$LCQ_TEST_RECORD`` so tests can see which deliveries
ran, with which attempt, and whether ``current_job()`` was isolated per delivery. The test
process calls :func:`build` with an explicit config to dispatch the same jobs.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import anyio

from laravel_cloud_queues import JobContext, Registry, current_job


def record(context: JobContext, **extra: object) -> None:
    path = os.environ.get("LCQ_TEST_RECORD")
    if not path:
        return
    line = {
        "job": context.job_name,
        "message_id": context.message_id,
        "attempt": context.attempt,
        "max_tries": context.max_tries,
        "queue": context.queue,
        "current_is_context": current_job() is context,
        "pid": os.getpid(),
        **extra,
    }
    with open(path, "a") as file:
        file.write(json.dumps(line) + "\n")


def spin(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


def build(**options: Any) -> Registry:
    registry = Registry(**options)

    @registry.job(name="ok")
    async def ok(context: JobContext, value: int = 0) -> None:
        record(context, value=value)

    @registry.job(name="sync_ok")
    def sync_ok(context: JobContext) -> None:
        record(context)

    @registry.job(name="always_fails", tries=3)
    def always_fails(context: JobContext) -> None:
        record(context)
        raise RuntimeError("always fails")

    @registry.job(name="fails_default")
    def fails_default(context: JobContext) -> None:
        record(context)
        raise RuntimeError("no retries by default")

    @registry.job(name="sleepy")
    def sleepy(context: JobContext, seconds: float) -> None:
        record(context, phase="start")
        time.sleep(seconds)
        record(context, phase="end")

    @registry.job(name="slow_async", tries=3, timeout=0.5)
    async def slow_async(context: JobContext) -> None:
        record(context)
        await anyio.sleep(30)

    @registry.job(name="slow_loop", tries=3, timeout=0.5)
    def slow_loop(context: JobContext) -> None:
        record(context)
        spin(30)

    @registry.job(name="slow_native", tries=3, timeout=0.5)
    def slow_native(context: JobContext, n: int) -> None:
        record(context)
        sum(range(n))
        record(context, phase="native returned")

    @registry.job(name="slow_terminal", tries=1, timeout=0.5)
    def slow_terminal(context: JobContext) -> None:
        record(context)
        spin(30)

    @registry.job(name="slow_fail_on_timeout", tries=4, timeout=0.5, fail_on_timeout=True)
    def slow_fail_on_timeout(context: JobContext) -> None:
        record(context)
        spin(30)

    @registry.job(name="native_block", timeout=0)
    def native_block(context: JobContext, n: int) -> None:
        record(context, phase="start")
        sum(range(n))
        record(context, phase="end")

    return registry


registry = build()
