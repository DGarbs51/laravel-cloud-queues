"""End-to-end smoke test of the real laravel-cloud-queues package on Laravel Cloud (D6c).

Web: POST /queue/e2e dispatches one job per case; GET /queue/jobs/{uuid} shows what the
workers recorded. Workers: `python -m laravel_cloud_queues.cli work main:app` on the App and
worker clusters, using the environment's Laravel Valkey (`LARAVEL_CLOUD_QUEUES_BACKEND=redis`,
`REDIS_URL`). Records live in Redis for 24 hours under `lcq-probe:`.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from typing import Any

import redis
from fastapi import HTTPException, Query

from laravel_cloud_queues import current_job
from laravel_cloud_queues.fastapi import LaravelCloudQueues

from main import app, require_token

queues = LaravelCloudQueues(app)

RECORDS = "lcq-probe:events:"


def _redis() -> redis.Redis:
    url = os.environ.get("LARAVEL_CLOUD_QUEUES_REDIS_URL") or os.environ["REDIS_URL"]
    return redis.Redis.from_url(url, decode_responses=True, socket_timeout=10)


def record(event: str, **extra: Any) -> None:
    job = current_job()
    entry = {
        "event": event,
        "job": job.job_name,
        "attempt": job.attempt,
        "message_id": job.message_id,
        "at": time.time(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        **extra,
    }
    client = _redis()
    key = RECORDS + job.uuid
    client.rpush(key, json.dumps(entry))
    client.expire(key, 86_400)


@queues.job(name="probe.ok")
def ok(tag: str) -> None:
    record("ran", tag=tag)


@queues.job(name="probe.async_ok")
async def async_ok(tag: str) -> None:
    await asyncio.sleep(0.1)
    record("ran", tag=tag)


@queues.job(name="probe.flaky", tries=2, backoff=[3])
def flaky(tag: str) -> None:
    record("ran", tag=tag)
    if current_job().attempt == 1:
        raise RuntimeError("flaky job fails on its first attempt")


@queues.job(name="probe.always_fails", tries=2, backoff=[1])
def always_fails(tag: str) -> None:
    record("ran", tag=tag)
    raise RuntimeError("job always fails")


@queues.job(name="probe.slow", tries=2, timeout=3)
def slow(seconds: float) -> None:
    record("started", seconds=seconds)
    time.sleep(seconds)
    record("finished")


@app.post("/queue/e2e")
async def queue_e2e(token: str = Query("")) -> dict[str, str]:
    require_token(token)
    receipts = {
        "ok": await ok.dispatch_async(tag="ok"),
        "async_ok": await async_ok.dispatch_async(tag="async"),
        "delayed": await ok.options(delay=5).dispatch_async(tag="delayed"),
        "flaky_retry": await flaky.dispatch_async(tag="flaky"),
        "terminal_fail": await always_fails.dispatch_async(tag="fail"),
        "timeout_then_terminal": await slow.dispatch_async(seconds=10),
    }
    return {case: receipt.uuid for case, receipt in receipts.items()}


@app.post("/queue/burst")
async def queue_burst(token: str = Query(""), n: int = Query(50, ge=1, le=500)) -> list[str]:
    require_token(token)
    return [(await ok.dispatch_async(tag=f"burst-{i}")).uuid for i in range(n)]


@app.get("/queue/jobs/{job_uuid}")
def queue_job(job_uuid: str, token: str = Query("")) -> list[dict[str, Any]]:
    require_token(token)
    if len(job_uuid) > 64:
        raise HTTPException(422, "Invalid job id.")
    return [json.loads(e) for e in _redis().lrange(RECORDS + job_uuid, 0, -1)]
