"""Small FastAPI app for the worker subprocess test.

``laravel-cloud-queues work tests.integration.fastapi.apps.main:app``

Lifespan and dependency teardown append lines to ``$LCQ_FA_EVENTS``. Teardown records
whether the Redis reserved set still holds the delivery. Acknowledgement removes that
member (Laravel ``RedisQueue`` deletes with ``zrem`` on
``queues:{name}:reserved`` — ``RedisQueue.php:619``, key shape ``RedisQueue.php:683``).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues


def _event(line: str) -> None:
    path = os.environ["LCQ_FA_EVENTS"]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()


def _reserved_count() -> int:
    import redis

    url = os.environ["LARAVEL_CLOUD_QUEUES_REDIS_URL"]
    prefix = os.environ["LARAVEL_CLOUD_QUEUES_REDIS_PREFIX"]
    queue = os.environ.get("LARAVEL_CLOUD_QUEUES_REDIS_QUEUE", "default")
    client = redis.Redis.from_url(url)
    try:
        return int(client.zcard(f"{prefix}queues:{queue}:reserved"))
    finally:
        client.close()


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    _event("lifespan-start")
    yield
    _event("lifespan-stop")


app = FastAPI(lifespan=_lifespan)
queues = LaravelCloudQueues(app)


def resource() -> Iterator[object]:
    _event("dep-enter")
    try:
        yield object()
    finally:
        _event(f"dep-teardown reserved={_reserved_count()}")


@queues.job(name="probe.touch")
def touch(n: int, _res: object = Depends(resource)) -> None:
    _event(f"handled {n}")
