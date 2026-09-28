"""Redis/Valkey availability and cleanup restricted to a unique test prefix."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from harness.sqs import ServiceUnavailable

if TYPE_CHECKING:
    from redis import Redis


def redis_url() -> str:
    return os.environ.get("LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def unique_prefix() -> str:
    return f"lcq:{uuid.uuid4().hex}:"


def connect(url: str | None = None) -> Redis:
    try:
        from redis import Redis, RedisError
    except ImportError as exc:
        raise ServiceUnavailable("Redis helper requires redis") from exc
    client: Redis = Redis.from_url(url or redis_url(), socket_connect_timeout=1, socket_timeout=2)
    try:
        client.ping()
    except (RedisError, OSError) as exc:
        client.close()
        raise ServiceUnavailable(f"Redis/Valkey endpoint is unavailable: {exc}") from exc
    return client


def available(url: str | None = None) -> bool:
    try:
        client = connect(url)
    except ServiceUnavailable:
        return False
    client.close()
    return True


def cleanup(client: Redis, prefix: str) -> None:
    if not prefix or any(char in prefix for char in "*?[]\\"):
        raise ValueError("Cleanup requires a non-empty literal prefix")
    # SCAN is incremental; never flush the database or touch another test's keys.
    for key in client.scan_iter(match=prefix + "*", count=100):
        client.delete(key)


@contextmanager
def redis_service(url: str | None = None) -> Iterator[tuple[Redis, str]]:
    client = connect(url)
    prefix = unique_prefix()
    try:
        yield client, prefix
    finally:
        try:
            cleanup(client, prefix)
        finally:
            client.close()
