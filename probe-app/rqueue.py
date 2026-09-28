"""Minimal Redis/Valkey queue for end-to-end probing on Laravel Cloud.

Follows PROJECT_SCOPE.md §11 (Redis transport), D2 (timeouts), D4 (policy in
the message) and D6b (log-only terminal failures). Throwaway prototype, not the
package implementation.
"""

import json
import os
import time
import uuid
from typing import Any

import redis

PREFIX = "probe-queues:"
QUEUE = "default"

# Laravel RedisQueue-style scripts.
MIGRATE = """
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, 100)
if next(val) ~= nil then
  redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
  for i = 1, #val do redis.call('rpush', KEYS[2], val[i]) end
end
return #val
"""
POP = """
local job = redis.call('lpop', KEYS[1])
if not job then return false end
local reserved = cjson.decode(job)
reserved['attempts'] = reserved['attempts'] + 1
reserved = cjson.encode(reserved)
redis.call('zadd', KEYS[2], ARGV[1], reserved)
return reserved
"""
RELEASE = """
redis.call('zrem', KEYS[1], ARGV[1])
redis.call('zadd', KEYS[2], ARGV[2], ARGV[1])
"""


def client() -> redis.Redis:
    url = os.environ.get("LARAVEL_CLOUD_QUEUES_REDIS_URL") or os.environ["REDIS_URL"]
    return redis.Redis.from_url(url, decode_responses=True, socket_timeout=10)


def keys(queue: str = QUEUE) -> dict[str, str]:
    base = f"{PREFIX}queues:{queue}"
    return {"pending": base, "delayed": f"{base}:delayed", "reserved": f"{base}:reserved"}


def record(r: redis.Redis, job_id: str, event: str, **extra: Any) -> None:
    entry = {"event": event, "at": time.time(), "pid": os.getpid(), **extra}
    r.rpush(f"{PREFIX}events:{job_id}", json.dumps(entry))
    r.expire(f"{PREFIX}events:{job_id}", 86400)


def dispatch(r: redis.Redis, kind: str, *, tries: int = 1, backoff: list[int] | None = None,
             timeout: int = 60, delay: int = 0, args: dict[str, Any] | None = None) -> str:
    job_id = str(uuid.uuid4())
    payload = json.dumps({
        "uuid": job_id,
        "displayName": f"probe.{kind}",
        "kind": kind,
        "args": args or {},
        "attempts": 0,
        "tries": tries,
        "backoff": backoff or [0],
        "timeout": timeout,
    })
    k = keys()
    if delay > 0:
        r.zadd(k["delayed"], {payload: time.time() + delay})
    else:
        r.rpush(k["pending"], payload)
    record(r, job_id, "queued", delay=delay)
    return job_id


def reserve(r: redis.Redis, reservation_seconds: int) -> str | None:
    k = keys()
    now = time.time()
    r.eval(MIGRATE, 2, k["delayed"], k["pending"], now)
    r.eval(MIGRATE, 2, k["reserved"], k["pending"], now)
    return r.eval(POP, 2, k["pending"], k["reserved"], now + reservation_seconds)


def delete(r: redis.Redis, reserved: str) -> None:
    r.zrem(keys()["reserved"], reserved)


def release(r: redis.Redis, reserved: str, delay: int) -> None:
    k = keys()
    r.eval(RELEASE, 2, k["reserved"], k["delayed"], reserved, time.time() + delay)


def stats(r: redis.Redis) -> dict[str, int]:
    k = keys()
    return {"pending": r.llen(k["pending"]), "delayed": r.zcard(k["delayed"]), "reserved": r.zcard(k["reserved"])}


def events(r: redis.Redis, job_id: str) -> list[dict[str, Any]]:
    return [json.loads(e) for e in r.lrange(f"{PREFIX}events:{job_id}", 0, -1)]
