# Coming From Celery

## Introduction

If you have used Celery with FastAPI, most of Laravel Cloud Queues will feel familiar:
you declare tasks, called *jobs* here, dispatch them from your application, and run them
in a separate worker. The biggest difference is that async code is native on both sides
of the queue, so the usual FastAPI workarounds are not needed.

| With Celery | With Laravel Cloud Queues |
|---|---|
| Tasks are sync, and `async def` code is wrapped in `asyncio.run()` per task, which creates a new event loop each time and breaks async clients and pools created at startup | `async def` jobs run natively, on one event loop that lives for the whole worker process |
| Startup resources come from worker signals, separate from your FastAPI app | The worker enters your app's lifespan once, so `app.state` resources and `Depends()` work in jobs as they do in routes |
| `.delay()` blocks the event loop while it talks to the broker | `await job.dispatch_async(...)` never blocks the loop |
| Plain `def` tasks | Plain `def` jobs work too, and `job.dispatch(...)` is the blocking form for scripts and sync code |

```python
@queues.job(name="users.sync_profile", tries=3, backoff=[5, 30])
async def sync_profile(
    user_id: int, http: Annotated[httpx.AsyncClient, Depends(get_http_client)]
) -> None:
    await http.post("/profiles/sync", json={"user_id": user_id})


@app.post("/users/{user_id}/sync")
async def request_sync(user_id: int) -> dict[str, str]:
    receipt = await sync_profile.dispatch_async(user_id=user_id)
    return {"message_id": receipt.message_id}
```

## Translating Concepts

| Celery | Laravel Cloud Queues |
|---|---|
| `@app.task` | [`@queues.job(name=...)`](jobs.md) |
| `task.delay(...)` / `task.apply_async(...)` | [`await job.dispatch_async(...)`](dispatching.md), or `job.dispatch(...)` |
| `apply_async(queue=..., countdown=...)` | `job.options(queue=..., delay=...).dispatch_async(...)` |
| `autoretry_for`, `max_retries`, `retry_backoff` | [`tries` and `backoff`](retries-and-timeouts.md) on the job |
| `self.retry(countdown=...)` | [`current_job().release(delay=...)`](job-context.md#releasing-a-job) |
| `self.request.retries` | `current_job().attempt - 1` |
| `time_limit` | `timeout` |
| `task_always_eager` | [`registry.testing()`](testing.md) |
| `celery -A myapp worker` | [`laravel-cloud-queues work myapp.main:app`](workers.md) |

## Differences to Plan For

- **One job in flight per worker process.** Celery's prefork and gevent pools run several
  tasks per worker. Here, you scale by running more worker processes, and async jobs do
  not run concurrently inside one worker.
- **Fire-and-forget.** There is no result backend, `AsyncResult`, chains, groups or
  chords.
- **Retries are declared on the job** (`tries`, `backoff`), or requested from inside it
  with `current_job().release(delay=...)`, instead of `self.retry()`. Jobs run once by
  default.
- **Delivery is at least once**, with SQS or Redis semantics, like Celery with
  `acks_late=True`. Keep your handlers idempotent.
- **Arguments are typed JSON, never pickle.** Argument decoding follows your handler's
  annotations. See [Arguments and Payloads](serialization.md).
