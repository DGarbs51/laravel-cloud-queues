# FastAPI Integration

## Introduction

The FastAPI integration binds a job registry to your FastAPI application. Your jobs can
then use `Depends()` exactly like your routes, and the worker enters your application's
lifespan once, so everything you set up at startup is available to your jobs.

The integration requires the `fastapi` extra:

```shell
pip install "laravel-cloud-queues[fastapi]"
```

## Binding the Integration

Create a `LaravelCloudQueues` instance with your application:

```python
from fastapi import FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues

app = FastAPI()
queues = LaravelCloudQueues(app)


@queues.job(name="emails.send")
async def send_email(user_id: int) -> None: ...
```

The integration stores itself on `app.state.laravel_cloud_queues`. That is how the worker
finds it when you pass your app as the worker target:

```shell
laravel-cloud-queues work myapp.main:app
```

`LaravelCloudQueues` accepts an optional `config` (see [Configuring in Code](configuration.md#configuring-in-code))
or an existing `registry`, and exposes both `queues.app` and `queues.registry`.

## Dependency Injection

Jobs support `Depends()` just like route handlers:

<!-- runnable -->
```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI

from laravel_cloud_queues import JobContext, current_job
from laravel_cloud_queues.fastapi import LaravelCloudQueues


class Mailer:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    async def send(self, user_id: int, template: str) -> None: ...


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.mailer = Mailer("smtp://localhost")  # A process-scoped resource
    yield


app = FastAPI(lifespan=lifespan)
queues = LaravelCloudQueues(app)


async def get_mailer() -> AsyncIterator[Mailer]:
    yield app.state.mailer  # Teardown after the ``yield`` runs before acknowledgement


@queues.job(name="emails.send", tries=3)
async def send_email(
    user_id: int,
    template: str,
    mailer: Annotated[Mailer, Depends(get_mailer)],
    job: Annotated[JobContext, Depends(current_job)],
) -> None:
    await mailer.send(user_id, template)
    print(f"sent {template} to {user_id} (attempt {job.attempt})")
```

Only `user_id` and `template` are serialized. Injected parameters are excluded from the
message, and callers only pass the job's real arguments:

```python
await send_email.dispatch_async(user_id=1, template="welcome")
```

:::{tip}
Type checkers still see injected parameters in the signature of `dispatch`. If you run a
strict type checker, declare dependencies as defaults, such as
`mailer: Mailer = Depends(get_mailer)`, so a payload-only `dispatch` call type-checks,
and read the job context with `current_job()` inside the handler.
:::

### The Dependency Scope

Each delivery gets a fresh dependency scope, which behaves like a request:

- Sub-dependencies are cached within one job, and never across jobs.
- `app.dependency_overrides` is honored, which makes dependencies easy to replace in
  tests.
- Both sync and async dependencies work. Sync dependencies run in FastAPI's threadpool.

### Yield Dependencies

Teardown code after `yield` runs on success, on failure and on release, and it always
completes **before** the worker acknowledges the job. As in a request, the handler's
exception, including the release or fail raised through `JobContext`, is thrown into
your `yield` dependencies. That makes transactional dependencies work naturally:

```python
def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()  # Only when the job succeeded
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
```

An exception raised during teardown turns a success into a handler failure, and the
retry policy applies.

After an explicit release or fail, **async** teardown runs under a 10-second deadline.
Sync `yield` teardown runs in FastAPI's threadpool, cannot be cancelled, and is bounded
only by the job's timeout.

### Request-Only Dependencies

A job has no HTTP request, so dependencies that read one have no meaning in a job.
Declaring them raises a `ConfigurationError` when the job is registered:

- `Request`, `WebSocket`, `HTTPConnection`, `Response`, `BackgroundTasks` and
  `SecurityScopes`
- `Header()`, `Query()`, `Cookie()`, `Body()`, `Path()`, `Form()` and `File()`
- `Security()`

The check walks nested dependencies too, and is repeated when the job runs, to catch
overrides added after registration.

## The Lifespan

The worker enters your application's lifespan **once per process**, before it takes the
first job, and exits it on a clean shutdown. Resources you create at startup, like
database pools and HTTP clients, are shared by every job the worker runs:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    async with httpx.AsyncClient(base_url="https://api.example.com") as client:
        yield {"http": client}
```

Keys yielded by a lifespan are normally exposed as `request.state`. Queue jobs have no
request, so the integration copies identifier keys onto `app.state` instead. In a
dependency, read them as `app.state.http`.

`on_startup` and `on_shutdown` handlers run when your application uses FastAPI's default
lifespan. A custom `lifespan=` replaces them, as it does in FastAPI itself.

When a worker is recycled, for example with `--max-jobs`, the lifespan runs again in the
new process.

The worker closes the registry's async producer after your lifespan's shutdown. Your web
server runs the lifespan without the integration, so the web process should close it
itself: add `await queues.registry.aclose_producer()` after the `yield` in your lifespan.
See [Closing the Async Producer](dispatching.md#closing-the-async-producer).

## Calling Jobs Directly

Calling a job directly runs the raw function without dependency injection. Pass injected
values yourself:

```python
await send_email(1, "welcome", mailer=mailer, job=context)
```

## Testing

Use `queues.registry.testing()` to run jobs eagerly in your tests, with dependency
injection and teardown exactly as the worker runs them. See [Testing](testing.md).
