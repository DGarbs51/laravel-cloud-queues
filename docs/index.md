# Introduction

While building your web application, you may have some tasks, such as sending an email or
syncing with a third-party API, that take too long to perform during a typical web
request. Laravel Cloud Queues lets you move that work into queued jobs that run in the
background, so your application can respond to web requests quickly and give your users a
better experience.

Laravel Cloud Queues brings [Laravel Cloud](https://cloud.laravel.com)'s queue
infrastructure to Python applications. You dispatch jobs from a FastAPI (or plain Python)
process and run them in a separate worker process, and Laravel Cloud hosts the queue.
Laravel Cloud's managed queues are supported once the platform enables them for Python.
Today, on worker clusters, you can use your own Amazon SQS queues or a Laravel Valkey cache.

The package feels like Python and FastAPI. It matches Laravel's queue *infrastructure*
contract, not Laravel's PHP API: the SQS message lifecycle, retries, FIFO queues,
timeouts, Laravel Cloud's queue agent and its observability events.

```python
from fastapi import FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues

app = FastAPI()
queues = LaravelCloudQueues(app)


@queues.job(name="emails.send", tries=3, backoff=[5, 30])
async def send_email(user_id: int, template: str = "welcome") -> None:
    ...


@app.post("/signup")
async def signup(user_id: int) -> dict[str, str]:
    receipt = await send_email.dispatch_async(user_id=user_id)
    return {"message_id": receipt.message_id}
```

```shell
laravel-cloud-queues work myapp.main:app
```

:::{important}
**Platform status (verified 2026-09-27).** Laravel Cloud runs Python 3.10–3.14
applications (this package requires 3.11+), including FastAPI, but **managed queues are not yet available for Python**.
Worker clusters *do* run Python today, so this package ships two self-managed backends
that work on Laravel Cloud now: `sqs` (your own SQS queues) and `redis` (a Laravel Valkey
cache). See [Deploying to Laravel Cloud](deployment.md) for details.
:::

## Where to Start

If you are new to the package, work through these pages in order:

1. [Installation](installation.md): install the package and its optional extras.
2. [Configuration](configuration.md): choose a queue backend.
3. [Creating Jobs](jobs.md) and [Dispatching Jobs](dispatching.md): write and queue your
   first job.
4. [Running the Worker](workers.md): process your jobs.
5. [Deploying to Laravel Cloud](deployment.md): run it in production.

Coming from Celery? Start with [Coming From Celery](celery.md).

## Feature Overview

- **Typed jobs.** `dispatch` and `dispatch_async` take exactly the handler's parameters,
  so your type checker checks your arguments.
- **Async native.** `async def` jobs run on one event loop that lives as long as the
  worker, and `dispatch_async` never blocks your event loop.
- **FastAPI integration.** Jobs use `Depends()` like routes, and the worker enters your
  app's lifespan once.
- **Retries, backoff and timeouts** that follow Laravel's worker, with the policy carried
  inside each message.
- **FIFO and fair queues** on SQS.
- **A safe wire format.** Messages are versioned JSON, argument decoding is driven by your
  type annotations, and nothing is ever pickled.
- **Testing helpers** that run your jobs eagerly through the worker's own execution path.
- **Laravel Cloud observability.** Python jobs appear in the Queues dashboard like PHP
  jobs (managed mode).

```{toctree}
:hidden:
:caption: Prologue

self
celery
limitations
```

```{toctree}
:hidden:
:caption: Getting Started

installation
configuration
deployment
```

```{toctree}
:hidden:
:caption: The Basics

jobs
dispatching
retries-and-timeouts
job-context
failed-jobs
```

```{toctree}
:hidden:
:caption: Digging Deeper

fastapi
workers
serialization
testing
observability
local-development
extending
```

```{toctree}
:hidden:
:caption: Reference

cli
environment-variables
laravel-parity
api/index
```
