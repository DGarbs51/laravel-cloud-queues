# Creating Jobs

## Introduction

A job is an ordinary Python function, sync or async, registered with a **registry**. The
registry maps each job's stable *wire name* to its handler, so the worker knows which
function to call when a message arrives.

FastAPI applications create their registry through the `LaravelCloudQueues` integration.
Plain Python applications create a `Registry` directly. Both expose the same `job`
decorator.

## Defining Jobs

### With FastAPI

`LaravelCloudQueues(app)` binds a registry to your FastAPI application. Decorate a
function with `@queues.job` to turn it into a job:

<!-- runnable -->
```python
from fastapi import FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues

app = FastAPI()
queues = LaravelCloudQueues(app)


@queues.job(name="emails.send")
async def send_email(user_id: int, template: str = "welcome") -> None:
    print(f"sending {template} to user {user_id}")
```

FastAPI jobs may also use `Depends()`, just like your routes. See the
[FastAPI integration](fastapi.md) for details.

### Without a Framework

For plain Python applications, scripts and services, create a `Registry`:

<!-- runnable -->
```python
from laravel_cloud_queues import Registry

registry = Registry()


@registry.job(name="reports.build")
def build_report(account_id: int) -> None:
    print(f"building report for account {account_id}")
```

The worker accepts the registry itself as its target:

```shell
laravel-cloud-queues work myapp.jobs:registry
```

## Job Names

The `name` you give a job is its **wire name**: the identifier stored in every queued
message. When you omit it, the name defaults to the function's import path, such as
`myapp.jobs.send_email`.

:::{warning}
Always give your jobs an explicit `name`. A name derived from the import path changes
when you move or rename the function, and every message already on the queue with the
old name then fails as an unknown job.
:::

Each name may only be registered once per registry. Registering a duplicate name raises a
`ConfigurationError`. A good convention is a dotted `area.action` name, such as
`emails.send` or `invoices.render`.

## Job Options

The `job` decorator accepts a default queue and a retry policy:

```python
@queues.job(
    name="emails.send",
    queue="emails",
    tries=5,
    backoff=[1, 5, 30, 120],
    timeout=60,
    fail_on_timeout=False,
)
async def send_email(user_id: int) -> None: ...
```

| Option | Description |
|---|---|
| `name` | The job's wire name |
| `queue` | The default queue the job is dispatched to. See [Customizing the Queue](dispatching.md#customizing-the-queue) |
| `tries` | The total number of deliveries allowed |
| `backoff` | The delay before each retry |
| `timeout` | How long a single delivery may run |
| `fail_on_timeout` | Fail the job on its first timeout |
| `policy` | A reusable `RetryPolicy` |

The retry options are covered in depth in [Retries and Timeouts](retries-and-timeouts.md).

## Calling Jobs Directly

Jobs remain normal callables. Calling a job runs the handler immediately, in the current
process, without involving a queue:

```python
await send_email(1)
```

Direct calls do not run FastAPI dependency injection, so pass any injected values
yourself.

## Sync and Async Handlers

Both kinds of handler are supported, and you may mix them freely in one application:

- **Async handlers** (`async def`) run on the worker's event loop, which lives for the
  whole worker process. Async clients and connection pools created at startup keep
  working across jobs.
- **Sync handlers** (`def`) run directly on the worker's main thread, so the timeout
  signal can interrupt them.

A worker runs **one job at a time**. Async jobs do not run concurrently inside one worker;
to process more jobs in parallel, run more worker processes.

## Handler Parameters

A job's parameters are its payload. When you dispatch a job, its arguments are encoded
into the message, and the worker decodes them using your type annotations. Keep these
rules in mind:

- Annotate every parameter. The annotation drives both validation and decoding, and
  unsupported annotations are rejected at registration.
- `*args` and `**kwargs` are not allowed in FastAPI jobs, since every parameter must be
  validated.
- Parameters annotated `JobContext`, and FastAPI `Depends()` parameters, are *injected*
  at run time and never serialized.

The supported argument types, and how to add your own, are covered in
[Arguments and Payloads](serialization.md).

## Registering Job Modules

The worker needs to import every module that registers jobs before it starts pulling
messages. With FastAPI, the modules imported by your `app` usually take care of this.
When your jobs live in modules your target does not import, tell the registry about
them:

```python
registry = Registry(include=["myapp.jobs.emails", "myapp.jobs.billing"])
```

`include` modules are imported when the worker starts. As an opt-in convenience,
`discover` walks whole packages and imports every module inside them:

```python
registry = Registry(discover=["myapp.jobs"])
```

With FastAPI, import your job modules from the module that defines your `app`, after
creating the integration, so that both the web process and the worker register them:

```python
app = FastAPI()
queues = LaravelCloudQueues(app)

from myapp import jobs  # noqa: E402, F401  (registers the jobs on ``queues``)
```

Job lookup is registry-only. The worker never imports a module because a message named it,
and an unknown job name is a terminal failure.

## Listing Registered Jobs

You may look up a job by its name, or list every registered job:

```python
job = registry.get("emails.send")
all_jobs = registry.jobs()  # A read-only mapping of name to job
```

`registry.get` raises `UnknownJobError` for names that are not registered. From the
command line, `laravel-cloud-queues inspect` prints every registered job and its declared
policy.
