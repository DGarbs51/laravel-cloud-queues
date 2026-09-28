# laravel-cloud-queues

Laravel Cloud queues for Python applications. Dispatch jobs from a FastAPI (or plain
Python) process, run them in a separate worker process, and let Laravel Cloud host the
queue: managed queues once the platform enables them for Python, and today, on worker
clusters, your own SQS queues or a Laravel Valkey cache.

The package feels like Python and FastAPI. It matches Laravel's queue *infrastructure*
contract (SQS message lifecycle, retries, FIFO, timeouts, Laravel Cloud's queue agent and
observability events), not Laravel's PHP API.

> **Platform status (verified 2026-09-27).** Laravel Cloud runs Python 3.10–3.14
> applications, including FastAPI, but **managed queues are not yet available for
> Python**: the Cloud API rejects managed-queue creation for FastAPI applications, and
> Python containers receive no managed queue configuration, no queue agent and no AWS
> credentials. **Worker clusters do run Python today**, so this package ships two
> self-managed backends that work on Laravel Cloud now: `LARAVEL_CLOUD_QUEUES_BACKEND=sqs`
> (your own SQS queues) and `LARAVEL_CLOUD_QUEUES_BACKEND=redis` (a Laravel Valkey cache).
> Managed mode is built and tested against the pinned Laravel contract and local
> emulators; live verification waits on the platform. See
> [Running on Laravel Cloud today](#running-on-laravel-cloud-today) and
> [`docs/audits/2026-09-27/platform-findings.md`](docs/audits/2026-09-27/platform-findings.md).

## Contents

- [Quick start (FastAPI)](#quick-start-fastapi)
- [Running on Laravel Cloud today](#running-on-laravel-cloud-today)
- [Configuration](#configuration)
- [Jobs](#jobs): queues, delays, retries, timeouts, FIFO, fair queues, `JobContext`,
  dependencies, arguments and payloads
- [The worker](#the-worker): lifecycle, flags, exit codes, shutdown
- [Delivery guarantees and failures](#delivery-guarantees-and-failures)
- [Observability](#observability)
- [Local development](#local-development)
- [Testing your application](#testing-your-application)
- [Demo and conformance suite](#demo-and-conformance-suite)
- [Support matrix and public API](#support-matrix-and-public-api)
- [Known limitations and roadmap](#known-limitations-and-roadmap)
- Further reading: [`docs/architecture.md`](docs/architecture.md),
  [`docs/deviations.md`](docs/deviations.md), [`docs/decisions.md`](docs/decisions.md)

## Quick start (FastAPI)

### 1. Install

```sh
pip install "laravel-cloud-queues[fastapi]"
```

Extras: `[fastapi]` (the FastAPI adapter), `[redis]` (the Redis/Valkey backend, needed on
Laravel Cloud today), `[otel]` (OpenTelemetry trace propagation). The core package needs no
extra and depends on `boto3`, `anyio`, `httpx` and `typing-extensions`. Pydantic v2 models
are supported whenever Pydantic is installed (FastAPI brings it).

```sh
pip install "laravel-cloud-queues[fastapi,redis]"
```

### 2. Bind the integration to your app and declare a job

`LaravelCloudQueues(app)` binds a job registry to your FastAPI app. `@queues.job` turns an
ordinary function (sync or async) into a job you can call directly or dispatch to a queue.

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

The explicit `name="emails.send"` is the job's wire name: the identifier stored in every
queued message. Without it the default is the import path (`myapp.jobs.send_email`), which
breaks queued messages when you move or rename the function. Prefer explicit names.

Jobs stay normal callables: `await send_email(1)` runs the handler right here, with no
queue involved (and without dependency injection; see [Dependencies](#fastapi-dependencies-in-jobs)).

### 3. Dispatch

```python
@app.post("/signup")
async def signup(user_id: int) -> dict[str, str]:
    receipt = await send_email.dispatch_async(user_id, template="welcome")
    return {"message_id": receipt.message_id, "queue": receipt.queue}
```

- `await job.dispatch_async(...)` never blocks the event loop: every blocking SQS or Redis
  call runs in a worker thread. Use it in async code.
- `job.dispatch(...)` is the blocking form for scripts, sync code and tests. Inside a
  running event loop it still works (it never starts a nested loop) but it blocks that
  loop; async code should use `dispatch_async`.
- Both take **exactly the handler's parameters**, so `mypy --strict` checks your arguments.
  Prefer keyword arguments: they survive signature changes better than positional ones.
- Both return a `DispatchReceipt(message_id, queue, uuid)`. Jobs are fire-and-forget:
  there is no result handle, and handler return values are ignored.

Laravel Cloud cuts web requests off after 20 seconds (`NGINX_HTTP_TIMEOUT=20`), so keep
dispatch inside requests short; a single dispatch is one SQS `SendMessage` or Redis call.

### 4. Configure a backend

Nothing is selected implicitly. Set `LARAVEL_CLOUD_QUEUES_BACKEND` (or, in managed mode,
let Laravel Cloud inject `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`). For a local Redis or
Valkey:

```sh
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0
```

The full variable tables are under [Configuration](#configuration).

### 5. Run the worker

```sh
laravel-cloud-queues work myapp.main:app
```

`myapp.main:app` is the same `module:attribute` target you give uvicorn. The worker
imports it, finds the bound `LaravelCloudQueues`, enters your app's lifespan once, then
processes one job at a time until it is told to stop. Run it as a separate process; on
Laravel Cloud, as a worker cluster or a background process (next section). Scale by
running more worker processes, not with in-process concurrency.

`laravel-cloud-queues inspect myapp.main:app` prints the resolved mode, queues and
registered jobs without touching the broker or printing secrets.

## Running on Laravel Cloud today

Laravel Cloud worker clusters and App-cluster background processes run Python. Point one at
the worker command and pick a backend.

### Redis backend: Laravel Valkey (`redis`)

Attach a Laravel Valkey cache to the environment and set:

```text
LARAVEL_CLOUD_QUEUES_BACKEND=redis
```

Laravel Cloud injects `REDIS_URL` (`rediss://...caches.laravel.cloud:6379/0`, TLS) for the
attached cache. **Only in `redis` mode** does the package fall back to `REDIS_URL` when
`LARAVEL_CLOUD_QUEUES_REDIS_URL` is unset; `REDIS_URL` on its own never selects a
backend, because applications commonly attach Valkey just for caching. Set
`LARAVEL_CLOUD_QUEUES_REDIS_URL` explicitly to use a different Redis, or a different
database number, from the cache your application uses. `rediss://` enables TLS with
certificate verification on. Install the `[redis]` extra.

Worker command for the cluster:

```text
laravel-cloud-queues work myapp.main:app
```

The Redis backend follows Laravel's `redis` queue driver semantics: a pending list plus
delayed and reserved sorted sets under `LARAVEL_CLOUD_QUEUES_REDIS_PREFIX` (default
`laravel-cloud-queues:`), atomic Lua scripts for reserve/release/delete, and attempt counts
per reservation. Several workers across clusters share one queue without double delivery.
FIFO and fair-queue options are not available in this mode (they raise
`InvalidQueueOptionError`).

### SQS backend: your own queues (`sqs`)

Create SQS queues in your AWS account and set:

```text
LARAVEL_CLOUD_QUEUES_BACKEND=sqs
LARAVEL_CLOUD_QUEUES_SQS_PREFIX=https://sqs.us-east-2.amazonaws.com/123456789012
LARAVEL_CLOUD_QUEUES_SQS_REGION=us-east-2
LARAVEL_CLOUD_QUEUES_SQS_KEY=...
LARAVEL_CLOUD_QUEUES_SQS_SECRET=...
```

#### Why `sqs` mode has its own variables: the `AWS_*` collision

When you attach Laravel Cloud object storage (Cloudflare R2), the platform injects the
**standard AWS variable names** into every container: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION=auto`, `AWS_DEFAULT_REGION=auto`,
`AWS_BUCKET`. boto3 honors `AWS_ENDPOINT_URL` for every service, so an SQS client built
from the default credential chain in that container would send SQS requests to R2, with R2
keys and region `auto`.

This package therefore never reads `AWS_*` in `sqs` mode. Every setting is passed to the
boto3 client explicitly from `LARAVEL_CLOUD_QUEUES_SQS_*`; `AWS_ENDPOINT_URL` and
`AWS_ENDPOINT_URL_SQS` are ignored unless you set `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT`.
Credentials are required. If you deliberately want boto3's default chain (local AWS
profiles, IAM roles on your own infrastructure), opt in with
`LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default`; it is never implicit. The same rule applies
in managed mode: the injected config's `credentials: "ecs"` selects the ECS container
credential provider explicitly, never the default chain.

#### Visibility and long jobs

There is no Laravel Cloud queue agent outside managed mode, so the worker renews the
message's visibility timeout itself: it receives with a 60-second lease and a watchdog
thread extends it every 20 seconds while the job runs, even while a sync handler blocks
the event loop. A renewal failure (the message was deleted or reassigned) is a lost lease:
the worker never reports success for a job it no longer owns, and exits 1.

### Worker clusters are supervised services

On worker clusters and as App-cluster background processes, Laravel Cloud runs the worker
as a **long-lived service and restarts it whenever it exits**, for any reason. Consequences:

- Do **not** use `--stop-when-empty` or `--stop-when-empty-for` there: a worker that exits
  on an empty queue is restarted immediately, in a loop. Both default to off.
- `--max-jobs N` and `--max-time S` remain useful for recycling the process periodically.
  The FastAPI lifespan runs again in the new process.
- A configuration error (exit 2) restart-loops. The worker logs it clearly on every start;
  check the Logs tab.
- After a job timeout the worker exits 124 and is restarted (see [Timeouts](#timeouts)).
- Worker clusters scale on **CPU, memory or a fixed instance count, not on queue depth**.
  Size the cluster for your expected throughput.
- Laravel Cloud gives **Flex** workers 90 seconds to finish on shutdown and caps job
  runtime at 90 seconds; **Pro** workers have no fixed runtime limit and one hour to finish.
  Keep job timeouts inside those limits.

### Live smoke test on Laravel Cloud

[`probe-app/`](probe-app/README.md) is a repository-only FastAPI probe that runs the real
package on Laravel Cloud today: its App and worker clusters run
`python -m laravel_cloud_queues.cli work main:app` as background processes in `redis` mode
against the environment's Laravel Valkey, and its routes dispatch one job per case (success,
delay, retry, terminal failure, timeout, bursts) and report what the workers recorded. Use
it as the template for a live check of your own deployment. It is not shipped in the
package, and its `/verify` route dumps the container environment, so deploy it only to a
throwaway environment.

### Managed queues (when Laravel Cloud enables them for Python)

In managed mode the platform injects `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`; the package
auto-detects it and needs no manual AWS or queue wiring: queue URL prefix and suffix,
region, credential provider, the worker's queue assignment and whether the in-container
queue agent is enabled all come from that document. Dispatch goes straight to Laravel
Cloud's SQS; the worker receives through the agent's Unix socket (`GET /next`,
`POST /result`) when the agent is enabled, or directly from SQS otherwise. Lifecycle and
`failed_job` events go to Laravel Cloud's log socket so jobs appear in the Queues
dashboard, and failed jobs can be inspected and retried there. The agent extends a
running job's visibility in three-minute increments and owns worker lifetime and scaling.

The Cloud queue assignment is authoritative: a `--queue` that differs from it is a startup
configuration error, not an override.

## Configuration

Backend selection is explicit and never inferred from ambient variables:

| Mode | Selected when | Broker | Receive |
|---|---|---|---|
| `managed` | `LARAVEL_CLOUD_QUEUES_BACKEND=managed`, or unset while `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present | Laravel Cloud SQS | Queue agent when enabled, else direct SQS |
| `sqs` | `LARAVEL_CLOUD_QUEUES_BACKEND=sqs` | Your SQS queues | Direct SQS |
| `redis` | `LARAVEL_CLOUD_QUEUES_BACKEND=redis` | Redis/Valkey | Redis transport |

With no backend and no managed config, `load_config()` raises `ConfigurationError` (the
worker exits 2). Every setting can also be passed in code, and code wins over the
environment:

```python
from laravel_cloud_queues import Registry, load_config

config = load_config(backend="redis", redis_url="redis://127.0.0.1:6379/0")
registry = Registry(config=config)
```

### `sqs` mode

| Variable | Meaning |
|---|---|
| `LARAVEL_CLOUD_QUEUES_SQS_PREFIX` | Queue URL prefix, e.g. `https://sqs.us-east-2.amazonaws.com/<account>` (required) |
| `LARAVEL_CLOUD_QUEUES_SQS_SUFFIX` | Optional queue-name suffix appended to logical names |
| `LARAVEL_CLOUD_QUEUES_SQS_QUEUE` | Default queue (default `default`) |
| `LARAVEL_CLOUD_QUEUES_SQS_REGION` | Region (required, never defaulted) |
| `LARAVEL_CLOUD_QUEUES_SQS_KEY` / `LARAVEL_CLOUD_QUEUES_SQS_SECRET` | Credentials (required unless `_SQS_CREDENTIALS=default`) |
| `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS` | `default` opts into boto3's default credential chain. Never implicit |
| `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` | Endpoint override for LocalStack/moto. Refused when managed config is present |

Queue names resolve like Laravel's `sqs` driver: `{prefix}/{queue}{suffix}`, FIFO names as
`{prefix}/{base}{suffix}.fifo`, the suffix added only if not already present, and a value
that is already a full URL passes through. Laravel Cloud queue names are at most 39
characters including `.fifo`. Dispatching to a queue that does not exist raises
`ManagedQueueNotFoundError`; queues are never created or silently substituted.

### `redis` mode

| Variable | Meaning |
|---|---|
| `LARAVEL_CLOUD_QUEUES_REDIS_URL` | Connection URL. Falls back to `REDIS_URL` **only in `redis` mode**. `rediss://` enables TLS |
| `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE` | Default queue (default `default`) |
| `LARAVEL_CLOUD_QUEUES_REDIS_PREFIX` | Key prefix (default `laravel-cloud-queues:`) |

### `managed` mode

| Variable | Meaning |
|---|---|
| `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` | Injected by Laravel Cloud: `{"driver": "cloud", "queue": ..., "queues": [...], "connection": {"prefix", "suffix", "queue", "region", "credentials": "ecs"}, "agent": {"enabled", "socket"}}` |
| `LARAVEL_CLOUD_AGENT_SOCKET` | Agent socket when the config omits it (default `/tmp/cloud-agent.sock`) |
| `LARAVEL_CLOUD_LOG_SOCKET` | Observability socket (default `unix:///tmp/cloud-init.sock`) |

Malformed JSON, a `driver` other than `cloud`, a missing `connection` or `region`, or a
`credentials` value other than `ecs`/`instance` is a configuration error. `after_commit`,
`overflow` and `credential_cache` are parsed and preserved but not implemented; the worker
logs a warning when either of the latter two is enabled. Credentials, receipt handles and
job payloads are never logged by default.

## Jobs

### Named queues

A job can declare its default queue; a dispatch can override it with `.options(...)`,
which returns a typed copy of the job carrying dispatch options. `dispatch`/`dispatch_async`
keep the handler's own signature, so options never collide with handler parameters, even
ones named `queue`, `delay` or `timeout`.

<!-- runnable -->
```python
from laravel_cloud_queues import Registry

registry = Registry()


@registry.job(name="reports.build", queue="reports")
def build_report(account_id: int, queue: str = "monthly") -> None:
    # ``queue`` here is an ordinary handler parameter.
    print(f"building {queue} report for {account_id}")


with registry.testing() as recorder:
    build_report.dispatch(account_id=7)
    build_report.options(queue="priority").dispatch(account_id=8, queue="daily")

assert [d.queue for d in recorder.dispatched] == ["reports", "priority"]
```

(`registry.testing()` is the eager test mode; see [Testing your application](#testing-your-application).)

Without a job-level or per-dispatch queue, the backend's default queue is used
(`LARAVEL_CLOUD_QUEUES_SQS_QUEUE`, `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE`, or the managed
config's `connection.queue`). The worker consumes the backend default queue unless you pass
`--queue emails,default`: a comma-separated priority list polled in order (direct SQS and
Redis modes only; in agent mode the Cloud assignment wins).

### Delays

`.options(delay=...)` accepts `int`/`float` seconds or a `timedelta`. Positive fractional
delays round **up** to the next whole second, so a short delay never becomes immediate.
The maximum is **900 seconds** (SQS's per-message limit; applied to Redis too so behavior is
portable). Negative, non-finite or longer delays raise `InvalidQueueOptionError` before
anything is sent. FIFO queues do not support per-message delay; a positive delay on a
`.fifo` queue is rejected.

<!-- runnable -->
```python
from datetime import timedelta

from laravel_cloud_queues import InvalidQueueOptionError, Registry

registry = Registry()


@registry.job(name="reminders.send")
async def send_reminder(user_id: int) -> None: ...


with registry.testing(eager=False) as recorder:
    send_reminder.options(delay=timedelta(minutes=5)).dispatch(user_id=1)
    send_reminder.options(delay=0.2).dispatch(user_id=2)
    try:
        send_reminder.options(delay=901).dispatch(user_id=3)
    except InvalidQueueOptionError:
        pass

assert [d.delay_seconds for d in recorder.dispatched] == [300, 1]
```

Retries are different from fresh delays: retry backoff uses SQS message visibility (or the
Redis delayed set) and may extend up to 12 hours.

### Retry policy

```python
@queues.job(name="emails.send", tries=5, backoff=[1, 5, 30, 120], timeout=60, fail_on_timeout=False)
async def send_email(user_id: int) -> None: ...
```

| Field | Default | Meaning |
|---|---|---|
| `tries` | `1` | Total deliveries allowed. `0` = unlimited. The default is one attempt, like Laravel's worker |
| `backoff` | `0` | Seconds before a retry: a number, or a list indexed by attempt (`backoff[attempt - 1]`; the last value repeats). Positive fractions round up; the delay is clamped to 43,200 s (12 h) |
| `timeout` | `60` (worker `--timeout`) | Seconds a delivery may run. `0` disables. Maximum 604,800 (7 days) |
| `fail_on_timeout` | `False` | Fail terminally on the first timeout instead of letting the message be redelivered |

The same fields are available as a reusable `RetryPolicy(...)` object via
`@queues.job(policy=...)`; shorthand fields override the policy's fields.

Attempt counting follows Laravel's worker exactly. The delivery attempt comes from the
transport (SQS `ApproximateReceiveCount`, or the Redis reservation counter), never from the
message body:

- **Before running:** if `tries > 0` and `attempt > tries`, the job is failed without
  running (`MaxAttemptsExceededError`). This catches deliveries that exceeded the budget
  through crashes or timeouts.
- **After an exception:** if `tries > 0` and `attempt >= tries` the failure is terminal;
  otherwise the **same message** is released for retry with the configured backoff, by
  changing its SQS visibility or moving it to the Redis delayed set. No duplicate message
  is ever created.

**The policy travels in the message.** At dispatch the effective `tries`, `backoff`,
`timeout` and `fail_on_timeout` are written into the envelope, and the worker follows the
message; worker defaults apply only to fields the job did not declare. A deploy never
changes the rules for jobs already queued, and a dashboard retry keeps its original rules.

### Timeouts

Timeouts are **process-level**, matching Laravel's worker, not asyncio cancellation. Before
each job the worker arms `signal.setitimer` with the job's timeout (from the message, else
`--timeout`, default 60 s). When it fires, the handler decides whether this delivery is
terminal (last attempt, or `fail_on_timeout`), writes the failure record or lifecycle event,
and exits the process immediately with **exit code 124**. A retryable timeout does not
release the message and applies no backoff: the message comes back through SQS visibility
or Redis reservation expiry with an incremented attempt count, and Laravel Cloud restarts
the worker. Sync handlers run on the worker's main thread precisely so the alarm can
interrupt them.

Known limitation, shared with Laravel: Python runs signal handlers between bytecode
instructions. A job blocked inside native code (a long C call, a big `sum(range(n))`) can
overrun its timeout until control returns to the interpreter. Pure-Python loops,
`await asyncio.sleep`, `time.sleep`, `re.match` and `hashlib` calls were measured as
interruptible within about 10 ms; see
[`tests/runtime_proof/FINDINGS.md`](tests/runtime_proof/FINDINGS.md). There is no
supervisor process in v1.

### FIFO queues

Queues whose logical name ends in `.fifo` use SQS FIFO semantics (SQS and managed modes).

```python
@queues.job(name="ledger.post", queue="ledger.fifo")
async def post_entry(account_id: int, amount: int) -> None: ...

# Default group = the queue name including ".fifo": the whole queue is serialized.
await post_entry.dispatch_async(account_id=1, amount=500)

# Per-account ordering and an explicit business deduplication ID.
await post_entry.options(
    group="account-1", deduplication_id="txn-8f1c"
).dispatch_async(account_id=1, amount=500)

# Empty dedup ID = omit the attribute and rely on the queue's content-based deduplication.
await post_entry.options(deduplication_id="").dispatch_async(account_id=1, amount=500)
```

- Default `group`: the logical queue name **including `.fifo`** (Laravel's convention). This
  serializes the whole queue; pass `group=` for per-tenant ordering.
- Default `deduplication_id`: a fresh unique ID per logical dispatch, chosen once and
  reused across internal network retries.
- `deduplication_id=""` omits the attribute so the queue's content-based deduplication
  applies. Note that every envelope contains a fresh `uuid` and trace data, so two identical
  business calls still differ; pass an explicit business ID when you want them collapsed.
- Group and deduplication IDs must be 1–128 printable ASCII characters (SQS rules) and are
  validated before sending. A positive delay on a FIFO queue is rejected.

### Fair queues (message groups on standard queues)

On **standard** SQS queues, `message_group=` sets the SQS message group used by fair
queues, so one noisy tenant does not starve the others:

```python
await send_email.options(message_group=f"tenant-{tenant_id}").dispatch_async(user_id=1)
```

`message_group` (fair queue, standard queues) and `group`/`deduplication_id` (FIFO) are
different models. Passing FIFO options to a standard queue, or `message_group` to a `.fifo`
queue, raises `InvalidQueueOptionError` rather than silently dropping attributes. The
Redis backend has neither model; all of these options are rejected in `redis` mode.

### `JobContext`: attempt, queue, release, fail

Inside a handler, `current_job()` returns the typed `JobContext` of the running delivery:
`job_name`, `uuid`, `message_id`, `queue`, `attempt`, `max_tries`, plus two control calls.
With FastAPI use `Depends(current_job)`.

<!-- runnable -->
```python
from laravel_cloud_queues import JobContext, Registry, current_job

registry = Registry()


@registry.job(name="payments.capture", tries=3, backoff=[5, 30])
def capture_payment(payment_id: str) -> None:
    job: JobContext = current_job()
    status = lookup_status(payment_id)
    if status == "pending":
        # Try again in 60 s. Consumes an attempt like any delivery.
        job.release(delay=60)
    if status == "cancelled":
        # Terminal now, regardless of remaining attempts.
        job.fail(f"payment {payment_id} was cancelled")
    print(f"captured {payment_id} on attempt {job.attempt} of {job.max_tries}")


def lookup_status(payment_id: str) -> str:
    return "captured"


with registry.testing():
    capture_payment.dispatch(payment_id="pay_1")
```

- `release(delay=0)` releases **this** message for another delivery after `delay` seconds
  (`int`/`float`/`timedelta`, rounded up, clamped to 12 h). It always releases (Laravel
  parity); when attempts are exhausted, the next delivery fails the pre-run check.
- `fail(reason=None)` fails the job terminally now. `reason` may be a string or an
  exception; it becomes the failure record's exception (`JobFailedError` by default).
- Both raise a control-flow exception to leave the handler. If your code swallows it, the
  recorded outcome still wins over success. They perform no I/O inside the handler; the
  worker reports the outcome once, after teardown.
- `JobContext` is a runtime object: it is never serialized and can never be supplied from
  message data. A handler parameter annotated `JobContext` is also injected, but it then
  appears in the typed `dispatch` signature, so the `current_job()` pattern is preferred for
  strictly typed code.

### FastAPI dependencies in jobs

Jobs support `Depends()` like route handlers. Each delivery gets a fresh dependency scope:
sub-dependencies are cached within one job, `yield` teardown runs on success, failure and
release, `app.dependency_overrides` is honored, and teardown completes **before** the
outcome is acknowledged. A teardown exception turns success into a handler failure (retry
policy applies).

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
    app.state.mailer = Mailer("smtp://localhost")  # process-scoped resource
    yield


app = FastAPI(lifespan=lifespan)
queues = LaravelCloudQueues(app)


async def get_mailer() -> AsyncIterator[Mailer]:
    yield app.state.mailer  # teardown after the ``yield`` runs before acknowledgement


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

Only `user_id` and `template` are serialized; injected parameters are excluded from the
payload and from `dispatch`'s typed signature. The worker enters your app's lifespan once
per process, so `app.state` resources (and identifier keys yielded by a lifespan) are
available to jobs exactly as they are to routes. Request-only dependencies (`Request`,
`WebSocket`, `Response`, `BackgroundTasks`, `Security()`) have no meaning in a job and raise
`ConfigurationError` at registration. Direct calls (`await send_email(1, "welcome", mailer=m, job=...)`)
do not run dependency injection; pass injected values yourself.

### Arguments and payloads

Messages are a versioned JSON envelope. Argument decoding is **driven by the handler's type
annotations**: the payload never names a Python type, module or callable, and the worker
never imports anything because a message asked it to. Supported argument types: JSON
primitives, `list`/`dict`, `tuple` (with element annotations), dataclasses, enums, `UUID`,
`datetime`/`date`/`time`, `Decimal`, `bytes`, Pydantic v2 models when Pydantic is
installed, and custom codecs registered on the registry's `CodecRegistry`. Decoded
arguments are validated against the handler signature before execution.

<!-- runnable -->
```python
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from laravel_cloud_queues import Registry

registry = Registry()


class Priority(Enum):
    LOW = "low"
    HIGH = "high"


@dataclass(frozen=True)
class Invoice:
    id: UUID
    issued_at: datetime
    lines: tuple[str, ...]


@registry.job(name="invoices.render")
def render_invoice(invoice: Invoice, priority: Priority = Priority.LOW) -> None:
    assert isinstance(invoice.id, UUID) and isinstance(invoice.issued_at, datetime)
    print(f"rendering {invoice.id} at {priority.value} priority")


with registry.testing():
    render_invoice.dispatch(
        Invoice(uuid4(), datetime.now(timezone.utc), ("Widget", "Gadget")), priority=Priority.HIGH
    )
```

Payload rules and limits:

- **Size.** The encoded body is measured in UTF-8 bytes before sending. SQS and managed
  queues allow **1,048,576 bytes** (1 MiB, Laravel Cloud's documented job payload limit);
  larger payloads raise `PayloadTooLargeError` with `.size` and `.limit`, and a queue's
  lower `MaximumMessageSize` rejection maps to the same error. The Redis backend has no
  transport limit of its own; it is bounded only by the package's 16 MiB envelope decode
  ceiling, above which dispatch also raises `PayloadTooLargeError`. **Moving a
  Redis-backed application to SQS or managed queues reintroduces the 1 MiB limit.** Nothing
  is truncated, compressed or offloaded in v1 (see the roadmap).
- **Trust boundary.** Message bodies are untrusted input. No pickle, ever. Decoding is
  bounded in size and nesting depth; NaN/Infinity, duplicate keys and invalid UTF-8 are
  rejected. Malformed envelopes, unsupported envelope versions, unknown job names, codec
  failures and argument mismatches are deterministic defects: they fail on first delivery
  and are never retried.
- **Never put secrets in job arguments.** Failure records carry the full payload (so a job
  can be retried from the Laravel Cloud dashboard) into logs and the dashboard. Pass
  identifiers and look secrets up inside the handler.
- The producer and the worker are the same application but may run on different hardware
  and on different deployment revisions. Never rely on process-local objects, memory or
  filesystem state in a payload.

## The worker

```text
laravel-cloud-queues work TARGET [--queue Q[,Q...]] [--max-jobs N] [--max-time S]
                              [--stop-when-empty] [--stop-when-empty-for S]
                              [--timeout S] [--sleep S] [--rest S] [--debug]
```

`TARGET` is `module:attribute`: a FastAPI app with `LaravelCloudQueues` bound, a plain
`Registry`, or any object exposing `registry` and `lifespan()`. The current directory is
put on `sys.path`, like uvicorn.

| Option | Default | Meaning |
|---|---|---|
| `--queue Q[,Q...]` | backend default | Priority list (direct SQS and Redis). In agent mode it must match the Cloud assignment or be omitted |
| `--max-jobs N` | off | Stop after N deliveries (exit 0) |
| `--max-time S` | off | Stop after S seconds, checked between jobs (exit 0) |
| `--stop-when-empty` | off | Stop on the first empty poll. **Not for supervised worker clusters** |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job. Same warning |
| `--timeout S` | 60 | Default job timeout when the message omits one; `0` disables |
| `--sleep S` | 3 | Wait after an empty poll (multi-queue SQS); blocking-pop wait (Redis) |
| `--rest S` | 0 | Pause between jobs |
| `--debug` | off | Show tracebacks; errors are one actionable line otherwise |

Behavior:

- **One in-flight job per process.** Async handlers run on the AnyIO/asyncio loop; sync
  handlers run directly on the main thread so the timeout signal can interrupt them.
  Scale horizontally with more worker processes or instances.
- **Polling.** A single SQS queue long-polls (`WaitTimeSeconds=20`,
  `MaxNumberOfMessages=1`) with no extra sleep after an empty poll. Several `--queue`
  entries are short-polled in priority order, then the worker sleeps `--sleep` if all were
  empty. Redis uses a bounded blocking pop of `--sleep` seconds; there is no busy polling.
  Transient receive errors are logged, followed by a 1-second sleep and a retry.
- **Graceful shutdown (`SIGTERM`/`SIGINT`).** The worker stops fetching, lets the current
  job finish, reports its outcome, runs dependency teardown, exits the app lifespan and
  exits 0. Repeated signals never skip reporting. If a signal arrives during an idle SQS
  long poll, the worker waits for that poll to return (up to 20 s) and runs any message it
  hands over rather than abandoning a message it now holds. Laravel Cloud sends `SIGTERM`
  on deploy and gives Flex workers 90 seconds and Pro workers one hour to finish; if the
  platform kills the process anyway, SQS visibility or Redis reservation expiry redelivers
  the job.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Clean stop (signal, `--max-jobs`, `--max-time`, `--stop-when-empty*`), or the Laravel Cloud agent became unhealthy (Laravel parity) |
| `1` | Other fatal transport error: lost visibility lease, broker connection lost after retries, ambiguous acknowledgement on a direct broker |
| `2` | Configuration error at startup (logged on every start; restart-loops on clusters) |
| `124` | Job timeout |

On worker clusters every exit is followed by a restart, so these codes are diagnostic.

## Delivery guarantees and failures

**Delivery is at least once.** SQS, Redis reservations and the Laravel Cloud agent all
redeliver a message whose worker crashed, timed out or lost its acknowledgement. After an
ambiguous acknowledgement (for example a lost agent `/result` response) the worker stops
instead of guessing. Design handlers to be **idempotent**: key side effects on the job
`uuid` or a business ID, and tolerate a second delivery.

**Terminal failures.** A job is failed terminally when its last attempt raises, when
`JobContext.fail()` is called, when a delivery exceeds `tries` before running, on a
terminal timeout, or on a deterministic defect (malformed message, unknown job, argument
mismatch, Laravel overflow `@pointer` body).

- In **managed mode** the message is completed, then a `failed_job` event and a `failed`
  lifecycle event are sent to Laravel Cloud, which owns failed-job inspection and retry.
  A dashboard retry re-queues the envelope verbatim; the worker runs it as a fresh first
  attempt with its original retry policy.
- In **`sqs` and `redis` modes** the worker writes the full failure record as **one JSON
  line to stdout** (visible in Laravel Cloud's Logs tab) and then deletes the message. There
  is no Python failed-job store, dead-letter queue or retry command in v1; re-running a
  failed job means dispatching it again.

**Failure records are best-effort.** In managed mode the message is completed before the
record is written, so a log-socket outage at that moment loses the record (the same trade
Laravel makes). In `sqs`/`redis` modes the line is written before deletion, but nothing
stores it beyond your logs.

**Error classes.** Every package error has one classification, exported from
`laravel_cloud_queues`:

| Class | Examples | Behavior |
|---|---|---|
| Dispatch error (`DispatchError`) | `ConfigurationError`, `ManagedQueueNotFoundError`, `PayloadTooLargeError`, `InvalidQueueOptionError`, `SerializationError`, `ArgumentError` | Raised to the caller; nothing is sent |
| Job defect (`JobDefectError`) | malformed envelope, unsupported version, unknown job, codec or argument mismatch, `@pointer` body | Terminal on first delivery |
| Handler failure | any exception from the handler or its teardown | Retry policy applies |
| Fatal worker error | agent unhealthy, lost lease, ambiguous acknowledgement | Worker stops |

## Observability

**Managed mode.** The package emits Laravel Cloud's queue lifecycle events (`queued`,
`started`, `processed`, `released`, `failed`, with normalized queue names and
`duration_ms`) and Laravel's `failed_job` event as newline-delimited JSON over the
observability Unix socket (`LARAVEL_CLOUD_LOG_SOCKET`, default `unix:///tmp/cloud-init.sock`),
so Python jobs appear in the Queues dashboard like PHP jobs. `failed_job` records follow a
size policy for the log collector's line limit: sent whole when they fit, otherwise the
exception is trimmed, then the payload, in which case the record is marked
`"replayable": false`. Observability is best-effort: a socket outage never turns a
successful job into a failed one.

**`sqs` and `redis` modes.** Laravel Cloud currently ingests queue lifecycle events for
managed queues only, so the package sends **no** socket events in these modes. The worker
logs structured lines to stderr/stdout instead: one info line per completed or released
delivery, and one JSON failure record per terminal failure with `queue`, `message_id`,
`attempts`, `job_name`, `started_at`, `failed_at`, `exception_preview`, `exception` and the
original `payload`.

**Tracing.** With the `[otel]` extra installed, dispatch injects W3C trace context
(`traceparent`/`tracestate`) into the envelope and the worker extracts and activates it
around each job, resetting afterwards so context never leaks between jobs. Without the
extra nothing happens and nothing breaks.

## Local development

Pick whichever broker you have:

**Redis or Valkey** (Laravel Herd bundles Valkey on `127.0.0.1:6379`; any Redis works):

```sh
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0
laravel-cloud-queues work myapp.main:app
```

**Local SQS with LocalStack** (or moto's server, which the test suite uses; no Docker
needed):

```sh
export LARAVEL_CLOUD_QUEUES_BACKEND=sqs
export LARAVEL_CLOUD_QUEUES_SQS_PREFIX=http://localhost:4566/000000000000
export LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT=http://localhost:4566
export LARAVEL_CLOUD_QUEUES_SQS_REGION=us-east-1
export LARAVEL_CLOUD_QUEUES_SQS_KEY=test
export LARAVEL_CLOUD_QUEUES_SQS_SECRET=test
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name default
laravel-cloud-queues work myapp.main:app
```

`LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` is the only endpoint override the package honors;
`AWS_ENDPOINT_URL` is ignored, and the override is refused when managed configuration is
present.

**Managed mode locally.** Set `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` to a document like
the one under [Configuration](#managed-mode) with `agent.enabled: false` to exercise direct
SQS receive, or run the repository's agent emulator (`harness/`) for the agent path.

The repository's own tests run SQS against moto locally and LocalStack in CI
(`LARAVEL_CLOUD_QUEUES_TEST_SQS=moto|localstack`), and Redis against
`LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL` (default `redis://127.0.0.1:6379/15`).

## Testing your application

`registry.testing()` (or `queues.registry.testing()` with FastAPI) swaps dispatch for an
in-process test double for the duration of the block. It records every dispatch, with the
real encoded body and options, and never touches a broker or loads configuration.

- `eager=True` (default) runs each dispatched job **immediately**, through the worker's own
  execution path: encode, decode, validate against the handler signature, resolve FastAPI
  dependencies, invoke, tear down. Handler exceptions and `JobContext.fail()` reasons are
  raised to the test; `JobContext.release()` is recorded and the job is not re-run; a job
  defect (an argument that does not round-trip) raises the `JobDefectError`.
- `eager=False` only records, for asserting what would have been dispatched.
- It works inside and outside a running event loop without nesting loops: `dispatch_async`
  awaits the job in your loop; sync `dispatch` of an async job inside a running loop runs it
  on a helper thread.

<!-- runnable -->
```python
from laravel_cloud_queues import Registry

registry = Registry()
sent: list[tuple[int, str]] = []


@registry.job(name="emails.send", queue="emails")
async def send_email(user_id: int, template: str = "welcome") -> None:
    sent.append((user_id, template))


def test_signup_sends_welcome_email() -> None:
    with registry.testing() as recorder:
        send_email.dispatch(user_id=42)

    assert sent == [(42, "welcome")]
    [dispatch] = recorder.for_job("emails.send")
    assert dispatch.queue == "emails"
    assert dispatch.kwargs == {"user_id": 42}


test_signup_sends_welcome_email()
```

What eager mode does **not** prove: retry and backoff behavior, timeouts, visibility,
FIFO ordering, queue existence, payload limits against a real broker, or anything about
the worker process. Those belong to the transport tests and the conformance suite below.

## Demo and conformance suite

`demo/` is an executable FastAPI conformance application, and `demo/conformance` runs every
feature in the conformance catalog ([`docs/contract/catalog.json`](docs/contract/catalog.json))
against local emulators (moto or LocalStack for SQS, a Redis/Valkey server, the Laravel
Cloud agent emulator and observability collector in `harness/`) and reports what matches
the pinned Laravel baseline (`laravel/framework` v13.33.0) and what deliberately deviates.
From a repository checkout:

```sh
uv sync
uv run python -m demo.conformance --sqs moto --report compatibility-report.json
# equivalent entry point:
uv run laravel-cloud-queues conformance --sqs moto --report compatibility-report.json
```

It prints one `PASS`/`FAIL`/`PARTIAL`/`SKIPPED`/`UNSUPPORTED` line per feature plus a
summary, and writes a JSON report with expected and observed behavior, evidence, upstream
source references, evidence tier (`unit`, `socket`, `emulated`, `live`), deviation labels
and environment metadata. The command exits non-zero on any missing catalog record or
unapproved non-pass status; only the two live Laravel Cloud managed-queue checks may be
skipped today. Run a subset with `--only <feature.id> ...`. See
[`demo/README.md`](demo/README.md) for services and options.

**`demo/`, `probe-app/`, `harness/`, `tests/` and `docs/` are repository-only development
tooling. They are not shipped in the PyPI wheel or sdist**, and a packaging test proves it.
`laravel-cloud-queues conformance` from an installed package explains how to run the suite
from a checkout instead of failing obscurely. `probe-app/` is a throwaway FastAPI app
deployed to Laravel Cloud to inspect what the platform injects.

## Support matrix and public API

| | |
|---|---|
| Python | CPython 3.10, 3.11, 3.12, 3.13, 3.14 (CI on Linux) |
| Operating systems | Linux (production runtime on Laravel Cloud), macOS (development). Windows: best effort, not a release gate |
| Frameworks | FastAPI >= 0.121 (Pydantic v2) via `laravel_cloud_queues.fastapi`; plain Python via `Registry`. Django and Flask adapters are planned, not shipped, and have no extras yet |
| Async | AnyIO on the asyncio backend. Trio is not supported |
| Brokers | Laravel Cloud managed queues (pending platform support for Python), SQS (including LocalStack/moto), Redis/Valkey via `redis-py` 5+ |
| Typing | `py.typed`; the package passes `mypy --strict`, and dispatch/`.options()` are checked against your handler signatures |

**Public import surface**, stable within the 0.x line as far as practical:

- `laravel_cloud_queues`: `Registry`, `Job`, `JobContext`, `current_job`, `RetryPolicy`,
  `DispatchReceipt`, `QueueConfig`, `load_config`, `__version__`, and the errors
  `LaravelCloudQueuesError`, `DispatchError`, `ConfigurationError`,
  `ManagedQueueNotFoundError`, `PayloadTooLargeError`, `InvalidQueueOptionError`,
  `SerializationError`, `ArgumentError`, `JobDefectError`.
- `laravel_cloud_queues.fastapi`: `LaravelCloudQueues`, `JobContext`, `current_job`.
- `laravel_cloud_queues.testing`: `DispatchRecorder`, `RecordedDispatch`.
- `laravel_cloud_queues.errors`: the full exception hierarchy.
- `laravel_cloud_queues.config`: configuration models.
- The `laravel-cloud-queues` console script (`work`, `inspect`, `conformance`).

Everything else, in particular **underscore-prefixed modules and subpackages**
(`config/_loader`, `observability/_socket`, `fastapi/_invoker`, ...), is internal and may
change without notice. `worker`, `transports`, `codecs` and `jobs` are extension points
for adapters and are documented in [`docs/architecture.md`](docs/architecture.md); their
signatures may still move during 0.x.

**Versioning.** This is a `0.x` package: the FastAPI API is still settling, conformance
gaps remain, and live Laravel Cloud support for Python is not yet verified. `1.0.0` will be
published only when the core and FastAPI APIs are intentionally stable and the Cloud
contract is verified live; after that, breaking changes require a major version.

## Known limitations and roadmap

Limitations in v1:

- **No compression and no large-payload offload.** Payloads above 1 MiB (SQS/managed) are
  rejected with `PayloadTooLargeError`; Laravel's cache-backed overflow (`@pointer` bodies)
  is not implemented and such a body received by the worker fails deterministically.
- **Timeouts cannot interrupt native code.** A handler blocked in a C extension or a long
  non-Python call overruns its timeout until control returns to the interpreter
  ([details](#timeouts)).
- **Failure records are best-effort**, and there is no failed-job store, dead-letter queue
  or retry command outside Laravel Cloud's dashboard (managed mode only).
- **At-least-once delivery.** Handlers must be idempotent.
- **No lifecycle events in `sqs`/`redis` modes**: Laravel Cloud ingests them for managed
  queues only; workers log structured lines instead.
- One in-flight job per worker process; no result backend; no job chains, batches, unique
  jobs or middleware; no `retry_until`/`max_exceptions`; no memory-limit recycling
  (Laravel Cloud restarts a worker that exceeds its memory allocation and redelivers the
  job); no `after_commit`/credential caching; no PHP payload interoperability.
- Live Laravel Cloud managed-queue behavior is unverified until the platform enables managed
  queues for Python.

Roadmap, in rough priority order:

1. **Transparent payload compression.**
2. **Transparent S3/object-storage offload for large payloads**, with configurable
   thresholds and worker-side hydration and cleanup (Laravel's `SqsQueue::overflow` is the
   reference).
3. Django adapter (`[django]`), then Flask adapter (`[flask]`), on the same core and wire
   contract.
4. Live Laravel Cloud managed-queue verification once the platform allows Python in
   managed-queue validation, injects the managed config and container credentials into
   Python containers, accepts a Python worker command and runs the queue agent in Python
   worker containers.
5. A manual live worker-cluster smoke test on Laravel Cloud.
6. `retry_until` and `max_exceptions` (the envelope already reserves room for them).
7. Dead-letter handling and failed-job tooling outside managed mode.
8. Advanced queue workflow features, only after core compatibility is proven live.
9. Follow upstream Laravel managed-queue changes detected by the weekly drift check.

## License

MIT. See [`LICENSE`](LICENSE).

<!--
§25 checklist -> README sections
 1 install core + FastAPI extra ......... Quick start / 1. Install
 2 create/configure integration ......... Quick start / 2, 4; Configuration
 3 declare a job ........................ Quick start / 2
 4 dispatch sync + async ................ Quick start / 3
 5 FastAPI dependencies in a job ........ Jobs / FastAPI dependencies in jobs
 6 run the worker ....................... Quick start / 5; The worker
 7 named queues ......................... Jobs / Named queues
 8 delays ............................... Jobs / Delays
 9 retry policy ......................... Jobs / Retry policy (+ Timeouts)
10 FIFO + fair queues ................... Jobs / FIFO queues; Fair queues
11 JobContext release/fail .............. Jobs / JobContext
12 local dev, direct SQS / LocalStack ... Local development
13 eager testing ........................ Testing your application
14 demo/conformance ..................... Demo and conformance suite
15 zero/low-config + platform status .... Platform status callout; Managed queues; Configuration
16 worker clusters, sqs/redis, AWS_* .... Running on Laravel Cloud today
17 support matrix ....................... Support matrix and public API
18 limitations + roadmap ................ Known limitations and roadmap (compression/S3 offload,
                                           native-code timeout, best-effort records, at-least-once)
19 demo/ + probe-app/ not shipped ....... Demo and conformance suite (last paragraph)
-->
