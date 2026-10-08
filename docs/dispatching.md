# Dispatching Jobs

## Introduction

Once you have written a job, you dispatch it to a queue with `dispatch_async` or
`dispatch`. Both take **exactly the handler's parameters**, so your type checker checks
the arguments you pass:

```python
@app.post("/signup")
async def signup(user_id: int) -> dict[str, str]:
    receipt = await send_email.dispatch_async(user_id, template="welcome")

    return {"message_id": receipt.message_id, "queue": receipt.queue}
```

Use `await job.dispatch_async(...)` in async code. It never blocks the event loop. On the
`redis` backend it sends with a native asyncio client and never leaves the loop. SQS has no
asyncio client, so `sqs` and `managed` sends run in a worker thread. See
[Native Async Paths](#native-async-paths).

Use `job.dispatch(...)` in scripts, sync code and tests. Inside a running event loop it
still works, and it never starts a nested loop, but it blocks that loop while it talks to
the broker.

:::{tip}
Prefer keyword arguments when dispatching. Keyword arguments survive changes to the
handler's signature better than positional ones.
:::

## Native Async Paths

`dispatch_async` and the worker always await one interface. A backend with a native
asyncio client uses it; otherwise its sync transport runs in an AnyIO worker thread:

| Backend | `dispatch_async` | Worker receive and acknowledgement |
|---|---|---|
| `redis` | native (`redis.asyncio`) | native (`redis.asyncio`) |
| `managed`, agent enabled | worker thread (SQS) | native (`httpx` over the agent socket) |
| `managed`, agent disabled, or `sqs` | worker thread (SQS) | worker thread (SQS) |
| A custom backend without async factories | worker thread | worker thread |

The first `dispatch_async` in a process builds the configuration and backend in a worker
thread; after that, a native path never leaves the event loop. Without a running asyncio
loop (for example under trio), the whole dispatch runs in a worker thread.

## Closing the Async Producer

`dispatch_async` keeps one async producer (a Redis connection pool, for example) per
running event loop, because an asyncio client must never be shared between loops. The
worker closes its producer for you when it stops. In a FastAPI web app, close it from your
lifespan's shutdown, on the same loop (`queues` is your `LaravelCloudQueues` binding):

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    await queues.registry.aclose_producer()
```

Without FastAPI, call `await registry.aclose_producer()`, or wrap your async entry point
in `async with registry.lifespan():`, which closes it on exit.

If your code starts a new event loop for each call, for example `asyncio.run()` per
dispatch, close the producer before that loop ends. A producer left open is only dropped
once its loop has closed, and its connections are then freed by garbage collection.

## Dispatch Receipts

Both methods return a `DispatchReceipt` with three attributes:

| Attribute | Description |
|---|---|
| `message_id` | The identifier assigned to the message by the broker |
| `queue` | The logical name of the queue the message was sent to |
| `uuid` | The unique identifier of this dispatch, stored in the message |

Jobs are fire-and-forget. There is no result handle, and the return value of a handler
is ignored.

## Dispatch Options

To customize a single dispatch, call `options` on the job first. It returns a typed copy
of the job that carries the options:

```python
await send_email.options(queue="priority", delay=30).dispatch_async(user_id=1)
```

Because options are passed to `options` rather than to `dispatch`, they never collide with
your handler's own parameters, even parameters named `queue`, `delay` or `timeout`. Calls
may be chained, and later calls override earlier ones:

```python
urgent = send_email.options(queue="priority")

await urgent.options(delay=10).dispatch_async(user_id=1)
```

| Option | Description |
|---|---|
| `queue` | The queue to dispatch to |
| `delay` | How long to wait before the job becomes available |
| `group` | The FIFO message group (`.fifo` queues only) |
| `deduplication_id` | The FIFO deduplication ID (`.fifo` queues only) |
| `message_group` | The fair-queue message group (standard SQS queues only) |

## Customizing the Queue

By pushing jobs to different queues, you may categorize your queued jobs and even
prioritize how many workers you assign to them. A job may declare its default queue, and
any dispatch may override it:

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

The queue is chosen in this order:

1. The `queue` passed to `options`.
2. The `queue` declared on the job.
3. The backend's default queue: `LARAVEL_CLOUD_QUEUES_SQS_QUEUE`,
   `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE` or the managed configuration's `connection.queue`.
   Each defaults to `default`.

Remember to [tell the worker](workers.md#queue-priorities) which queues to process.

## Delayed Dispatching

If you would like to specify that a job should not be available for processing
immediately, pass a `delay`. It accepts seconds, as an `int` or a `float`, or a
`timedelta`:

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

- Positive fractional delays round **up** to the next whole second, so a short delay
  never becomes immediate.
- The maximum delay is **900 seconds** (15 minutes). This is SQS's per-message limit, and
  it applies to Redis too so that your application behaves the same on every backend.
- Negative, non-finite or longer delays raise `InvalidQueueOptionError` before anything is
  sent.
- FIFO queues do not support per-message delays. A positive delay on a `.fifo` queue is
  rejected.

Retries are different from fresh delays. Retry backoff may extend up to 12 hours; see
[Retries and Timeouts](retries-and-timeouts.md).

## FIFO Queues

Queues whose name ends in `.fifo` use SQS FIFO semantics, available in `sqs` and
`managed` mode. Messages in the same *group* are delivered in order, one at a time:

```python
@queues.job(name="ledger.post", queue="ledger.fifo")
async def post_entry(account_id: int, amount: int) -> None: ...


# The default group is the queue name, so the whole queue is serialized.
await post_entry.dispatch_async(account_id=1, amount=500)

# Per-account ordering, with a business deduplication ID.
await post_entry.options(
    group="account-1", deduplication_id="txn-8f1c"
).dispatch_async(account_id=1, amount=500)

# An empty deduplication ID relies on the queue's content-based deduplication.
await post_entry.options(deduplication_id="").dispatch_async(account_id=1, amount=500)
```

- **Group.** The default `group` is the queue name *including* `.fifo`, following
  Laravel's convention. That serializes the entire queue. Pass `group` for per-tenant or
  per-entity ordering.
- **Deduplication ID.** By default, each dispatch gets a fresh unique ID, chosen once and
  reused across internal network retries.
- **Content-based deduplication.** An empty `deduplication_id` omits the attribute, so the
  queue's content-based deduplication applies. Every message contains a fresh `uuid` and
  trace data, so two identical calls still differ. Pass an explicit business ID when you
  want duplicates collapsed.
- Group and deduplication IDs must be 1 to 128 printable ASCII characters without spaces,
  and they are validated before sending.

## Fair Queues

On **standard** SQS queues, the `message_group` option sets the message group used by SQS
fair queues, so one noisy tenant cannot starve the others:

```python
await send_email.options(message_group=f"tenant-{tenant_id}").dispatch_async(user_id=1)
```

Fair queues and FIFO queues are different models. Passing FIFO options (`group`,
`deduplication_id`) to a standard queue, or `message_group` to a `.fifo` queue, raises
`InvalidQueueOptionError` instead of silently dropping the attribute. The `redis` backend
supports neither model, and rejects all of these options.

## Dispatch Errors

Dispatch validates everything it can before any network call. When something is wrong,
it raises a `DispatchError` subclass and **nothing is sent**:

| Exception | Raised when |
|---|---|
| `ArgumentError` | The arguments do not match the handler's signature, or supply an injected parameter |
| `SerializationError` | An argument cannot be encoded |
| `InvalidQueueOptionError` | An option, or combination of options, is invalid |
| `PayloadTooLargeError` | The encoded message is larger than the backend allows |
| `ManagedQueueNotFoundError` | The target SQS queue does not exist |
| `ConfigurationError` | The configuration is missing or invalid |

`ArgumentError` is also a `TypeError`, and `InvalidQueueOptionError` is also a
`ValueError`, so existing `except` clauses keep working.
