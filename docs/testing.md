# Testing

## Introduction

When testing code that dispatches jobs, you usually want to do one of two things: run
the job right away and assert on its effects, or assert that the job *would* have been
dispatched without running it. The registry's `testing` context manager does both.

Inside a `registry.testing()` block, dispatching never touches a broker and never loads
configuration. Every dispatch is recorded, including the real encoded message and its
options.

## Running Jobs Eagerly

By default, `testing()` runs each dispatched job **immediately**, through the worker's own
execution path: the arguments are encoded, decoded, validated against the handler's
signature, FastAPI dependencies are resolved, the handler is invoked, and dependencies are
torn down:

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

With FastAPI, use the integration's registry:

```python
with queues.registry.testing() as recorder:
    client.post("/signup", params={"user_id": 42})
```

Eager jobs run as delivery attempt 1, and behave as follows:

- An exception raised by the handler is raised to your test. The retry policy is not
  applied.
- `JobContext.fail()` raises its reason (a `JobFailedError`, or the exception you passed).
- `JobContext.release()` is recorded, and the job is not run again.
- A job defect, such as an argument that does not survive encoding and decoding, raises
  the `JobDefectError`.

Eager mode works inside and outside a running event loop, without nesting loops:
`dispatch_async` awaits the job in your loop, and a sync `dispatch` of an async job inside
a running loop runs it on a helper thread.

## Faking Dispatch

To only record dispatches without running them, pass `eager=False`:

<!-- runnable -->
```python
from laravel_cloud_queues import Registry

registry = Registry()


@registry.job(name="reports.build")
def build_report(account_id: int) -> None:
    raise AssertionError("never runs")


with registry.testing(eager=False) as recorder:
    build_report.options(delay=60).dispatch(account_id=7)

[dispatch] = recorder.dispatched
assert dispatch.job_name == "reports.build"
assert dispatch.kwargs == {"account_id": 7}
assert dispatch.delay_seconds == 60
```

## Asserting Dispatches

The recorder yielded by `testing()` is a `DispatchRecorder`:

- `recorder.dispatched` lists every `RecordedDispatch`, in order.
- `recorder.for_job(name)` lists the dispatches of one job.

Each `RecordedDispatch` has these attributes:

| Attribute | Description |
|---|---|
| `job_name` | The job's wire name |
| `queue` | The resolved queue |
| `body` | The exact encoded message |
| `args` / `kwargs` | The encoded arguments, as JSON values |
| `delay_seconds` | The delay, in whole seconds |
| `fifo_group` | The FIFO message group, if any |
| `deduplication_id` | The FIFO deduplication ID, if any |
| `message_group` | The fair-queue message group, if any |

Arguments are recorded in their encoded form, so tagged types appear as JSON. A `UUID`,
for example, is recorded as `{"$type": "uuid", "value": "..."}`.

## Options and Validation in Tests

Dispatch options are validated in tests just as they are in production. Unless you pass a
configuration to the registry, the test double follows SQS rules: FIFO and fair-queue
options are allowed, and the 1 MiB payload limit applies. The default queue is `default`,
or the configured default queue when the registry has a configuration.

## Overriding Dependencies

Since eager mode resolves FastAPI dependencies, `app.dependency_overrides` works as it does
for routes:

```python
app.dependency_overrides[get_mailer] = lambda: FakeMailer()

with queues.registry.testing():
    send_email.dispatch(user_id=1, template="welcome")
```

## What Eager Mode Does Not Test

Eager mode proves that your arguments bind, encode and decode, that your dependencies
resolve, and that your handler works. It does **not** exercise retries and backoff,
timeouts, visibility, FIFO ordering, queue existence, payload limits against a real
broker, or anything about the worker process. For those, run a worker against a local
broker; see [Local Development](local-development.md).
