# The Job Context

## Introduction

Inside a running job, `current_job()` returns the `JobContext` of the current delivery.
It tells you which attempt this is, and it lets the job release itself back onto the
queue or fail right away:

<!-- runnable -->
```python
from laravel_cloud_queues import JobContext, Registry, current_job

registry = Registry()


@registry.job(name="payments.capture", tries=3, backoff=[5, 30])
def capture_payment(payment_id: str) -> None:
    job: JobContext = current_job()
    status = lookup_status(payment_id)

    if status == "pending":
        # Try again in 60 seconds. This consumes an attempt like any delivery.
        job.release(delay=60)

    if status == "cancelled":
        # Fail now, regardless of the remaining attempts.
        job.fail(f"payment {payment_id} was cancelled")

    print(f"captured {payment_id} on attempt {job.attempt} of {job.max_tries}")


def lookup_status(payment_id: str) -> str:
    return "captured"


with registry.testing():
    capture_payment.dispatch(payment_id="pay_1")
```

Calling `current_job()` outside a running job raises a `RuntimeError`.

## Available Properties

| Property | Description |
|---|---|
| `job_name` | The job's wire name |
| `uuid` | The unique identifier of the dispatch, stable across retries |
| `message_id` | The broker's identifier for the message |
| `queue` | The logical queue the message was received from |
| `attempt` | The current delivery attempt, starting at 1 |
| `max_tries` | The job's effective `tries` (`0` means unlimited) |

The `uuid` is a good idempotency key for side effects, since it stays the same when a
message is retried.

## Releasing a Job

`release` puts **this** message back onto the queue for another delivery after `delay`
seconds. The delay may be an `int`, a `float` or a `timedelta`; it is rounded up to whole
seconds and capped at 12 hours:

```python
current_job().release(delay=timedelta(minutes=2))
```

A released job always goes back onto the queue, as in Laravel, and the release consumes an
attempt. If the job has no attempts left, its next delivery fails the pre-run check with
`MaxAttemptsExceededError`.

## Failing a Job

`fail` fails the job terminally, right away, no matter how many attempts remain:

```python
current_job().fail("The account was closed.")
```

The `reason` may be a string or an exception. It becomes the exception in the
[failure record](failed-jobs.md), and defaults to a `JobFailedError`.

## How Release and Fail Work

Both methods raise a control-flow exception to leave your handler immediately, so code
after them does not run. They perform no I/O inside the handler: the worker reports the
outcome once, after your dependencies have been torn down.

The first call wins. If your code catches the control-flow exception, for example in a
broad `except Exception:` block, the outcome it recorded still takes precedence over a
successful return.

## Injecting the Context

With FastAPI, declare the context as a dependency:

```python
@queues.job(name="emails.send", tries=3)
async def send_email(
    user_id: int,
    job: Annotated[JobContext, Depends(current_job)],
) -> None:
    print(f"attempt {job.attempt}")
```

Any job may also declare a parameter annotated `JobContext`, and the worker injects it:

```python
@registry.job(name="emails.send")
def send_email(user_id: int, job: JobContext) -> None: ...
```

:::{note}
A parameter annotated `JobContext` still appears in the static type of `dispatch`, so a
strict type checker would ask you to pass it. Calling `current_job()` inside the handler,
or `Depends(current_job)` with FastAPI, avoids that and is the preferred pattern.
:::

The `JobContext` is a runtime object. It is never serialized, and it can never be
supplied from message data.
