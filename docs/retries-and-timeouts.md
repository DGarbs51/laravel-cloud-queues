# Retries and Timeouts

## Introduction

Sometimes a job fails because of a temporary problem, like a flaky network connection or
a rate-limited API. Laravel Cloud Queues lets you declare how many times a job may be
attempted, how long to wait between attempts, and how long each attempt may run:

```python
@queues.job(name="emails.send", tries=5, backoff=[1, 5, 30, 120], timeout=60)
async def send_email(user_id: int) -> None: ...
```

## The Retry Policy

| Field | Default | Description |
|---|---|---|
| `tries` | `1` | The total number of deliveries allowed. `0` means unlimited |
| `backoff` | `0` | The seconds to wait before a retry: a number, or a list indexed by attempt |
| `timeout` | `60` | The seconds a single delivery may run. `0` disables the timeout. The maximum is 604,800 (7 days) |
| `fail_on_timeout` | `False` | Fail the job on its first timeout, instead of letting it be retried |

As in Laravel, jobs are attempted **once** by default. Declare `tries` to retry them.

An invalid value, such as a negative `tries` or a `timeout` over seven days, raises a
`ConfigurationError` when the job is declared.

### Backoff

A single number applies to every retry. A list is indexed by attempt: after attempt `n`
fails, the job waits `backoff[n - 1]` seconds, and the last value repeats once the list
runs out. So `backoff=[1, 5, 30]` waits 1 second after the first attempt, 5 seconds after
the second, and 30 seconds after every attempt from then on.

Positive fractional delays round up to whole seconds, and retry delays are capped at
43,200 seconds (12 hours), the maximum SQS visibility timeout.

### Reusable Policies

If several jobs share the same rules, declare a `RetryPolicy` once and pass it with
`policy`:

<!-- runnable -->
```python
from laravel_cloud_queues import Registry, RetryPolicy

registry = Registry()
external_api = RetryPolicy(tries=5, backoff=[10, 60, 300], timeout=120)


@registry.job(name="crm.sync", policy=external_api)
def sync_contact(contact_id: int) -> None: ...


@registry.job(name="crm.import", policy=external_api, tries=10)
def import_contacts(batch_id: int) -> None: ...


assert import_contacts.policy.tries == 10
assert import_contacts.policy.timeout == 120
```

Options passed directly to the `job` decorator override the policy's fields.

### The Policy Travels in the Message

When you dispatch a job, its effective `tries`, `backoff`, `timeout` and `fail_on_timeout`
are written into the message, and the worker follows the message. Worker defaults only
apply to fields the job did not declare.

This means a deploy never changes the rules for jobs that are already queued, and a retry
from the Laravel Cloud dashboard keeps the job's original rules.

## How Attempts Are Counted

Attempt counting follows Laravel's worker exactly. The attempt number comes from the
broker, never from the message body: SQS's `ApproximateReceiveCount`, or the Redis
reservation counter.

- **Before running.** If `tries > 0` and the attempt is greater than `tries`, the job fails
  without running, with a `MaxAttemptsExceededError`. This catches deliveries that ran
  over budget through crashes or timeouts.
- **After an exception.** If `tries > 0` and the attempt is greater than or equal to
  `tries`, the failure is terminal. Otherwise, the **same message** is released for retry
  after the configured backoff, by changing its SQS visibility or moving it to the Redis
  delayed set. No duplicate message is ever created.

A job can also decide for itself to be retried later or to fail right away. See
[The Job Context](job-context.md).

## Timeouts

Timeouts are **process-level**, like Laravel's worker, rather than asyncio cancellation.
Before each job, the worker arms a timer with the job's timeout: the value from the
message, or else the worker's `--timeout` option, which defaults to 60 seconds.

When the timer fires, the worker:

1. Decides whether the timeout is terminal: the job was on its last attempt, or it
   declared `fail_on_timeout=True`.
2. Writes the failure record or lifecycle event.
3. Exits the process immediately with **exit code 124**.

A timeout that is not terminal does not release the message and applies no backoff. The
message comes back when its SQS visibility or Redis reservation expires, with an
incremented attempt count, and Laravel Cloud restarts the worker.

The timer only covers the handler and its dependency teardown. It is armed after the
message has been decoded and disarmed before the outcome is reported, so a timeout never
interrupts an acknowledgement.

:::{warning}
Python runs signal handlers between bytecode instructions. A job blocked inside native
code, like a long C extension call or a huge `sum(range(n))`, overruns its timeout until
control returns to the interpreter. Pure-Python loops, `await asyncio.sleep`,
`time.sleep`, `re.match` and `hashlib` calls were measured as interruptible within about
10 milliseconds. Laravel's worker shares this limitation.
:::

Keep your timeouts inside Laravel Cloud's limits: 90 seconds on Flex workers, with no
fixed limit on Pro workers. See [Runtime and Shutdown Limits](deployment.md#runtime-and-shutdown-limits).

## Designing for Retries

Because a job may run more than once, write your handlers to be **idempotent**. Key side
effects on the job's `uuid` or on a business identifier, so that a second delivery of the
same message does no harm. See [Delivery Guarantees](failed-jobs.md#delivery-guarantees).
