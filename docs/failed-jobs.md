# Failed Jobs and Delivery

## Introduction

Sometimes your queued jobs will fail. Don't worry, things don't always go as planned!
This page explains when a job fails for good, where the record of that failure goes, and
what delivery guarantees you can rely on when designing your handlers.

## Delivery Guarantees

Delivery is **at least once**. SQS, Redis reservations and the Laravel Cloud agent all
redeliver a message whose worker crashed, timed out or lost its acknowledgement. When an
acknowledgement is ambiguous, such as a lost response from the agent, the worker stops
rather than guessing.

Design your handlers to be **idempotent**:

- Key side effects on the job's `uuid` (see [The Job Context](job-context.md)) or on a
  business identifier.
- Tolerate a second delivery of the same message.

## When Jobs Fail

A job fails *terminally* when:

- its last attempt raises an exception,
- it calls `JobContext.fail()`,
- a delivery arrives after `tries` is already exhausted (`MaxAttemptsExceededError`),
- it times out on its last attempt, or with `fail_on_timeout=True` (`JobTimeoutError`), or
- the message has a deterministic defect.

### Job Defects

Some problems can never succeed on a retry, so they fail on the **first** delivery and
are never retried:

- a malformed message, or an unsupported message version,
- a job name that is not registered,
- arguments that cannot be decoded, or that do not match the handler's signature,
- a Laravel overflow (`@pointer`) body, which is not supported.

These all raise a `JobDefectError` subclass.

## Failure Records

In every mode, the worker first logs the failure record as a log line; see
[The Failure Log Line](#the-failure-log-line).

### Managed Mode

In managed mode, the worker then completes the message, then sends a `failed_job` event and a
`failed` lifecycle event to Laravel Cloud, which owns failed-job inspection and retry. You
may inspect and retry failed jobs from the Laravel Cloud **Queues** dashboard.

A dashboard retry re-queues the message verbatim. The worker runs it as a fresh first
attempt, with its original retry policy.

### SQS and Redis Modes

In `sqs` and `redis` mode, the worker then deletes the message. There is no failed-job
store, dead-letter queue or retry command in these modes yet. To re-run a failed job,
dispatch it again.

### The Failure Log Line

In every mode, the worker logs the full failure record as **one error-level log line**,
which appears in Laravel Cloud's **Logs** tab, before it completes the message. The line is written by [`laravel-cloud-logging`](https://pypi.org/project/laravel-cloud-logging/)
in Laravel's log format. The exception, with its trace and chain, is in `context.exception`.
The record's fields are in `context`:

| Field | Description |
|---|---|
| `laravel_cloud_queues` | Always `"failed_job"`, so you can find these lines in your logs |
| `queue` | The queue the job was received from |
| `message_id` | The broker's message identifier |
| `attempts` | The delivery attempt that failed |
| `job_name` | The job's wire name |
| `started_at` / `failed_at` | When the attempt started and failed |
| `exception_preview` | A short summary of the exception |
| `payload` | The original message body |

Log lines are capped at 256 KiB. A longer record has each top-level field cut to 16 KiB.

:::{warning}
**Never put secrets in job arguments.** Failure records carry the full payload, so that a
job can be retried from the dashboard, into your logs and the dashboard. Pass identifiers
instead, and look secrets up inside the handler.
:::

### Records Are Best-Effort

The failure log line is written before the message is completed, but nothing stores it
beyond your logs. In managed mode the `failed_job` event is sent after the message is
completed, so an outage of the log socket at that moment loses the event. Laravel makes the
same trade-off.

## Error Classes

Every exception the package raises has exactly one classification, which decides how it
is handled:

| Class | Examples | Behavior |
|---|---|---|
| Dispatch error (`DispatchError`) | `ConfigurationError`, `ManagedQueueNotFoundError`, `PayloadTooLargeError`, `InvalidQueueOptionError`, `SerializationError`, `ArgumentError` | Raised to the caller. Nothing is sent |
| Job defect (`JobDefectError`) | Malformed message, unknown job, codec or argument mismatch, `@pointer` body | Terminal on the first delivery |
| Handler failure | Any exception from the handler or its dependency teardown | The retry policy applies |
| Transport error (`TransportError`) | A transient receive error | Logged. The worker retries after one second |
| Fatal worker error (`FatalWorkerError`) | Agent unavailable, lost lease, ambiguous acknowledgement, broker connection lost | The worker stops |

Every class derives from `LaravelCloudQueuesError`. The full hierarchy lives in
`laravel_cloud_queues.errors`; see the [API reference](api/errors.rst).
