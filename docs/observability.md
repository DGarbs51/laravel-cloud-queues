# Observability

## Introduction

Laravel Cloud Queues reports what your jobs are doing in a way that fits each backend.
In managed mode, your Python jobs appear in the Laravel Cloud **Queues** dashboard, just
like PHP jobs. In `sqs` and `redis` modes, the worker writes structured log lines you can
search in the **Logs** tab. OpenTelemetry trace context can follow each job from the code
that dispatched it into the worker.

Observability is always best-effort: an outage of the logging socket never turns a
successful job into a failed one.

## Laravel Cloud Events

In managed mode, the package emits Laravel Cloud's queue lifecycle events as
newline-delimited JSON over the observability Unix socket (`LARAVEL_CLOUD_LOG_SOCKET`,
which defaults to `unix:///tmp/cloud-init.sock`):

| Event | Emitted when |
|---|---|
| `queued` | A job is dispatched |
| `started` | The worker starts a delivery |
| `processed` | A job completes successfully |
| `released` | A job is released for another attempt |
| `failed` | A job fails terminally |

Events carry normalized queue names and, for completed deliveries, a `duration_ms`.

Terminal failures are also reported with Laravel's `failed_job` event, which carries the
full payload so the job can be retried from the dashboard. The log collector limits line
size, so a `failed_job` record is sent whole when it fits. Otherwise, the exception is
trimmed first, then the payload, and a record with a trimmed payload is marked
`"replayable": false`.

## Worker Logs

Laravel Cloud currently ingests queue lifecycle events for managed queues only, so in
`sqs` and `redis` modes the package sends **no** events to the socket. Instead, the
worker logs:

- one info line to stderr for each completed or released delivery, and
- one JSON [failure record](failed-jobs.md#sqs-and-redis-modes) on stdout for each
  terminal failure.

Credentials, receipt handles and payloads are never logged by default. Payloads only
appear inside failure records.

## Tracing

With the `otel` extra installed, dispatching a job injects the current W3C trace context
(`traceparent` and `tracestate`) into the message. The worker extracts it and activates
it around the job, then resets it afterwards so context never leaks from one job into the
next:

```shell
pip install "laravel-cloud-queues[otel]"
```

That means spans you create inside a handler join the trace of the request that
dispatched the job. Configure the OpenTelemetry SDK and exporter in your application as
usual; the package only depends on `opentelemetry-api`.

Without the extra, dispatch sends an empty trace context and nothing else changes.
