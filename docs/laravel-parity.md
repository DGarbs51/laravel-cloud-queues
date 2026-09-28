# Laravel Parity

## Introduction

Laravel Cloud Queues matches the *infrastructure* contract of Laravel's queue worker and
SQS driver, pinned to `laravel/framework` v13.33.0: the message lifecycle, attempt
counting, retries, FIFO semantics, timeouts, Laravel Cloud's queue agent protocol and its
observability events. It does not copy Laravel's PHP API, and it cannot exchange payloads
with PHP jobs.

Where the package intentionally behaves differently from Laravel, the difference is
deliberate, documented and tracked by the conformance suite. Behavior not listed here
matches Laravel.

## Notable Differences

**Configuration**

- A malformed managed configuration, an unexpected `driver`, or a missing `connection`
  or `region` is a startup error, rather than being silently skipped.
- Only the `ecs` and `instance` credential providers are accepted in managed mode. The
  AWS default credential chain is never used implicitly, to avoid Laravel Cloud's R2
  variables.

**Payloads**

- Jobs are resolved only through the registry, and argument types only through your
  annotations. Payloads never name a class.
- Oversized payloads raise `PayloadTooLargeError` before sending. Cache-backed overflow
  is not supported.

**Dispatch options**

- Delays over 900 seconds, negative delays and non-finite delays are rejected before
  sending, and fractional delays round up rather than down.
- A delay on a FIFO queue is rejected instead of silently dropped.
- FIFO options on standard queues, and fair-queue groups on FIFO queues, are rejected.

**Receiving and retries**

- A single SQS queue is long-polled for 20 seconds, with no extra sleep after an empty
  poll.
- Outside managed mode, a watchdog renews the message's visibility while a job runs, so
  long jobs are not delivered twice.
- Sub-second retry delays round up to one second, and retry delays are capped at 12
  hours.
- The timeout only covers the handler and its teardown, never outcome reporting.
- Deterministic defects, such as an unknown job or a malformed message, fail on the first
  delivery instead of being retried.
- The retry policy travels in the message, so a deploy never changes the rules for jobs
  that are already queued.

**Observability**

- Completion events are emitted immediately after the outcome is reported, so
  `duration_ms` measures the job alone.
- Oversized `failed_job` records are trimmed to fit the log collector's line limit,
  rather than dropped.

## The Full Record

The complete list of deviations, each with Laravel's behavior, this package's behavior,
the reason and the upstream source lines, lives in the repository at
[`docs/deviations.md`](https://github.com/DGarbs51/laravel-cloud-queues/blob/main/docs/deviations.md).
The conformance catalog that backs it is
[`docs/contract/catalog.json`](https://github.com/DGarbs51/laravel-cloud-queues/blob/main/docs/contract/catalog.json).
