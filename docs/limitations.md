# Limitations and Roadmap

## Known Limitations

Laravel Cloud Queues is a young package. Before you adopt it, make sure these
limitations are acceptable for your application:

- **Managed queues are not yet verified live.** Laravel Cloud has not yet enabled managed
  queues for Python. Managed mode is tested against the pinned Laravel contract and
  local emulators.
- **No compression or large-payload offload.** Payloads over 1 MiB on SQS and managed
  queues are rejected with `PayloadTooLargeError`. Laravel's cache-backed overflow is not
  implemented.
- **Timeouts cannot interrupt native code.** A handler blocked in a C extension or a long
  non-Python call overruns its timeout until control returns to the interpreter. See
  [Timeouts](retries-and-timeouts.md#timeouts).
- **Failure records are best-effort**, and outside Laravel Cloud's dashboard (managed
  mode) there is no failed-job store, dead-letter queue or retry command.
- **Delivery is at least once.** Handlers must be idempotent.
- **No lifecycle events in `sqs` and `redis` modes.** Laravel Cloud ingests them for
  managed queues only, so workers log structured lines instead.
- **One job in flight per worker process**, and no result backend.
- **No Laravel Cloud exception reporting yet.** Laravel 13.34+ can send each job
  exception to Cloud's exceptions view (`LARAVEL_CLOUD_EXCEPTIONS`, an
  `{"_cloud_event": "exception"}` log line, and an `exception_id` on `failed_job`).
  That Cloud feature is in early access and only Laravel environments can enable it
  today, so this package does not emit those events. Support will be added once the
  feature is generally available. Until then, job exceptions appear in the Logs tab and,
  for terminal failures, in the Queues dashboard's failed jobs.
- **Not yet supported:** job chains, batches, unique jobs, job middleware,
  `retry_until`, `max_exceptions`, memory-limit recycling, `after_commit`, credential
  caching, and payload interoperability with PHP jobs.

## Roadmap

In rough priority order:

1. Transparent payload compression.
2. Transparent S3 or object-storage offload for large payloads, with configurable
   thresholds and worker-side hydration and cleanup.
3. A Django adapter (`[django]`), then a Flask adapter (`[flask]`), on the same core and
   wire format.
4. Live verification of Laravel Cloud managed queues, once the platform enables them for
   Python.
5. A manual live smoke test on Laravel Cloud worker clusters.
6. `retry_until` and `max_exceptions`. The message format already reserves room for
   them.
7. Dead-letter handling and failed-job tooling outside managed mode.
8. Laravel Cloud exception reporting, once the Cloud feature is generally available:
   one `exception` event per job exception, with Python stack frames, app-relative
   paths, `execution_type: "job"` and the job's execution context. Each line stays under
   1 MB, and its id is reused as `failed_job.exception_id`.
9. Advanced queue workflow features, once core compatibility is proven live.
10. Following upstream changes to Laravel's managed queues, detected by a weekly drift
    check.

Have an idea, or found a bug? Open an issue on
[GitHub](https://github.com/DGarbs51/laravel-cloud-queues/issues).
