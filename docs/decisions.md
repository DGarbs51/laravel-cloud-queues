# Project decisions

Decisions that resolve open questions from `docs/audits/2026-09-27/README.md`. They take precedence over conflicting wording in `PROJECT_SCOPE.md` and `AGENT_BUILD_PROMPT.md` until those documents are updated. See `docs/references.md` for where to verify each one.

## D1 — Failed-job events: hybrid size policy (2026-09-27)

- Emit Laravel's `failed_job` field set: `_cloud_event`, `id` (UUIDv7), `queue`, `started_at`, `attempts`, `payload`, `exception_preview`, `job_name`, `exception`.
- Send the full payload when the whole NDJSON line fits the collector limit.
- If it does not fit, trim `exception` first.
- If it still does not fit, send a trimmed record marked as not replayable.
- Never apply Symfony's payload projection to the Python envelope: it discards arguments and context even for small messages.
- Document that failure records are best-effort. Laravel deletes the message before the record is written, so a log-socket outage at that moment loses the record.

## D2 — Timeouts: Laravel parity (2026-09-27)

- One worker process. Arm `signal.setitimer` for each job, using the job timeout, else the worker default (60 s).
- On timeout:
  1. Apply the failure checks (attempts exhausted or `fail_on_timeout`).
  2. Emit the lifecycle event: `failed` if failed, else `released`.
  3. Exit immediately with `os._exit(124)`.
- Do not release the message or apply backoff; the message returns through agent/SQS visibility.
- The FastAPI lifespan runs once per worker process. After a timeout, the platform starts a fresh worker.
- Known limitation, shared with Laravel: code blocked in native extensions or blocking calls delays the signal handler, so such a job can overrun its timeout.
- No supervisor process in v1.

## D3 — Dashboard retry is supported (2026-09-27)

- The envelope must survive being re-queued verbatim by Laravel Cloud.
- The failed-job record carries the full payload whenever D1 allows it.
- Conformance: re-send a captured failed payload and verify the worker runs it as a fresh first attempt.
- The live dashboard check stays `skipped` until Laravel Cloud supports managed queues for Python.
- Do not port Laravel's `FailedJobProvider` fetch path (downloading and decrypting failed payloads from URLs). v1 only emits `failed_job` events.

## D4 — Retry policy travels in the message (2026-09-27)

- At dispatch, store `tries`, `backoff`, `timeout` and `fail_on_timeout` in the envelope. The worker follows the message.
- Worker defaults (`tries` 1, `backoff` 0, `timeout` 60) apply only to fields the message omits.
- A deploy never changes the rules for jobs already queued, and dashboard retries keep their original rules.
- `retry_until` and `max_exceptions` are deferred from v1. The envelope reserves room for them so they can be added without a version break.

## D5 — Dispatch options use a builder (2026-09-27)

```python
send_email.dispatch(user_id=1)
await send_email.options(queue="priority", delay=30).dispatch_async(user_id=1)
```

- `.options(...)` returns a typed copy of the job carrying dispatch options: queue, delay, FIFO group and dedup ID, fair-queue group.
- `dispatch` and `dispatch_async` keep exactly the job's own parameter signature, so options never collide with job parameters.
- `mypy --strict` must check both the option types and the job's argument types, with downstream typing samples in the test suite.
