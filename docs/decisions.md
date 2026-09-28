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

## D6 — Worker-cluster backends: SQS and Redis/Valkey in v1 (2026-09-27)

Laravel Cloud worker clusters run Python today but get no managed queue (see `docs/audits/2026-09-27/platform-findings.md`). v1 supports three modes:

| Mode | Selected when | Broker | Receive |
|---|---|---|---|
| Managed | `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present | Laravel Cloud SQS | Agent when `agent.enabled`, else direct SQS |
| Self-managed SQS | `LARAVEL_CLOUD_QUEUES_BACKEND=sqs` | Customer's own SQS | Direct SQS |
| Redis/Valkey | `LARAVEL_CLOUD_QUEUES_BACKEND=redis` | Laravel Valkey or any Redis | Redis transport |

- **Backend selection (D6a):** `LARAVEL_CLOUD_QUEUES_BACKEND=managed|sqs|redis`.
  - When unset: managed if `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present, otherwise a configuration error.
  - The presence of `REDIS_URL` or `AWS_*` variables never selects a backend: apps commonly attach Valkey for caching and object storage for files.
  - The same settings may be passed in code; code wins over the environment.
- **Settings (D6a):**
  - SQS: `LARAVEL_CLOUD_QUEUES_SQS_PREFIX`, `_SQS_SUFFIX`, `_SQS_QUEUE` (default queue), `_SQS_REGION`, `_SQS_KEY`, `_SQS_SECRET`, `_SQS_ENDPOINT` (LocalStack).
  - Redis: `LARAVEL_CLOUD_QUEUES_REDIS_URL`, falling back to `REDIS_URL` only when the backend is `redis`.
- **Self-managed SQS** uses package-specific settings passed explicitly to the boto3 client. It must not read the standard `AWS_*` variables or boto3's default endpoint settings, because Cloud object storage occupies `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` and `AWS_REGION`.
- **The worker must extend visibility itself** on self-managed SQS, or require a queue visibility timeout longer than the maximum job timeout. No agent heartbeat exists outside managed mode.
- **Redis/Valkey** follows Laravel's `RedisQueue` semantics:
  - pending list, delayed and reserved sorted sets, and atomic Lua scripts;
  - attempts counted per reservation;
  - retry as a delayed re-release of the same job ID, never a duplicate;
  - TLS via `rediss://`.
- D1–D5 apply to every mode. Retry policy stays in the message, and timeouts exit 124. Custom worker processes are restarted by Cloud when they exit.
- Worker clusters scale on CPU, memory or a fixed count, not on queue depth. Document this.
- Laravel Cloud's Queues dashboard covers managed queues only. Whether lifecycle events from worker clusters appear anywhere is unverified.

- **Terminal failures outside managed mode (D6b):** log only.
  1. Write the full failure record as one structured JSON line to the worker's log output (visible in Cloud's Logs tab).
  2. Also emit the D1 `failed_job` event to the log socket when it exists; best-effort, and unverified whether it surfaces anywhere.
  3. Delete the message.
  - No failed-job store, dead-letter queue or retry command in v1. Re-running a failed job means dispatching it again.

- **Testing (D6c):**
  - Redis conformance suite against local Valkey and Redis containers in CI.
  - Self-managed SQS against LocalStack.
  - Once the package can dispatch and consume, a manual, optional live smoke test on Laravel Cloud: web dispatch through a probe route, a worker cluster running `laravel-cloud-queues work`, result verified through logs or a probe route.
  - The live test is not a release gate, matching the scope's treatment of live Cloud verification.

## D7 — Choices made while locking in the spec (2026-09-27)

Smaller choices made when applying D1–D6 and the audit fixes to `PROJECT_SCOPE.md`. Review and override as needed.

- `/result` 4xx is fatal and raises `AgentProtocolError`; both upstreams treat it as non-fatal (§11).
- Worker exit codes: 0 clean stop, 1 agent unhealthy or other fatal transport error, 2 configuration error, 124 timeout. Laravel exits 0 on agent loss (§11, §23).
- Fresh delays accept `int` or `timedelta`; positive fractions round up; >900 s, negative and non-finite values are rejected (§10).
- The Redis backend applies the same 1 MiB payload limit and 900-second delay cap as SQS, so payloads and behavior stay portable; FIFO and fair-queue options are rejected in `redis` mode (§8, §10).
- Self-managed SQS requires explicit credentials unless `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default` opts into boto3's default chain (§6).
- Direct SQS and Redis run a watchdog thread that renews visibility or reservations during a job (§11).
- AnyIO on the asyncio backend only (§5).
- Packaging: `hatchling`; extras `fastapi`, `redis`, `otel`; Pydantic support activates when installed (§4).
- CI: CPython 3.10–3.14 on Linux; macOS not required (§5).
- Worker targets may be a FastAPI app or a core registry object (§23).

## D8 — Worker lifecycle on worker clusters (2026-09-28)

Per the Laravel Cloud team: worker clusters and App-cluster background processes run the worker as a long-lived, supervised service that is restarted on exit. It is not a run-once process. Stop-when-empty flags default to off and are discouraged there (they would restart-loop). Exit codes are diagnostic. Managed queues keep a platform-controlled lifecycle. See `PROJECT_SCOPE.md` §13.
