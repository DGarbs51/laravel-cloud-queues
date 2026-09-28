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
- `ty` must check both the option types and the job's argument types, with downstream typing samples in the test suite.

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
- Laravel Cloud's Queues dashboard and lifecycle events cover managed queues only (confirmed; see D12).

- **Terminal failures outside managed mode (D6b):** log only.
  1. Write the full failure record as one structured JSON line to the worker's log output (visible in Cloud's Logs tab).
  2. Delete the message.
  - (Revised by D12: no `failed_job` event is sent in these modes.)
  - No failed-job store, dead-letter queue or retry command in v1. Re-running a failed job means dispatching it again.

- **Testing (D6c):**
  - Redis conformance suite against local Valkey and Redis containers in CI.
  - Self-managed SQS against LocalStack.
  - Once the package can dispatch and consume, a manual, optional live smoke test on Laravel Cloud: web dispatch through a probe route, a worker cluster running `laravel-cloud-queues work`, result verified through logs or a probe route.
  - The live test is not a release gate, matching the scope's treatment of live Cloud verification.

## D7 — Choices made while locking in the spec (2026-09-27)

Smaller choices made when applying D1–D6 and the audit fixes to `PROJECT_SCOPE.md`. Review and override as needed.

- ~~`/result` 4xx is fatal~~ Revised 2026-09-28: 4xx is logged as `AgentProtocolError` and the worker continues, matching Laravel and Symfony (§11).
- Worker exit codes: 0 clean stop or agent unhealthy (revised 2026-09-28 to match Laravel), 1 other fatal transport error, 2 configuration error, 124 timeout (§11, §23).
- Fresh delays accept `int` or `timedelta`; positive fractions round up; >900 s, negative and non-finite values are rejected (§10).
- The Redis backend has no transport payload limit (revised 2026-09-28; bounded by the 16 MiB envelope decode ceiling since D14.2); it keeps the 900-second delay cap, and FIFO and fair-queue options are rejected in `redis` mode (§8, §10).
- Self-managed SQS requires explicit credentials unless `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default` opts into boto3's default chain (§6).
- Direct SQS and Redis run a watchdog thread that renews visibility or reservations during a job (§11). Confirmed 2026-09-28:
  - lease window 60 s by default;
  - renewal every third of the window;
  - the thread starts with the job and stops before the outcome is reported;
  - a failed renewal (message deleted or reassigned) is a lost lease: never report success for a job the worker no longer owns;
  - `timeout=0` (unlimited) is allowed because the lease keeps renewing.
- AnyIO on the asyncio backend only (§5).
- Packaging: `hatchling`; extras `fastapi`, `redis`, `otel`; Pydantic support activates when installed (§4).
- CI: CPython 3.10–3.14 on Linux; macOS not required (§5).
- Worker targets may be a FastAPI app or a core registry object (§23).

## D8 — Worker lifecycle on worker clusters (2026-09-28)

Per the Laravel Cloud team: worker clusters and App-cluster background processes run the worker as a long-lived, supervised service that is restarted on exit. It is not a run-once process. Stop-when-empty flags default to off and are discouraged there (they would restart-loop). Exit codes are diagnostic. Managed queues keep a platform-controlled lifecycle. See `PROJECT_SCOPE.md` §13.

## D9 — Local SQS emulator (2026-09-28)

Local development and agents use `moto` for SQS tests (no Docker required). CI uses LocalStack and is the authoritative gate. One test suite, backend selected by `LARAVEL_CLOUD_QUEUES_TEST_SQS=moto|localstack`. Local Redis tests use Laravel Herd's Valkey on `127.0.0.1:6379`.

## D10 — Build orchestration and branching (2026-09-28)

- The build runs with Claude Opus 5.5 as lead orchestrator using the `solo-orchestrator` skill inside Solo. The lead routes lanes to any configured model; reviews preferably come from a different lab.
- Explicit stay-on-main run: lanes use their own worktrees and branches, and the lead integrates accepted lanes directly into `main`. There is no final PR.
- Each push to `main` deploys `probe-app/` to Laravel Cloud, used as a continuous production deploy test. Keep `main` deployable, and move `probe-app/`'s workers to `laravel-cloud-queues work` once the package can consume.

## D11 — CI provider (2026-09-28)

GitHub Actions:
- Full gate (§5) on every push to `main` and on pull requests: Python 3.10–3.14 matrix on Linux, with LocalStack and Valkey/Redis service containers.
- Upstream drift check (§2) on a weekly schedule, advisory only.

## D12 — No lifecycle events outside managed queues (2026-09-28)

Per the Laravel Cloud team, only managed queues receive queue lifecycle events today. In `sqs` and `redis` modes the package sends no lifecycle or `failed_job` events to the log socket; workers log structured lines instead. Revisit if the platform starts ingesting them for worker clusters.

## D13 — Contract decisions from the v0.1 build (2026-09-27)

Made by the lead while freezing the contract pack (`docs/contract/architecture.md`), with source evidence in `docs/contract/upstream-evidence.md`. Reviewed against the pinned sources by an agent from another lab (Codex GPT-6 Astra, review R2); its corrections are applied below.

1. **Terminal-failure order is mode-specific (D1, D6b, D12).** In managed mode, through either the agent or direct SQS, complete the message, then emit `failed_job`, then `failed` with the same timestamp. Laravel's `Job::fail` deletes the job (agent `processed` / `DeleteMessage`) before `JobFailed` triggers `FailedJobProvider::log`. This corrects the reversed managed-mode order in §11/§12 and matches D1/§15. In self-managed `sqs`/`redis` modes, preserve D6b: write the structured failure record to stdout **first**, then complete (delete/remove the reservation), so deletion cannot precede the only failure record. These modes send no socket events (D12). Terminal timeouts follow the same mode-specific order, then exit 124.
2. **Completion events are immediate; the timeout covers only handler execution and per-job teardown.** Deviation `completion-event-immediate` (follows project): emit completion lifecycle events right after the outcome is reported, with `failed_job` before `failed` on terminal failure. Laravel emits lazily at the next `pop`, on `WorkerStopping`, or right after `failed_job`, so its `duration_ms` includes `--rest` and idle time before the next poll. Deviation `timeout-window-handler-only` (follows project): arm the timer after decoding/pre-run checks, around the handler and per-job teardown, and disarm before outcome reporting or `--rest`. Laravel arms before `process()` and resets after reporting/rest, so it can time out during either. Both timing choices are intentional project deviations.
3. **`GET /next` retries follow Laravel:** up to 3 attempts (retry after 0 ms and 500 ms) on connection errors **and HTTP 4xx/5xx responses**. A 204 ends the poll as empty; a 200 is decoded; any other final status is fatal. Returned 3xx and 201 responses are not retried and are rejected immediately. This corrects §11's connection-errors-only wording: Laravel's `retry([0, 500], throw: false)` has no `when` filter, but `Response::throw()` throws only for HTTP client/server errors.
4. **Managed config is stricter than Laravel (deviations `managed-config-strict-shape` and `credentials-explicit-only`, follows project).** A `driver` other than `cloud`, a missing `connection` or `region`, and any `credentials` value other than the strings `ecs`/`instance` are configuration errors, including an absent value. Laravel skips a non-`cloud` driver and auto-creates `connection`. It also accepts provider objects/callables; when `credentials` is absent, it honors explicit `key`/`secret` (including `token`) before falling back to the SDK default chain. The project forbids that chain in managed mode because it holds object-storage keys on Laravel Cloud.
5. **Additional labeled deviations recorded in the conformance catalog:** deterministic decode/unknown-job failures are terminal on first delivery (Laravel uses normal retries; the project follows Symfony's terminal decode rule while keeping Laravel's managed events). Numeric `ApproximateReceiveCount` strings are parsed; missing/invalid values default to 1. Symfony casts and clamps; Laravel indexes the key without a fallback, and its console `HandleExceptions` handler raises `ErrorException` for a missing key. Agent telemetry uses `queueUrl` with a config fallback (Symfony). Direct SQS uses `MaxNumberOfMessages=1`; one queue uses `WaitTimeSeconds=20` with no extra `--sleep` after an empty long poll; several queues use `WaitTimeSeconds=0` in priority order, then `--sleep` if all are empty (`receive-long-poll-params`, follows Symfony's long-poll parameters, with project polling/sleep rules). Laravel always sleeps after an empty pop. Redis uses a bounded blocking wait of `--sleep` seconds. After agent `/result` 4xx, log `AgentProtocolError`, send no second outcome, and emit the chosen completion event (`outcome-event-after-ack-rejection`, follows project): Laravel can instead emit `failed` without `failed_job` when a successful handler's processed report is rejected on its last attempt. Ambiguous direct-broker acknowledgements exit 1; agent unavailability/5xx, including exhausted retries after a lost `/result` response, is `AgentUnavailableError` and exits 0.
6. **Dependencies:** `httpx` (agent client, §11) and `typing-extensions` (Python 3.10 typing) are core dependencies. UUIDv7 is implemented in-package because `failed_job.id` must be bound to the failure timestamp.
7. **Envelope layout:** Laravel's top-level `uuid` and `displayName`; every other field lives under one versioned `laravel_cloud_queues` object. Argument decoding is driven by the handler's type annotations; payloads never name Python types.
8. **Redis reservation expiry is now + lease (60 s), renewed every third of the lease by the watchdog,** rather than now + job timeout + margin (§11). D7's watchdog already protects running jobs; this makes `timeout=0` safe and bounds redelivery after a crash or a timeout exit to one lease window. Direct SQS receive likewise sets `VisibilityTimeout` to the lease.
9. **Explicit `JobContext.release()` always releases** (Laravel parity). When attempts are exhausted, the next delivery fails the pre-run check with `MaxAttemptsExceededError`.

## D14 — Trust-boundary limits from cross-lab review R1 (2026-09-27)

1. **Maximum job timeout: 604,800 seconds (7 days).** Declared policies, worker defaults and decoded envelopes reject larger values (`ConfigurationError` at declaration; `MalformedEnvelopeError` when decoding a message). `signal.setitimer` overflows on very large values, which would otherwise crash the worker instead of failing the message. `0` still disables the timeout.
2. **Redis payloads are bounded by the envelope decode ceiling (16 MiB).** §8 bounds decoded size at the trust boundary; a Redis dispatch above that ceiling is rejected at dispatch with `PayloadTooLargeError` instead of being accepted and failing on receipt. SQS and managed queues keep the 1 MiB limit. This refines §8's "no package-imposed limit" for Redis.
3. **Envelopes must be valid UTF-8.** Arguments containing lone surrogates are rejected at dispatch with `SerializationError`.
4. **Decoding work is bounded, not only decoding size:** union decoding must not be exponential in nesting depth.
