# Worker contract: delivery state machine and outcome ownership

Decisions D2, D4, D6b, D7, D12, D13. Laravel references: `Illuminate\Queue\Worker`,
`Foundation\Cloud\{Queue,QueueConnector,FailedJobProvider,CloudJob}` (see
`upstream-evidence.md` for line numbers).

## Process

- One process, one in-flight delivery, main thread. `anyio.run(main, backend="asyncio")`.
- The consumer is an `AsyncConsumer` (`backend.open_async_consumer()`): native for `redis` and
  the agent, threaded for SQS. receive/complete/release are awaited on the loop. Every loop
  iteration yields at least once, so stop signals run even when a receive never suspends.
- The target's `lifespan()` is entered once at start and exited on clean termination.
- SIGTERM/SIGINT (via the loop's signal handling): set `stopping`, call
  `consumer.interrupt()`. The current delivery always runs to completion and is reported.
  Repeated signals never skip reporting. A message handed over after the signal is still run.
  `interrupt()` never aborts an in-flight receive: like Laravel, an idle agent worker waits
  for the current `GET /next` (at most its 65 s timeout, within Flex's 90 s) and a single-queue
  SQS worker for its long poll (at most 20 s), so the agent never has a popped message with no
  worker to hand it to. Only the next receive returns immediately.
- SIGALRM uses `signal.signal` so it interrupts sync handlers on the main thread (D2). It
  applies to `async def` handlers too; there is no per-job cancellation.

## Per-delivery states

`received -> running -> outcome chosen -> reporting -> completed | ambiguous`

1. **received**: `Delivery` (message id, receipt, logical queue, attempt, body) is captured
   before any decoding. Record `started_at` (UTC). Emit `started` in managed mode.
2. **decode**: `prepare_execution(registry, body)`. `JobDefectError` -> terminal failure (below)
   on this first delivery; never retried, never imports anything.
3. **pre-run check**: `policy.exceeded_before_run(attempt)` -> terminal with
   `MaxAttemptsExceededError` (handler not run).
4. **running**: build `JobContext`; arm `setitimer(ITIMER_REAL, policy.timeout)` (0 = off);
   start lease renewal when `consumer.supports_renewal` (renews `lease_seconds` every third;
   `LeaseLostError` or 3 consecutive failures set `lease_lost`). `async def` handlers: an
   `AsyncWatchdog` task awaiting `consumer.renew` on the loop (runs only while the handler
   awaits). Other handlers: the `Watchdog` thread on `consumer.blocking`.
   `await run_prepared(...)`, including per-job teardown.
5. **outcome chosen** (exactly one, recorded outcome wins over success/exception):
   disarm the timer; stop the renewal (the thread join runs off the loop). If no renewal landed
   within the last lease window, ownership is confirmed with one more renew first.
   - `lease_lost` -> do not report anything; log; stop worker, exit 1.
   - `success` -> complete.
   - `release(delay)` (explicit) -> release(delay).
   - `fail(reason)` (explicit) -> terminal with `JobFailedError`/given exception.
   - `error` -> `is_last_attempt(attempt)` ? terminal : release(`retry_delay(attempt)`).
6. **reporting**: one transport outcome, with bounded retries. For terminal failures in every
   mode, log the structured failure record **before** completing the message (D6b, D15).
   Managed mode: report to the transport, then emit completion events. `sqs`/`redis`: no
   socket events.
7. **completed** or **ambiguous** (below).

## Outcome table

| Outcome | Managed mode (agent or direct SQS; log socket) | Self-managed `sqs` / `redis` (log lines only) |
|---|---|---|
| success | `complete` -> `processed` | `complete` -> info line |
| retry (error or explicit release) | `release(delay)` -> `released` | `release(delay)` -> info line |
| terminal (error on last attempt, explicit fail, pre-run exceeded, job defect) | failure record line -> `complete` -> `failed_job` -> `failed` (same timestamp) | failure record line -> `complete` (D6b) |
| timeout, retryable | no transport call; `released` -> exit 124 | no transport call; info line -> exit 124 |
| timeout, terminal (last attempt or `fail_on_timeout`) | failure record line -> `complete` -> `failed_job` (`JobTimeoutError`) -> `failed` (same timestamp) -> exit 124 | failure record line -> `complete` -> exit 124 |

Timeout path runs inside the SIGALRM handler: apply the terminal check, then follow the
mode-specific sequence above, settling through `consumer.blocking`, then
`laravel_cloud_logging.flush(0.5)` (atexit does not run) and `os._exit(124)`, which runs even
if the flush fails. A retryable timeout makes no release
call or backoff change; visibility/reservation expiry redelivers. Self-managed modes never
send socket events (D12). Every write in this path uses bounded lock waits (`lock_timeout`)
so it cannot deadlock on a write the main thread was performing.

`duration_ms` = non-negative, truncated milliseconds from `started_at` to the completion
timestamp. Deviation `completion-event-immediate` (follows project): completion events follow
outcome reporting immediately; Laravel emits at next pop/stop, including `--rest` and idle
time before the next poll. Terminal `failed_job` still precedes `failed`.

Deviation `timeout-window-handler-only` (follows project): the timer covers the handler and
per-job teardown only, excluding decoding/pre-run checks, outcome reporting and `--rest`.
Laravel arms before `process()` and resets after reporting/rest, so it can time out there.

## Acknowledgement failures (never treated as handler errors)

| Condition | Worker behavior | Exit |
|---|---|---|
| Agent `/result` 4xx (`AgentProtocolError`) | log; do not assume ack; never send a second outcome; completion event still reflects the outcome; continue | — |
| Agent unreachable after retries / 5xx (`AgentUnavailableError`), including a lost `/result` response whose retries fail | log agent loss; emit completion event; stop fetching | 0 |
| Direct-broker SQS / Redis ack failure after bounded retries (`AmbiguousAcknowledgementError`, `BrokerConnectionError`) | log; stop fetching | 1 |
| Lost lease (`LeaseLostError`) | log; do not report | 1 |

Deviation `outcome-event-after-ack-rejection` (follows project): after `/result` 4xx the
completion event reflects the chosen outcome. Laravel instead emits `failed` without
`failed_job` when a successful handler's processed report is rejected on its last attempt.
The exit-1 ambiguous-ack rule applies only to direct brokers; agent unavailability exits 0.

## Loop and stop conditions

- Queues: agent mode ignores `--queue`; a `--queue` that differs from the Cloud assignment is
  a startup `ConfigurationError` (exit 2). Direct modes: CLI priority list or the backend
  default queue. Single SQS queue: `WaitTimeSeconds=20`, with no extra `--sleep` after an
  empty long poll. Several queues: `WaitTimeSeconds=0` in priority order, then `--sleep`
  only when all are empty. Redis: bounded blocking wait of `--sleep` seconds, with no extra
  sleep after the wait (no busy polling).
- Direct SQS receive sets `MaxNumberOfMessages=1`, requests `ApproximateReceiveCount`, and
  sets `VisibilityTimeout=lease_seconds` (default 60 s). Redis reservation expiry is now +
  lease; both renew every lease/3, allowing `timeout=0` (D13.8).
- Deviation `receive-long-poll-params` follows Symfony's single-queue SQS parameters;
  the priority short polls and sleep rules are project choices. Laravel always sleeps
  `--sleep` after an empty pop, even if the receive already waited.
- Transient receive error (`TransportError`): log, sleep 1 s, retry.
- After each delivery: `--rest` seconds.
- Stop (exit 0): signal, `--max-jobs`, `--max-time`, `--stop-when-empty` (first empty poll),
  `--stop-when-empty-for` (seconds since last job, or since start).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean stop, or agent unhealthy (Laravel parity) |
| 1 | Other fatal transport error: lost lease, broker connection lost, direct-broker ambiguous acknowledgement |
| 2 | Configuration error at startup (logged clearly on every start; restart-loops on clusters) |
| 124 | Job timeout |
