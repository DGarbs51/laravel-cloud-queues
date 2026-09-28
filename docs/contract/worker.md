# Worker contract: delivery state machine and outcome ownership

PROJECT_SCOPE.md §11-§14, D2, D4, D6b, D7, D12. Laravel references: `Illuminate\Queue\Worker`,
`Foundation\Cloud\{Queue,QueueConnector,FailedJobProvider,CloudJob}` (see
`upstream-evidence.md` for line numbers).

## Process

- One process, one in-flight delivery, main thread. `anyio.run(main, backend="asyncio")`.
- The target's `lifespan()` is entered once at start and exited on clean termination.
- SIGTERM/SIGINT (via the loop's signal handling): set `stopping`, call
  `consumer.interrupt()`. The current delivery always runs to completion and is reported.
  Repeated signals never skip reporting. A message handed over after the signal is still run.
- SIGALRM uses `signal.signal` so it interrupts sync handlers on the main thread (D2).

## Per-delivery states

`received -> running -> outcome chosen -> reporting -> completed | ambiguous`

1. **received**: `Delivery` (message id, receipt, logical queue, attempt, body) is captured
   before any decoding. Record `started_at` (UTC). Emit `started`.
2. **decode**: `prepare_execution(registry, body)`. `JobDefectError` -> terminal failure (below)
   on this first delivery; never retried, never imports anything.
3. **pre-run check**: `policy.exceeded_before_run(attempt)` -> terminal with
   `MaxAttemptsExceededError` (handler not run).
4. **running**: build `JobContext`; arm `setitimer(ITIMER_REAL, policy.timeout)` (0 = off);
   start the watchdog when `consumer.supports_renewal` (renews `lease_seconds` every third;
   a failed renewal sets `lease_lost`); `await run_prepared(...)`.
5. **outcome chosen** (exactly one, recorded outcome wins over success/exception):
   disarm the timer; stop + join the watchdog.
   - `lease_lost` -> do not report anything; log; stop worker, exit 1.
   - `success` -> complete.
   - `release(delay)` (explicit) -> release(delay).
   - `fail(reason)` (explicit) -> terminal with `JobFailedError`/given exception.
   - `error` -> `is_last_attempt(attempt)` ? terminal : release(`retry_delay(attempt)`).
6. **reporting**: one transport call. Then the completion event.
7. **completed** or **ambiguous** (below).

## Outcome table

| Outcome | Transport call | Managed mode (log socket) | `sqs` / `redis` (stdout) |
|---|---|---|---|
| success | `complete` | `processed` | info line |
| retry (error or explicit release) | `release(delay)` | `released` | info line |
| terminal (error on last attempt, explicit fail, pre-run exceeded, job defect) | `complete` | `failed_job` then `failed` (same timestamp) | failure record line (D6b) |
| timeout, retryable | none (visibility / reservation expiry redelivers) | `released` | info line |
| timeout, terminal (last attempt or `fail_on_timeout`) | `complete` | `failed_job` (`JobTimeoutError`) then `failed` | failure record line |

Timeout path runs inside the SIGALRM handler: terminal check -> (terminal: `complete`, failure
record) -> lifecycle event -> `os._exit(124)`. Every write in that path uses bounded lock
waits (`lock_timeout`) so it cannot deadlock on a write the main thread was performing.

`duration_ms` = truncated milliseconds from `started_at` to the completion timestamp.

## Acknowledgement failures (never treated as handler errors)

| Condition | Worker behavior | Exit |
|---|---|---|
| Agent `/result` 4xx (`AgentProtocolError`) | log; do not assume ack; never send a second outcome; completion event still reflects the outcome; continue | — |
| Agent unreachable / 5xx after retries (`AgentUnavailableError`) | log agent loss; emit completion event; stop fetching | 0 |
| Direct SQS / Redis ack failure after bounded retries (`AmbiguousAcknowledgementError`, `BrokerConnectionError`) | log; stop fetching | 1 |
| Lost lease (`LeaseLostError`) | log; do not report | 1 |

## Loop and stop conditions

- Queues: agent mode ignores `--queue`; a `--queue` that differs from the Cloud assignment is
  a startup `ConfigurationError` (exit 2). Direct modes: CLI priority list or the backend
  default queue. Single SQS queue: `receive(wait=20)`; several: short polls in order, then
  `--sleep`. Redis: bounded blocking wait of `--sleep` seconds (no busy polling).
- Empty poll (direct/Redis): `--sleep` unless the receive already waited.
- Transient receive error (`TransportError`): log, sleep 1 s, retry.
- After each delivery: `--rest` seconds.
- Stop (exit 0): signal, `--max-jobs`, `--max-time`, `--stop-when-empty` (first empty poll),
  `--stop-when-empty-for` (seconds since last job, or since start).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean stop, or agent unhealthy (Laravel parity) |
| 1 | Other fatal transport error: lost lease, broker connection lost, ambiguous acknowledgement |
| 2 | Configuration error at startup (logged clearly on every start; restart-loops on clusters) |
| 124 | Job timeout |
