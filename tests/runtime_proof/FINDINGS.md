# Runtime proof findings

D2 process-level timeouts and D7 visibility renewal, measured before the package worker exists. The suite does not import the package. Each job runs in its own subprocess (`asyncio.run` on the main thread, sync handlers called directly on that thread, `signal.setitimer(ITIMER_REAL)`, watchdog on a `threading.Thread`).

## Environment

| | |
|---|---|
| Host | Darwin arm64 (macOS) |
| Python | CPython 3.10.20 and 3.14.3 |
| Suite | `tests/runtime_proof` |
| Wall time | 19.00 s on 3.10, 19.13 s on 3.14 |

Checked with:

```text
uv run --no-project --python 3.10 --with pytest pytest tests/runtime_proof -q
uv run --no-project --python 3.14 --with pytest pytest tests/runtime_proof -q
```

Both runs: 14 passed. Durations below are the worker's `duration_ms` (monotonic time from arming the timer to the alarm handler), except the disarm row, which is parent-process wall time.

## Gated results

`overrun` is `duration_ms` minus the armed timeout. Prompt handlers used a 0.6 s timeout and a body that would have run for 6 s. The native body is `sum(range(n))` with `n` calibrated in-process to about 1.35 s, and a 0.45 s timeout. `time.sleep` used a 0.45 s timeout and a 3 s sleep.

| Scenario | Python 3.10.20 | Python 3.14.3 |
|---|---|---|
| Retryable async (`tries=3`) | exit 124, event `released`, 608 ms, overrun 8 ms, no failure record, receive count 1 then 2 | 609 ms, overrun 9 ms, same outcome |
| Retryable sync pure-Python loop | exit 124, `released`, 610 ms, overrun 10 ms, count 1 then 2 | 608 ms, overrun 8 ms, same |
| Retryable `sum(range(n))` | exit 124, `released`, 1343 ms, overrun 893 ms, `n=261061548`, count 1 then 2 | 1370 ms, overrun 920 ms, `n=303647526`, same |
| Last attempt async (`tries=1`) | exit 124, failure record then `failed`, message deleted, 609 ms | 609 ms, same |
| Last attempt sync | 609 ms, overrun 9 ms, `failed`, deleted | 610 ms, overrun 10 ms, same |
| Last attempt `sum(range(n))` | 1349 ms, overrun 899 ms, `failed`, deleted | 1361 ms, overrun 911 ms, same |
| `fail_on_timeout` async (`tries=4`, attempt 1) | 610 ms, overrun 10 ms, `failed`, deleted | 611 ms, overrun 11 ms, same |
| `fail_on_timeout` sync | 608 ms, overrun 8 ms, `failed`, deleted | 608 ms, overrun 8 ms, same |
| `fail_on_timeout` `sum(range(n))` | 1364 ms, overrun 914 ms, `failed`, deleted | 1364 ms, overrun 914 ms, same |
| Retryable `time.sleep` | exit 124, `released`, 459 ms, overrun 9 ms, not deleted | 458 ms, overrun 8 ms, same |
| Timer disarmed after a successful noop (timeout 0.4 s, then linger 0.85 s) | exit 0, no queue lifecycle event, message deleted, wall 0.895 s | wall 0.900 s, same |
| Retryable sync timeout while the watchdog renews (lease 0.6 s, timeout 0.8 s) | exit 124, `released`, 808 ms, 2 renewals, deadline extended past the original visibility, then count 2 | 810 ms, 2 renewals, same |
| Watchdog during a 2.5 s sync loop (lease 1.0 s, renew wait = lease/3) | exit 0, 7 renewals, max gap 0.446 s, mean gap 0.411 s, concurrent receiver got nothing, count stayed 1 | 7 renewals, max gap 0.444 s, mean gap 0.409 s, same |
| `sum(range(n))` with timeout disabled (lease 0.36 s) | native window 1.347 s, 0 renewals during the call, `renew_failed` 0.0 s after the call returned, other process received and deleted (count 2), owner exit 1, no success record | native window 1.358 s, same |

Lifecycle lines are NDJSON `{"_cloud_event":"queue","type":"released"|"failed",...}` with a UTC `Y-m-d H:i:s.u` timestamp. The alarm handler also wrote that line to a Unix socket; the collector's line matched the file line, and on the terminal path the sqlite delete was visible after `os._exit(124)`.

## Which calls are interruptible

Gated:

- Pure-Python loops and `await asyncio.sleep` return to the interpreter often. The handler ran about 10 ms after the 0.6 s timeout.
- `time.sleep` is interruptible. The handler ran about 9 ms after a 0.45 s timeout, and the process exited 124 instead of sleeping the remaining 3 s.
- `sum(range(n))` is not interruptible. The handler ran only after `sum` returned.

One-shot probe on the same host and interpreters, timer 0.30 s, not part of the gated suite:

| Call | 3.10.20 | 3.14.3 |
|---|---|---|
| `re.match(r"(a+)+b", "a"*26 + "X")` (~1.1–1.3 s) | handler at 0.314 s, during the match | handler at 0.314 s, during the match |
| `hashlib.sha256` of 80 MiB (~0.57 s) | handler at 0.316 s, during `update` | handler at 0.316 s, during `update` |
| `zlib.compress` (~0.025 s) | returned before the timer | returned before the timer |

`re` and `hashlib` check in often enough for `SIGALRM` to run during the call. A competing thread made little progress during `re` (about 1.4e6 increments) and a lot during `hashlib` (about 6.6e7–6.9e7), so `hashlib` releases the GIL and `re` mostly holds it. Neither is the overrun case. `zlib.compress` at this size finished before the timer and does not show deferral.

## Native-code limitation

CPython runs a Python signal handler between bytecode instructions. `sum(range(n))` iterates in C, does not poll for signals, and holds the GIL.

On both 3.10 and 3.14 a 0.45 s timeout became a 1.34–1.37 s exit 124 (overrun about 0.9 s). The same call starved the watchdog: with a 0.36 s lease, no `renew` was recorded during the call, another process received the message once visibility expired, and the owner's renewal then failed. Failure was timestamped within 1 ms of `sum` returning (`renew_failed_after_s=0.0`). The owner exited 1 and did not record success.

That is the same limitation §14 and D2 state for Laravel: a job blocked in native code can overrun its timeout until control returns to the interpreter. Holding the GIL adds a second effect the alarm overrun alone does not show: renewal does not run either, so the lease can be lost and another worker can take the message while the first is still inside the call.

## Implications for the real worker (L6)

- Arm `signal.setitimer` on the main thread and run sync handlers on that thread. Async handlers awaited on the asyncio loop were interrupted promptly. A thread pool for sync handlers would put them where this `SIGALRM` handler does not run.
- Renewal has to be a thread, not a task on the loop. During a pure-Python handler the watchdog kept the message hidden for longer than the lease (7 renewals, gaps about 0.41 s mean and 0.45 s max on a 1.0 s lease whose wait is 0.333 s). The extra gap is the renew call plus time spent waiting for the GIL. It stayed under the lease.
- Do not claim native calls are cut off at the timeout. `sum(range(n))` overran by about 0.9 s, and there is no supervisor process in v1. The observable result is a late exit 124.
- A GIL-holding body also stops renewal. After a failed renewal the worker must not report success. This proof uses exit 1 for that case (the spec's fatal-transport code) and does not use 124. L6 should keep lost-lease distinct from timeout.
- A retryable timeout must not delete the message or apply backoff. The message came back when visibility expired, and the receive count went from 1 to 2. Renewals only move that deadline later; they do not change the outcome. `fail_on_timeout`, or `tries > 0` and `attempt >= tries`, did delete, after writing the failure record and emitting `failed`.
- The alarm handler's blocking I/O finished before `os._exit`: file `fsync`, sqlite delete on the terminal path, and the Unix-socket write. The proof bounds the socket connect/send at 2 s. A stalled collector can delay the exit up to that bound. It did not here (prompt overruns stayed near 10 ms).
- `os._exit(124)` skips `finally`, `atexit`, and lifespan cleanup. The next delivery is a new process.
- Disarm the timer after the job. The same process then slept past the original timeout and exited 0. `time.sleep` would have taken a leftover alarm and exited 124.
- `timeout=0` disables the alarm (used for the starvation case). D7 allows that only because the lease keeps renewing. That failed when `sum` held the GIL: the lease expired anyway.
