# Cloud event fixtures

Wire samples for Laravel Cloud queue observability. Lifecycle files are the exact
objects produced by `lifecycle_event` for queue `emails` at
`2026-09-27 12:00:00.123456` UTC (`duration_ms` 42 on completion events). The
`failed_job` files illustrate the three D1 branches; their `id` values are
example UUIDv7s, not a recording of one `uuid7()` call.

Encoding for every line is compact UTF-8 JSON with a trailing newline when
written to the socket: unescaped slashes and Unicode, zero fractions preserved,
invalid UTF-8 replaced with U+FFFD.

D1 measures the encoded line in bytes, including that newline and JSON escaping.
Symfony's payload projection (`uuid` / `displayName` / truncated `body` only) is
not used.

| Fixture | What it shows | Upstream |
|---|---|---|
| `queued.json` | `queued`, no `duration_ms` | `framework/src/Illuminate/Foundation/Cloud/Queue.php:520-525` |
| `started.json` | `started`, no `duration_ms` | `framework/src/Illuminate/Foundation/Cloud/Queue.php:545-550` |
| `processed.json` | `processed` + `duration_ms` | `framework/src/Illuminate/Foundation/Cloud/Queue.php:481-491` |
| `released.json` | `released` + `duration_ms` | `framework/src/Illuminate/Foundation/Cloud/Queue.php:481-491` |
| `failed.json` | `failed` + `duration_ms` | `framework/src/Illuminate/Foundation/Cloud/Queue.php:481-491` |
| `failed_job.json` | full `failed_job` record | `framework/src/Illuminate/Foundation/Cloud/FailedJobProvider.php:70-87` |
| `failed_job_exception_trimmed.json` | exception head + `\n... [truncated N bytes]`, payload intact | `FailedJobProvider.php:70-87`; size ceiling `symfony-on-cloud/src/Queue/QueueEventSubscriber.php:50-60` |
| `failed_job_not_replayable.json` | payload head, `"replayable": false` | same D1 policy; do not copy `QueueEventSubscriber.php:202-219` |

Other citations the builders follow:

- Timestamp `Y-m-d H:i:s.u`: `Queue.php:483` (`toDateTimeString('microsecond')`).
- `duration_ms`: `Queue.php:490` (`(int) diffInMilliseconds`, non-negative here).
- Preview `mb_substr(..., 0, 1001)`: `FailedJobProvider.php:77-84`.
- `id` is UUIDv7 from the failure timestamp: `FailedJobProvider.php:72`, `Str.php:2094`.
- JSON flags: `framework/src/Illuminate/Foundation/Cloud/Events.php:124`.
- Socket connect/write timeouts, EOF, partial writes, 5 zero-byte writes: `Events.php:77-107`, `Events.php:148-160`, `Events.php:180`.

`sqs` / `redis` failure records (D6b) are stdout lines, not these socket events, and are not trimmed to 16 KiB.
