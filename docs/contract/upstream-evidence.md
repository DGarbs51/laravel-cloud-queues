# Upstream evidence map (lane L0R)

> Historical build record. Section references (`§N`) point to `PROJECT_SCOPE.md`, which was removed on 2026-09-28; see git history. Current behaviour is defined by `docs/decisions.md` and `docs/contract/`.

Verification of the build's "Resolved conflicts" table and the lead's open questions against the pinned sources. Every claim below was re-read at the pin; nothing was carried over from the 2026-09-27 audits without re-checking.

Pins (verified locally with `git rev-parse HEAD` / `git describe --tags`, working trees clean):

- `LF:` = `laravel/framework` `v13.33.0` (`91188a17ceaa3dbace6e8a5f7abd0d042e466359`), paths under `src/Illuminate/`.
- `SOC:` = `laravel/symfony-on-cloud` `50c945170b6cb5690370d15fd725c6f82495e9ba`, paths under `src/` and `tests/`.

Evidence hierarchy: executed code > tests > comments > docs. Where a statement rests on a comment or on code not in the pin (Symfony Messenger's worker, the Cloud agent, AWS), it is marked **not determinable from source**.

Line numbers are for the pinned files; ranges are inclusive.

---

## 1. Resolved-conflicts table, row by row

Verdict legend: **confirmed** = the row's Laravel and Symfony cells match executed code; **WRONG** = the row misstates one of them (with the corrected statement). Nuances are things the row is silent on that matter for the contract.

### Row 1 — Agent receive selection

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Foundation/Cloud/Queue.php:242-253` `pop()`; `:392-397` `usesAgent()`; `:404-408` `workerQueue()` | `usesAgent($queue)` is `agent.enabled && runningConsoleCommand('queue:work') && getQueue($queue) === getQueue(workerQueue())`, where `workerQueue()` is the `--queue` CLI option, else config `queue`, else `default`. A mismatched queue pops SQS directly. |
| Symfony | `SOC:Queue/Messenger/CloudQueueTransport.php:49-52` `get()`; `:67-82` `getFromQueues()` | `$this->useAgent ? getFromAgent() : ...`; `getFromQueues()` returns `getFromAgent()` before looking at `$queueNames`. `useAgent` is injected from `ManagedQueueConfig::agentAvailable()` (`SOC:Queue/ManagedQueueConfig.php:196-199`, `SOC:Queue/Messenger/CloudQueueTransportFactory.php:54`). |

Verdict: **confirmed**. Nuances: a Laravel worker with `--queue=high,low` never uses the agent because `workerQueue()` returns the whole comma list (test `LF:tests/Foundation/Cloud/QueueTest.php:546-561`). Laravel also requires the running command to be `queue:work`.

### Row 2 — `GET /next` retries

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Foundation/Cloud/Queue.php:294-324` `requestNextJobFromAgent()`: `->timeout(65)->retry([0, 500], throw: false)->get('/next')`; `LF:Support/helpers.php:311-346` `retry()`: array `$times` → `count + 1` attempts, delay `$backoff[$attempts - 1]`; `LF:Http/Client/PendingRequest.php:1055-1128` `send()` | Three attempts total; sleeps 0 ms after the first failure and 500 ms after the second. Because no `when` callback is given, both `ConnectionException` and HTTP 4xx/5xx responses are retried (`send()` throws the response on attempts 1–2; `throw: false` returns the final response instead of throwing). After HTTP handling: 204 → `null`; any other non-200 response → `AgentUnreachableException`; non-array JSON on 200 → `AgentUnreachableException`. Returned 3xx and 201 responses are not retried: `Response::throw()` throws only for client/server errors (`LF:Http/Client/Response.php:250-272, 352-362`). |
| Symfony | `SOC:Queue/Agent/AgentClient.php:44-76` `next()` | One request; `ConnectException` → `TransportException`; any other `GuzzleException` → `TransportException`. |

Verdict: **confirmed**. Test evidence: `LF:tests/Foundation/Cloud/QueueTest.php:1206-1250` (`testPopRetriesATimedOutLongPollImmediately`, `testPopThrowsWhenEveryLongPollAttemptTimesOut`: 3 requests, one `usleep(500_000)`). Nuance: Laravel also retries HTTP 4xx/5xx responses (see §3, question 4).

### Row 3 — 200 without `messageId`

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Foundation/Cloud/Queue.php:264-266` | `if (! (is_array($data) && is_string($messageId = $data['messageId'] ?? null) && $messageId !== '')) return null;` |
| Symfony | `SOC:Queue/Messenger/CloudQueueTransport.php:304-306` | Same predicate on `messageId`. |

Verdict: **confirmed**. A JSON array body (`[]`) passes the `is_array` check in both and becomes an empty poll; `"0"` is a valid ID (`LF:tests/Foundation/Cloud/QueueTest.php:982-991`).

### Row 4 — `/result` 4xx

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Foundation/Cloud/Queue.php:332-358` `reportJobStatusToAgent()` | `->throw()->retry(3, 100, fn ($e) => $e instanceof ConnectionException)`; `catch (RequestException $e)`: `serverError()` → `AgentUnreachableException`, otherwise `throw $e` (the plain `RequestException`). `LF:Foundation/Cloud/AgentAwareLostConnectionDetector.php:27-31` only treats `AgentUnreachableException` as a lost connection, so `Worker::runJob()` (`LF:Queue/Worker.php:558-573`) reports the exception and the loop continues. |
| Symfony | `SOC:Queue/Agent/AgentClient.php:125-133` | `>= 500` → `TransportException`; `>= 400` → `RuntimeException`. Test `SOC:tests/AgentClientTest.php:119-124` (409). |

Verdict: **confirmed**. Test evidence for "no second report": `LF:tests/Foundation/Cloud/QueueTest.php:992-1015` (422 on `delete()` → one `processed` report, `RequestException`). Whether Symfony's Messenger worker continues after the `RuntimeException` is **not determinable from source** (the worker is not in the pin; `AgentClient.php:84-90` is a comment).

### Row 5 — Agent-loss exit status

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Queue/Worker.php:37` `EXIT_SUCCESS = 0`; `:419-432` `stopIfNecessary()` → `$this->lostConnection => [static::EXIT_SUCCESS, WorkerStopReason::LostConnection]`; `:581-586` `stopWorkerIfLostConnection()`; `:501-507` (`getNextJob` catch: report, mark lost, `sleep(1)`) | `AgentUnreachableException` from pop or from a report sets `lostConnection`; the next `stopIfNecessary()` returns 0. On the pop path Laravel sleeps 1 s before the loop reaches `stopIfNecessary()`. |
| Symfony | `SOC:Queue/Agent/AgentClient.php:35-38, 84-90` (comments); `:53-57, 114-122, 127-129` | Raises Messenger `TransportException`; the consumer's exit code is **not determinable from source**. |

Verdict: **confirmed**.

### Row 6 — Timeout

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Queue/Worker.php:319-356` `registerTimeoutHandler()`; `:363-366` `resetTimeoutHandler()`; `:375-378` `timeoutForJob()`; `:729-740`, `:750-766`, `:791-796` failure checks; `:1057-1073` `kill()`; `LF:Foundation/Cloud/QueueConnector.php:101-134` `configureWorker()` | SIGALRM handler: `markJobAsFailedIfWillExceedMaxAttempts`, `...MaxExceptions`, `...ShouldFailOnTimeout` (each may call `fail()`), `JobTimedOut` event, then `kill($timedOutExitCode ?? 1)`. Cloud sets `$timedOutExitCode = 124`, `killUsing(pcntl_exec('/bin/sh', ['-c', 'exit 124']))`, and a `WorkerStopping` listener that calls `finishProcessingJob(default: 'released')` for `TimedOut`. No `release()` and no backoff on this path. |
| Symfony | (none) | `grep -rn "alarm\|pcntl" SOC:src` finds nothing; no timeout mechanism. |

Verdict: **confirmed**. Test: `LF:tests/Foundation/Cloud/QueueTest.php:174-199`, `:1346-1383`. See §3 question 8 for the exact order.

### Row 7 — Retry delay rounding

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Queue/Worker.php:817-827` `calculateBackoff()`: `(int) ($backoff[$job->attempts() - 1] ?? last($backoff))` | PHP `(int)` cast truncates toward zero. `SqsJob::release()` (`LF:Queue/Jobs/SqsJob.php:66-86`) forwards the value unclamped. |
| Symfony | `SOC:Queue/Messenger/CloudQueueTransport.php:151-173` | `min(43_200, (int) ceil($milliseconds / 1000))` with `max(0, delay)` first. |

Verdict: **confirmed**.

### Row 8 — Fresh delay > 900 s

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Queue/SqsQueue.php:579-592` `getQueueableOptions()`: `if (! empty($delay) && ! $isFifo) $options['DelaySeconds'] = $this->secondsUntil($delay);`; `LF:Support/InteractsWithTime.php:17-24` | No cap; forwarded to SQS. |
| Symfony | `SOC:Queue/Messenger/CloudQueueTransport.php:233-248` | `intdiv(ms, 1000) > 900` → `TransportException`. |

Verdict: **confirmed**. Nuance: Symfony floors before comparing (900 999 ms passes as 900).

### Row 9 — FIFO + delay

Laravel `LF:Queue/SqsQueue.php:589-592` omits `DelaySeconds` when `$isFifo`. Symfony `SOC:Queue/Messenger/CloudQueueTransport.php:209-216` throws for a positive `DelayStamp` on a FIFO queue. Verdict: **confirmed**.

### Row 10 — FIFO/fair cross-model options

Laravel `LF:Queue/SqsQueue.php:594-635`: one `messageGroup` property/method feeds `MessageGroupId` on both queue types; `MessageDeduplicationId` is only computed for FIFO; nothing is rejected. Symfony `SOC:Queue/Messenger/CloudQueueTransport.php:193-222`: `CloudMessageGroupStamp` on FIFO → throw; `CloudFifoStamp` on standard → throw. Verdict: **confirmed**.

### Row 11 — Default FIFO dedup

| | Source | Behaviour |
|---|---|---|
| Laravel | `LF:Queue/SqsQueue.php:619-635`; `LF:Support/Str.php:2106-2120` `orderedUuid()` | `default => (string) Str::orderedUuid()` (a COMB/time-ordered v4, not v7). `array_filter($options)` drops falsy values, so an explicit `''` (and `'0'`) omits the attribute. |
| Symfony | `SOC:Queue/Messenger/CloudQueueTransport.php:206-207` | `$fifo?->messageDeduplicationId ?? (string) Uuid::v7()`; `''` survives `??`. |

Verdict: **confirmed**.

### Row 12 — Default tries

Laravel `LF:Queue/WorkerOptions.php:107-134`: `$maxTries = 1` (also `$backoff = 0`, `$timeout = 60`, `$sleep = 3`, `$rest = 0`, `$memory = 128`). Symfony `SOC:README.md:171-190`: Messenger default 3 retries (4 deliveries); the Messenger source itself is not in the pin. Verdict: **confirmed**.

### Row 13 — Retry policy location

Laravel `LF:Queue/Queue.php:174-193` `createObjectPayload()` writes `maxTries`, `maxExceptions`, `failOnTimeout`, `backoff`, `timeout`, `retryUntil` into the payload; the worker reads them through `LF:Queue/Jobs/Job.php:294-347` and falls back to `WorkerOptions` only when the payload value is `null` (`LF:Queue/Worker.php:703, 731, 821-823`, `:377`). Symfony: Messenger `retry_strategy` config (`SOC:README.md:171-190`); the transport reconstructs a `RedeliveryStamp` from `ApproximateReceiveCount` (`SOC:Queue/Messenger/CloudQueueTransport.php:380-391`). Verdict: **confirmed**. Laravel's "payload wins, worker default fills gaps" is exactly D4.

### Row 14 — `failed_job` fields

Laravel `LF:Foundation/Cloud/FailedJobProvider.php:70-87`: `_cloud_event`, `id`, `queue`, `started_at`, `attempts`, `payload`, `exception_preview`, `job_name`, `exception`. Symfony `SOC:Queue/QueueEventSubscriber.php:174-187`: `_cloud_event`, `id`, `queue`, `started_at`, `attempts`, `payload` (projected/trimmed, `:201-220`), `exception` (≤ 4000 bytes + marker, `:226-235`); no `exception_preview`, no `job_name`. Verdict: **confirmed**.

### Row 15 — `failed_job` / `failed` order

Laravel `LF:Foundation/Cloud/FailedJobProvider.php:67-91`: `$timestamp = now('UTC')`; `emit(failed_job)`; `$this->queue->finishProcessingJob(timestamp: $timestamp)` → `failed` with the same timestamp. Symfony `SOC:Queue/QueueEventSubscriber.php:151-187`: lifecycle (`released`/`failed`) first, then `failed_job`. Verdict: **confirmed**.

### Row 16 — `duration_ms`

Laravel `LF:Foundation/Cloud/Queue.php:490`: `(int) $this->processingJobStartedAt->diffInMilliseconds($timestamp)` — truncation; Carbon 3 (`LF:composer.json:46`, `nesbot/carbon ^3.8.4`) returns a signed float, so no clamp. Symfony `SOC:Queue/QueueEventSubscriber.php:275-284`: `(int) max(0, round(...))`. Verdict: **confirmed**.

### Row 17 — Malformed managed config

Laravel `LF:Foundation/CloudBootstrapper.php:217-241`: `json_decode(..., flags: JSON_THROW_ON_ERROR)` → `JsonException`. Symfony `SOC:Queue/ManagedQueueConfig.php:48-92`: `is_array($decoded) ? $decoded : []` → reports "not configured". Verdict: **confirmed** for malformed JSON. Nuance: Laravel does **not** throw for a non-`cloud` `driver` (`bootManagedQueues` returns silently, `:264-268`) or a missing `connection` (created by `??=`, `:225-238`); the scope's stricter shape checks are a project decision.

### Row 18 — `credentials: "ecs"`

Laravel `LF:Queue/Connectors/SqsConnector.php:87-113` `resolveCredentialProvider()`: `match` on the string: `ecs` → `CredentialProvider::ecsCredentials`, `instance` → `instanceProfile`, other string → `InvalidArgumentException`. `withCredentials()` (`:61-78`): when the key is absent, it first honors explicit `key`/`secret` (including `token`), else optionally a cached default provider, else the SDK's default chain. Non-string values are not all fallbacks: `resolveCredentialProvider()` (`:89-107`) also accepts provider objects (for example `{"provider":"ecs"}`) and callable providers. The project accepts only the strings `ecs`/`instance` in managed mode (`credentials-explicit-only`, D13.4). Symfony `SOC:Queue/Sqs/SqsClientFactory.php:20-36`: `ecs` → ECS provider; anything else → SDK default chain; region defaults to `us-east-1`. Verdict: **confirmed**.

### Row 19 — Queue URL

Laravel `LF:Queue/SqsQueue.php:687-712`: `getQueue()` returns the value unchanged when `filter_var($queue, FILTER_VALIDATE_URL)` succeeds; `suffixQueue()` uses `rtrim($this->prefix, '/')` and `Str::finish()` (`LF:Support/Str.php:496-501`: strips repeated trailing suffix, appends once); `.fifo` handled via `Str::beforeLast`. Symfony `SOC:Queue/ManagedQueueConfig.php:130-143`: `sprintf('%s/%s%s', $prefix, $queue, $suffix)` (no URL check, no finish). Verdict: **confirmed**.

### Row 20 — Agent socket fallback

Laravel `LF:Foundation/Cloud/Queue.php:365-374`: `$this->config['agent']['socket'] ?? '/tmp/cloud-agent.sock'`. Symfony `SOC:Queue/ManagedQueueConfig.php:66-73` + `SOC:LaravelCloudBundle.php:101-104` (`%env(default:laravel_cloud.agent_socket_default:LARAVEL_CLOUD_AGENT_SOCKET)%`): config socket (non-empty string) → `LARAVEL_CLOUD_AGENT_SOCKET` → `/tmp/cloud-agent.sock`. Verdict: **confirmed**.

---

## 2. Summary

| Row | Verdict |
|---|---|
| 1–20 | confirmed (no row is WRONG) |

Nuances that change fixtures are listed with the rows and in §4.

---

## 3. Lead's questions

### Q1. Acknowledge before or after the lifecycle event?

**Before, for every outcome.** The lifecycle event is a by-product of Laravel's `pop()`/stop bookkeeping, not of the acknowledgement.

- **processed:** `LF:Queue/CallQueuedHandler.php:69-102` `call()` ends with `if (! $job->isDeletedOrReleased()) $job->delete();` — still inside `$job->fire()` (`LF:Queue/Worker.php:617`). `CloudJob::delete()` (`LF:Foundation/Cloud/CloudJob.php:38-47`) sets the flag, then `report('processed')` (POST `/result`); `SqsJob::delete()` (`LF:Queue/Jobs/SqsJob.php:93-112`) calls `DeleteMessage`. The `processed` lifecycle event is emitted only later: at the **next** `pop()` (`LF:Foundation/Cloud/Queue.php:244` → `finishProcessingJob()` `:473-497`) or by the `WorkerStopping` listener (`LF:Foundation/Cloud/QueueConnector.php:120-123`). Test: `LF:tests/Foundation/Cloud/QueueTest.php:408-435` (`testItEmitsProcessedEventWhenNextJobIsAboutToPop`).
- **released:** `LF:Queue/Worker.php:671-684` (`finally` of `handleJobException`): `$job->release($backoff)` → `CloudJob::release()` (`CloudJob.php:55-61`) reports `released` with `delay`; `SqsJob::release()` calls `ChangeMessageVisibility`. The exception is rethrown (`:686`) and the `released` event follows at the next `pop()`.
- **terminal failure:** `LF:Queue/Jobs/Job.php:182-225` `fail()`: `markAsFailed()` → (return if already deleted) → `try { $this->delete(); $this->failed($e); } finally { dispatch(JobFailed) }`. `delete()` is the acknowledgement (POST `processed` / `DeleteMessage`). `JobFailed` reaches `WorkCommand::logFailedJob()` (`LF:Queue/Console/WorkCommand.php:203-207, 421-428`) → `FailedJobProvider::log()` (`LF:Foundation/Cloud/FailedJobProvider.php:57-92`): `failed_job` emitted, then `finishProcessingJob(timestamp:)` emits `failed`. **Exact order: delete/`processed` → `failed_job` → `failed`.** Because the dispatch is in a `finally`, the two events are still emitted when `delete()` throws (agent 4xx/5xx).

Project resolution (D13.1): D1/§15 match Laravel in managed mode, through either the agent or direct SQS: complete → `failed_job` → `failed` (same timestamp). This supersedes the reversed managed-mode wording in §11/§12. Self-managed `sqs`/`redis` retains D6b: write the structured failure record to stdout first, then complete; no socket events (D12). Terminal timeouts follow the same mode-specific order, then exit 124.

### Q2. Undecodable payload / unknown job: is `started` emitted first? Which events?

Laravel emits `started` in `pop()` for any non-null job (`LF:Foundation/Cloud/Queue.php:535-551`) before anything decodes the body (`Job::payload()` is lazy, `LF:Queue/Jobs/Job.php:284-287`). Then:

- `process()` → `markJobAsFailedIfAlreadyExceedsMaxAttempts` reads `maxTries`/`retryUntil` through `?? null` (`Job.php:294-297, 344-347`), so a `null` decode falls back to worker options; `attempts()` comes from `ApproximateReceiveCount` (`SqsJob.php:132-135`).
- `fire()` (`Job.php:96-103`) does `JobName::parse($payload['job'])` / `resolve($class)`; a malformed body or unknown class throws here.
- `handleJobException` applies the normal retry checks (`Worker.php:648-687`). With the default `tries = 1` the job is failed: `fail()` → ack `processed` → `failed()` (throws again for a malformed body, inside the `try`) → `finally` `JobFailed` → `failed_job` (with `job_name` `''`, since `json_decode` fails and `?? []` applies, `FailedJobProvider.php:85`) → `failed`.

Events for a poison message on first delivery with default tries: **`started`, `failed_job`, `failed`**. Laravel has no "deterministic defect" rule: with `tries > 1` (payload or `--tries`) a malformed body or unknown class is **released and retried** like any exception.

Symfony: `decode()` (`SOC:Queue/Messenger/CloudQueueTransport.php:364-378`) reports `processed`/deletes on `MessageDecodingFailedException` and rethrows before an envelope exists; the subscriber only reacts to envelopes carrying `CloudQueueReceivedStamp` (`SOC:Queue/QueueEventSubscriber.php:256-259`), so it emits **no** `started`/`failed`/`failed_job` for a poison message. Whether Messenger's worker survives an exception thrown from `get()` is not determinable from source.

A `{"@pointer": ...}` body with overflow disabled behaves as a malformed body in Laravel (`SqsJob::overflowPointer()` returns `null` when `overflow.enabled` is false, `LF:Queue/Jobs/SqsJob.php:170-189`); with overflow enabled it is hydrated from the cache (`:152-163`).

### Q3. `/result` returns 4xx: which events; does the loop continue?

The loop continues in every case (`Worker::runJob()` `LF:Queue/Worker.php:558-573`: report, `stopWorkerIfLostConnection` is false for a plain `RequestException`). The flags are set before the report, so the job is never reported twice (`CloudJob.php:41-43, 58-60`; test `QueueTest.php:992-1015`). What is emitted depends on where the 4xx happened:

1. **4xx on `processed` after a successful handler** (inside `fire()`): `deleted = true`, exception propagates out of `fire()` → `handleJobException` → with `tries = 1` `markJobAsFailedIfWillExceedMaxAttempts` calls `fail()`, which returns early because `isDeleted()` (`Job.php:186-188`) — no `JobFailed`, so **no `failed_job`**; but `markAsFailed()` ran, so the next `pop()` emits a **`failed`** lifecycle event. With attempts remaining, nothing marks the job and the next `pop()` emits `processed`.
2. **4xx on `released`**: `released = true` was set first; the next `pop()` emits **`released`**.
3. **4xx on `processed` inside `fail()`**: `JobFailed` still fires from the `finally`, so **`failed_job` then `failed`** are emitted, then the `RequestException` propagates.

Symfony: `RuntimeException` (`AgentClient.php:131-133`); event behaviour after it is not determinable from source.

Project decision (D13.5): emit the lifecycle event for the chosen outcome (`processed`/`released`/`failed`) after 4xx, log `AgentProtocolError`, and do not report again. Label `outcome-event-after-ack-rejection` (follows project). Laravel differs when a successful handler's processed report is rejected on its last permitted attempt: it emits `failed` without `failed_job`. With attempts remaining it can emit `processed`; release/terminal paths remain as described above.

### Q4. Exact agent protocol

**Transport (both):** HTTP/1.1 over a Unix socket via `CURLOPT_UNIX_SOCKET_PATH`, base URL `http://localhost` (`LF:Foundation/Cloud/Queue.php:365-374`; `SOC:Queue/Agent/AgentClient.php:139-149`). Laravel uses `Http::withoutGlobalConfiguration` so app-level client config cannot leak in (test `QueueTest.php:362-390`). Laravel's client defaults: `connect_timeout` 10, `http_errors` false, JSON body format (`LF:Http/Client/PendingRequest.php:260-273`). Symfony's Guzzle client has no connect timeout; the total `timeout` bounds it.

**`GET /next`**

| | Laravel | Symfony |
|---|---|---|
| Path / query / body | `/next`, no query, no body | same |
| Headers | No bespoke agent headers. `asJson()` selects JSON body format, not `Accept: application/json`; that requires the separate `acceptJson()` call, which the agent does not make (`PendingRequest.php:325-328, 432-445`). Guzzle supplies its defaults | Guzzle defaults |
| Timeout | 65 s total request timeout (`Queue.php:298`) | 65 s (`AgentClient.php:50`) |
| Retries | 3 attempts, sleeps 0 ms then 500 ms; retried on connection errors **and** HTTP 4xx/5xx responses (`Queue.php:299`; `PendingRequest.php:1075-1128`; `helpers.php:311-346`) | 1 attempt |
| 204 | `null` (`Queue.php:307-309`) | `null` (`AgentClient.php:61-63`) |
| Other non-OK | `AgentUnreachableException` (`:311-315`) | `TransportException` (`:65-67`) |
| Body | `$response->json()` (associative decode, `LF:Http/Client/Response.php:107-115`); non-array → `AgentUnreachableException` (`:317-321`) | `json_decode(..., true)`; non-array → `TransportException` (`:69-73`) |
| `messageId` | non-empty string required, else empty poll (`:264-266`) | same (`CloudQueueTransport.php:304-306`) |
| `receiptHandle` | string, else `null` (`:269`) | same (`:308`) |
| `body` | string, else `''` (`:277`) | same (`:309`) |
| `attributes` | `?? []`; `SqsJob::attempts()` indexes `ApproximateReceiveCount` without a fallback (`:278`; `SqsJob.php:132-135`). Numeric strings are cast; a missing key raises **`ErrorException`** under the console error handler (`LF:Foundation/Console/Kernel.php:120-127`; `LF:Foundation/Bootstrap/HandleExceptions.php:47-49, 71-77`) | `max(1, (int) (... ?? 1))` — missing → 1 (`:315, 359-362`) |
| `queueUrl` | `?? null`, stored as the `CloudJob` queue; **not** used for telemetry (`:281`; telemetry uses the pop argument, `:250, 542`) | string non-empty, else config `queueUrl`; used for telemetry normalization and direct-mode SQS calls (`:313`; `QueueEventSubscriber.php:261-267`) |

**`POST /result`**

| | Laravel (`Queue.php:332-358`) | Symfony (`AgentClient.php:94-137`) |
|---|---|---|
| Body | JSON object `{messageId, receiptHandle, status, delay}` through `array_filter(..., fn ($v) => $v !== null)`: `null` omitted, `0` kept | identical filter (`:96-101`) |
| `status` values used | `processed` (`CloudJob::delete`), `released` (`CloudJob::release`, with `delay`); no `failed` status, no `queueUrl` | `processed` (`ack`, poison decode), `released` (`release`) |
| Timeout | 10 s per attempt | 10 s per attempt |
| Retries | `retry(3, 100, when: ConnectionException)` → 3 attempts, 100 ms sleeps, connection errors only; `throw()` makes HTTP 4xx/5xx responses raise immediately; it does not reject returned 3xx (`Response.php:250-272, 352-362`) | loop: `ConnectException` → up to 3 attempts, `usleep(100_000)`; any other `GuzzleException` → `TransportException` without retry |
| 5xx | `AgentUnreachableException` (fatal, exit 0 via lost-connection path) | `TransportException` |
| 4xx | rethrown `RequestException` (non-fatal) | `RuntimeException` |
| 2xx | success (no body parsing) | success |

Returned 3xx and 201 responses are not retried; GET rejects any final response other than 200/204. Agent unavailability, including exhausted retries after a lost `/result` response, is `AgentUnavailableError` and exits 0 in the project. The exit-1 ambiguous-ack rule applies only to direct SQS/Redis brokers.

Differences: GET retry count; GET connect timeout; missing receive count (`ErrorException` under Laravel console bootstrapping vs Symfony 1); telemetry queue source; Laravel additionally retries HTTP 4xx/5xx GET responses.

### Q5. `failed_job` event fields

`LF:Foundation/Cloud/FailedJobProvider.php:57-92`, fed by `WorkCommand::logFailedJob()` (`LF:Queue/Console/WorkCommand.php:421-428`: `$event->job->getQueue()`, `$event->job->getRawBody()`, `$event->exception`).

| Field | Source |
|---|---|
| `_cloud_event` | `'failed_job'` |
| `id` | `Str::uuid7($timestamp)->toString()` (`:72`; `LF:Support/Str.php:2094-2099` → `Ramsey\Uuid\Uuid::uuid7($time)`), where `$timestamp = CarbonImmutable::now('UTC')` captured at the top of `log()` (`:67`) — the failure time, which is also the `failed` lifecycle timestamp (`:89`). |
| `queue` | `processingJobDetails()['queue']` = `processingQueue`, the normalized pop queue (`Queue.php:504-511, 542`). |
| `started_at` | `processingJobStartedAt->toDateTimeString('microsecond')` — set when the job was popped (`Queue.php:543`). |
| `attempts` | `$this->processingJob->attempts()` → `(int) ApproximateReceiveCount` (`SqsJob.php:132-135`). |
| `payload` | `$payload` = `getRawBody()` — the message body **as a string**, exactly as received (after overflow hydration when enabled). |
| `exception_preview` | `mb_substr(string, 0, 1001, 'UTF-8')` of `"{Class}: {message} in {file}:{line}"`, or `"{Class} in {file}:{line}"` when `getMessage()` is falsy (`''` **or** `'0'`) (`:77-84`). 1 001 **characters**, not bytes (test `QueueTest.php:712-733`). |
| `job_name` | `(json_decode($payload, associative: true) ?? [])['displayName'] ?? ''` (`:85`). |
| `exception` | `(string) $exception` — PHP `Throwable::__toString()`: `"{Class}: {message} in {file}:{line}\nStack trace:\n#0 ..."` plus any `previous` chain; invalid UTF-8 is substituted by the writer's `JSON_INVALID_UTF8_SUBSTITUTE` (test `:734-757`). |

Field order in the emitted object is the array order above. Not emitted in v1: the `retried_at` variant (`:220-225`).

### Q6. Lifecycle event details

`LF:Foundation/Cloud/Queue.php:473-497` (`finishProcessingJob`), `:518-526` (`finishQueueingJob`), `:535-551` (`startProcessingJob`), `:559-570` (`normalizeQueue`).

- `timestamp`: `CarbonImmutable::now('UTC')->toDateTimeString('microsecond')` → `Y-m-d H:i:s.u` (test `QueueTest.php:1649-1676`: `2000-01-01 16:04:05.060708`). The `failed` event reuses the `failed_job` timestamp; `started` reuses `processingJobStartedAt`.
- `duration_ms`: start = `processingJobStartedAt` (set in `startProcessingJob`, i.e. when `pop()` returned the job, before `fire()`); end = the `finishProcessingJob` timestamp (next `pop()`, `WorkerStopping`, or the `failed_job` timestamp); `(int) diffInMilliseconds` truncates toward zero; signed (no clamp). Test `:1631-1647`.
- Types carrying `duration_ms`: `processed`, `released`, `failed` (all through `finishProcessingJob`). `queued` and `started` carry none.
- `type` selection: `hasFailed()` → `failed`; `isReleased()` → `released`; else the `$default` (`processed`, or `released` from the `TimedOut`/fatal-error listeners in `QueueConnector.php:120-133`).
- `queue` normalization (`normalizeQueue`): `getQueue($queue)` (full URL via `SqsQueue::getQueue`) → `chopStart($prefix.'/')` when prefix is truthy → when suffix is truthy: ends with `.fifo` ? `chopEnd('.fifo')->chopEnd($suffix)->append('.fifo')` : `chopEnd($suffix)`. `chopStart`/`chopEnd` (`LF:Support/Str.php:270-297`) remove the needle once, only if present. Tests `:2083-2113`.

### Q7. `queued` event

- Emitted by `Queue::finishQueueingJob($queue)` (`Queue.php:518-526`): `_cloud_event`, `timestamp`, `type: queued`, `queue: normalizeQueue($queue)`; no `duration_ms`.
- Wired by `QueueConnector::configureQueue()` (`QueueConnector.php:91-96`) as a listener on `JobQueued` filtered by connection name; `JobQueued` is raised in `Queue::enqueueUsing()` **after** the push callback returns the message ID (`LF:Queue/Queue.php:385-389, 490-497`), i.e. after `SendMessage` succeeded; for `bulk()` once per `Successful` batch entry (`LF:Queue/SqsQueue.php:442-452`); for after-commit jobs after the transaction commits (`Queue.php:370-383`). `pushRaw()` on the Cloud wrapper emits directly after the underlying push (`Queue.php:182-189`).
- Queue name: the raw `$queue` argument of the push (may be `null` → `getQueue(null)` → default → normalized logical name).
- Symfony: `onSend` (`SOC:Queue/QueueEventSubscriber.php:92-111`) on `SendMessageToTransportsEvent` with the `CloudQueueStamp` queue or the config default; whether Messenger dispatches that event before or after the sender runs is not determinable from the pin.

### Q8. Timeout path

Order at the pin (`LF:Queue/Worker.php:319-356`, Cloud configuration `LF:Foundation/Cloud/QueueConnector.php:101-134`):

1. Before each job (after `getNextJob`, also when no job): `pcntl_alarm(max(timeoutForJob, 0))`; `timeoutForJob` = payload `timeout` if not `null`, else `--timeout` (default 60). `0` disables the alarm. `resetTimeoutHandler()` (`pcntl_alarm(0)`) runs after every loop iteration (`:295-297`).
2. On SIGALRM with a job:
   1. `markJobAsFailedIfWillExceedMaxAttempts(maxTries, TimeoutExceededException)` (`:729-740`): `retryUntil` expired → `fail()`; else `maxTries > 0 && attempts >= maxTries` → `fail()`.
   2. `markJobAsFailedIfWillExceedMaxExceptions` (`:750-766`, needs cache + `uuid` + `maxExceptions`).
   3. `markJobAsFailedIfItShouldFailOnTimeout` (`:791-796`, payload `failOnTimeout`).
   4. `fail()` runs at most once effectively (`Job.php:186-188`): ack `processed`/`DeleteMessage` → `JobFailed` → `failed_job` → `failed` lifecycle (with `FailedJobProvider::log` clearing `processingJob`).
   5. `JobTimedOut` event.
   6. `$killOnTimeout` is `true` → `kill(124, TimedOut)` (`:1057-1073`): dispatches `WorkerStopping` → Cloud listener `finishProcessingJob(default: 'released')` — emits `released` only if the job is still being tracked (i.e. not already failed); then `killUsing` callback `pcntl_exec('/bin/sh', ['-c', 'exit 124'])` replaces the process; if `pcntl_exec` is unavailable or fails, `posix_kill(SIGKILL)` then `exit(124)`.
3. The message is **not** released and no backoff is applied on the retryable path (no `release()` call in the handler). Redelivery relies on agent/SQS visibility expiry.
4. Exit code: `Worker::$timedOutExitCode = 124` (`QueueConnector.php:106`); generic Laravel would use `EXIT_ERROR = 1`.

Events: retryable timeout → `released` (via `WorkerStopping`); terminal timeout → `failed_job`, `failed` (no `released`). Test `QueueTest.php:1346-1383, 1462-1509`.

Project deviation `timeout-window-handler-only` (D13.2, follows project): arm only around handler execution and per-job teardown, after decoding/pre-run checks, and disarm before outcome reporting and `--rest`. Laravel arms before `process()` and resets after reporting/rest (`LF:Queue/Worker.php:268-296`), so its timeout can fire during those phases. Managed terminal timeouts complete → `failed_job` → `failed` → exit 124; self-managed `sqs`/`redis` writes its stdout failure record → completes → exits 124, with no socket events (D6b/D12).

### Q9. Queue URL rules

- `SqsQueue::getQueue($queue)` (`LF:Queue/SqsQueue.php:687-694`): `enum_value($queue) ?: $this->default` (empty string → default), `resolveQueue()` (queue routing), then `FILTER_VALIDATE_URL` → pass-through unchanged, else `suffixQueue()`.
- `suffixQueue()` (`:703-712`): `.fifo` → `rtrim(prefix, '/') . '/' . Str::finish(beforeLast(queue, '.fifo'), suffix) . '.fifo'`; else `rtrim(prefix, '/') . '/' . Str::finish(queue, suffix)`. `Str::finish` (`Str.php:496-501`) removes one or more trailing copies of the suffix then appends exactly one; with an empty suffix it is a no-op. Empty prefix yields a leading `/`.
- Normalization: inverse rules in `Queue::normalizeQueue` (Q6). Symfony's `normalizeQueue` (`SOC:Queue/ManagedQueueConfig.php:154-175`) is equivalent for ordinary names.
- FIFO default group: `MessageGroupId = $queue` where `$queue = resolveQueue(enum_value($queue) ?? $this->default)` (`SqsQueue.php:582, 610-612`) — the logical name including `.fifo` (note `??`, so `''` is not defaulted here).
- Dedup: `deduplicator` callable, `deduplicationId()` method, else `Str::orderedUuid()`; falsy values dropped by `array_filter` (`:619-635`).
- Delay on FIFO: `DelaySeconds` omitted (`:589-592`); on standard: `secondsUntil($delay)` (`InteractsWithTime.php:17-24`: `DateTimeInterface` → `max(0, ts - now)`, else `(int)`); `empty($delay)` (0) omits it. No cap.
- Fair queues: `MessageGroupId` from `messageGroup` on standard queues too (`:604-614`); no rejection of combinations.

### Q10. Cloud `Events` socket writer

`LF:Foundation/Cloud/Events.php` (Symfony `SOC:Observability/Events.php` has byte-identical `write()`/`format()`/`connect()`/`connected()` bodies; only visibility and `void` vs `bool` returns differ).

- Address: `CloudBootstrapper::socket()` (`LF:Foundation/CloudBootstrapper.php:324-329`): `$_ENV['LARAVEL_CLOUD_LOG_SOCKET'] ?? $_SERVER[...] ?? 'unix:///tmp/cloud-init.sock'`; singleton registered at `:316-319`.
- Connect (`:141-169`): `stream_socket_client(address, timeout: 2, flags: STREAM_CLIENT_CONNECT | STREAM_CLIENT_PERSISTENT)`; failure → `RuntimeException`; then `stream_set_timeout($socket, 2)` (read/write timeout), failure → disconnect + throw.
- EOF detection (`:174-187`): `connected()` requires a resource and `! feof($socket)`; EOF → `disconnect()` → `ensureConnected()` reconnects before each write (`:131-136`).
- Write loop (`:71-108`): `@fwrite` returning `false` → disconnect + throw; accumulate `$written`; return once `>=` length; count zero-length writes (any, not only consecutive); `>= 5` → disconnect + throw; otherwise `substr` the remainder and loop.
- Format (`:117-126`): `json_encode($line, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE)`, lines joined by `"\n"`, trailing `"\n"`.
- Failure policy (`:51-66`): `emitMany` catches `Throwable` and returns `false`; nothing propagates. No locking/serialization across processes (single-threaded PHP).

### Q11. Attempt checks

- Pre-run `markJobAsFailedIfAlreadyExceedsMaxAttempts` (`LF:Queue/Worker.php:701-718`): `maxTries = payload maxTries ?? (int) options.maxTries`; if `retryUntil` set and `now <= retryUntil` → run; if no `retryUntil` and (`maxTries === 0` or `attempts <= maxTries`) → run; else `fail(MaxAttemptsExceededException)` and throw.
- After exception `markJobAsFailedIfWillExceedMaxAttempts` (`:729-740`): `retryUntil <= now` → fail; `! retryUntil && maxTries > 0 && attempts >= maxTries` → fail.
- `maxTries` `null` (payload key absent/null) → worker default; `0` → unlimited attempts (deadline still applies); negative → pre-check fails immediately, post-check never fails (edge, not worth reproducing).
- Backoff (`:817-827`): payload `backoff` (a comma-joined string built by `Queue::getJobBackoff`, `LF:Queue/Queue.php:251-266`) or `--backoff`; `explode(',')`; index `attempts - 1`; past the end → last value; `(int)` cast.
- Defaults: `maxTries 1`, `backoff 0`, `timeout 60` (`WorkerOptions.php:107-134`).
- `release()` never touches the receive count; the next delivery's `ApproximateReceiveCount` is what increments `attempts`.

### Q12. Contradictions with `PROJECT_SCOPE.md` / `docs/decisions.md`

Evidence-based resolutions incorporated in D13 and the contract pack after R2 review. `PROJECT_SCOPE.md` wording noted here remains superseded where D13 says so.

1. **Acknowledgement order (§11, §12 vs Laravel, D1, §15).** Laravel acknowledges (`delete`/`processed`) before `failed_job` and `failed` (Q1). §11 "Terminal failure deletes after failure reporting" and §12 "emit the `failed_job` event; complete the message" / "emit `failed` and `failed_job` from the worker, then complete the message" reverse it. D1 ("Laravel deletes the message before the record is written") and §15 are correct. D13.1 resolves managed mode as complete → `failed_job` → `failed`; self-managed `sqs`/`redis` keeps D6b's stdout failure record → complete, with no socket events (D12).
2. **Deterministic defects terminal on first delivery (§8, §12, §24) is not Laravel behaviour.** Laravel applies the ordinary retry policy to malformed bodies and unknown classes (Q2). Symfony deletes on decode failure. Catalog deviation label `deterministic-defects-terminal` (follows project).
3. **Missing `ApproximateReceiveCount` → 1 (§11)** is the project/Symfony fallback. Laravel indexes the missing key without a fallback, raising `ErrorException` under its console `HandleExceptions` handler (Q4). The project parses numeric strings and defaults missing/invalid values to 1; Symfony casts and clamps. Catalog label `missing-receive-count-is-one` (follows symfony).
4. **Telemetry queue from agent `queueUrl` (§11)** is Symfony; Laravel normalizes the pop argument (Q4, Q6). Catalog label `telemetry-queue-from-queue-url` (follows symfony).
5. **Direct polling (§11, D13.5).** One SQS queue uses Symfony's `WaitTimeSeconds=20`, `MaxNumberOfMessages=1`; several queues use priority short polls (`WaitTimeSeconds=0`), then `--sleep` only if all are empty. No extra sleep follows an empty long poll. Redis blocks for at most `--sleep`, without an extra sleep after the wait. Laravel's `SqsQueue::pop` passes neither wait nor batch size (`SqsQueue.php:644-657`), and its worker always sleeps `--sleep` after an empty pop (`Worker.php:289-292`). Label `receive-long-poll-params` follows Symfony's single-queue parameters with these project polling/sleep choices.
6. **`GET /next` retry trigger (§11).** Laravel retries HTTP 4xx/5xx responses as well as connection errors (Q4); §11 limits retries to connection errors. D13.3 adopts Laravel's retry triggers. Returned 3xx/201 responses are not retried and GET accepts only 200/204.
7. **§6 "driver other than `cloud`, or a missing `connection` … configuration error. This follows Laravel's throwing decoder."** Laravel throws only on malformed JSON; a non-`cloud` driver silently disables managed queues and a missing `connection` is auto-created (row 17). The stricter shape check is a project decision; catalog label `managed-config-strict-shape` (follows project).
8. **§6 credentials.** Laravel rejects unknown named providers, accepts provider objects/callables, and uses explicit `key`/`secret` (including `token`) before the SDK default chain when `credentials` is absent (row 18). D13.4 permits only the strings `ecs`/`instance` in project managed mode; all other or missing values are configuration errors (label `credentials-explicit-only`, follows project).
9. **`processed`/`released` timing (§15).** Laravel emits the completion event at the next `pop()`/stop, so `duration_ms` includes `--rest` and post-job time; the project emits at outcome completion. D13.2 labels this `completion-event-immediate` (follows project); the duration endpoint intentionally excludes `--rest` and idle time before the next poll.
10. **D7 exit codes: "0 … agent unhealthy (matches Laravel)".** Confirmed, with the nuance that on the `GET /next` path Laravel sleeps 1 s before exiting (`Worker.php:501-507`).

The timeout-window difference is labeled `timeout-window-handler-only` (D13.2; Q8). D13.8 replaces §11's Redis timeout+margin expiry with now + lease (default 60 s), renewed every lease/3, allowing `timeout=0`; direct SQS receive sets `VisibilityTimeout=lease`. Laravel Redis uses `availableAt(retryAfter)` (`LF:Queue/RedisQueue.php:589-594`).

No further contradiction found for: D2 (process-level timeout mechanics), D4 (policy in payload; Laravel fills only `null` fields from worker defaults), D1 field set and order, §6 queue URL intent (the precise repeated-suffix behavior is in Q9/§4), §10 FIFO defaults, §14 no-release-on-timeout, §15 timestamp format and `duration_ms` carriers, §23 exit 124.

---

## 4. Nuances to encode in fixtures

- `array_filter($options)` drops **falsy** `MessageGroupId`/`MessageDeduplicationId`: `''` and `'0'` are omitted by Laravel (`SqsQueue.php:635`). The project validates IDs (1–128 chars) so `'0'` is a valid explicit ID; only `''` means "omit".
- `exception_preview` is 1 001 UTF-8 **characters**; a message of `'0'` is treated as empty.
- `failed_job.id` is a UUIDv7 derived from the failure timestamp, which is also the `failed` lifecycle timestamp.
- In managed mode, terminal timeout completes → `failed_job` → `failed` (no `released`); retryable timeout emits only `released`. In self-managed `sqs`/`redis`, terminal timeout writes the failure record to stdout → completes; retryable timeout writes an info line. Both modes exit 124 without releasing the message; self-managed modes send no socket events.
- `started` is emitted before the body is decoded; `job_name` is `''` when the body is not JSON.
- `queued` is emitted after `SendMessage` succeeds, with the logical (normalized) queue name; never for a failed send.
- Missing receive count: Laravel console execution raises `ErrorException` on the missing key; Symfony defaults/casts/clamps to 1; the project parses numeric strings and defaults missing/invalid values to 1.
- `Str::finish` collapses repeated suffixes; `Str::chopEnd` removes only one — `name-s-s` with suffix `-s` builds `.../name-s` and normalizes to `name`, not `name-s` or `name-s-s`. Conformance fixtures must include an already-suffixed name.
- GET `/next` in Laravel is retried on HTTP 4xx/5xx responses too (row 2).
- A Laravel multi-queue worker never uses the agent; the project rejects a conflicting CLI queue at startup instead (row 1, §11).

## 5. Not determinable from source

- Symfony Messenger worker behaviour after `TransportException`/`RuntimeException` (exit code, continuation) — Messenger is not in the pin.
- Whether `SendMessageToTransportsEvent` fires before or after the SQS send.
- Cloud agent semantics beyond the wire protocol (heartbeat cadence, redelivery after a worker exit); `SOC:Queue/Agent/AgentClient.php:13-20` is a comment.
- Log collector line limit (16 KiB) — `SOC:Queue/QueueEventSubscriber.php:39-67` is a comment about a platform component.
- SQS visibility ceiling measurement (AWS behaviour).
