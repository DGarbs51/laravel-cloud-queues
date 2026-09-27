# Cross-verification by Claude Opus 5.5

Scope: claims raised by only one or two auditors (the ones not already confirmed during Claude's own source read). Checked against `/tmp/lf` (laravel/framework v13.33.0) and `/tmp/soc` (symfony-on-cloud @ 50c9451).

Legend: **CONFIRMED** (source matches), **WRONG** (source contradicts), **PARTIAL** (true with a caveat), **EXTERNAL** (depends on AWS/platform behavior not in these repos).

## Grok 4.7

| Claim | Verdict | Evidence |
|---|---|---|
| Lost agent connection stops worker with exit 0 | CONFIRMED | `Queue/Worker.php:422` `lostConnection => [EXIT_SUCCESS, ...]`; set at `Worker.php:581-585` |
| `ecs` → ECS provider, `instance` → instance profile, other strings throw | CONFIRMED | `Queue/Connectors/SqsConnector.php` `resolveCredentialProvider` `match` with `InvalidArgumentException` |
| SQS client HTTP and connect timeouts are 60s, `version` `latest` | CONFIRMED | `SqsConnector.php` `getDefaultConfiguration` |
| Laravel does not default region; Symfony defaults `us-east-1` | CONFIRMED | no region in `getDefaultConfiguration`; `soc/src/Queue/Sqs/SqsClientFactory.php` `?? 'us-east-1'` |
| Empty prefix yields a leading-slash queue name | CONFIRMED | `Queue/SqsQueue.php` `suffixQueue`: `rtrim($this->prefix, '/').'/'...` |
| `fail()` deletes, so terminal failure reports `processed` to the agent | CONFIRMED | `Queue/Jobs/Job.php` `fail()` calls `$this->delete()`; `Foundation/Cloud/CloudJob.php` `delete()` reports `processed` |
| Pre-run check fails job if `attempts > maxTries`; `maxTries === 0` is unlimited | CONFIRMED | `Worker.php` `process()` → `markJobAsFailedIfAlreadyExceedsMaxAttempts` (`:701-717`) |
| Post-error fails if `attempts >= maxTries` and `maxTries > 0` | CONFIRMED | `Worker.php:729-739` |
| Backoff indexed `attempts - 1`, last value repeats, `(int)` floors | CONFIRMED | `Worker.php:817-826` |
| Payload carries `maxTries`, `maxExceptions`, `failOnTimeout`, `backoff`, `timeout`, `retryUntil` (policy snapshot) | CONFIRMED | `Queue/Queue.php` `createObjectPayload` (`:174-190`) |
| Multiple queues comma-separated, first hit wins; pop error sleeps 1s | CONFIRMED | `Worker.php` `getNextJob` (`:481-507`) |
| Symfony default is 3 retries (4 deliveries) | CONFIRMED | `soc/README.md:183-189`; `soc/tests/CloudRetryIntegrationTest.php` asserts 4 SQS commands |
| Symfony agent-on receive ignores requested queue names | CONFIRMED | `soc/src/Queue/Messenger/CloudQueueTransport.php` `getFromQueues`: `if ($this->useAgent) return $this->getFromAgent();` |
| Symfony transport class comment says agent chosen "when its socket is present" (stale) | CONFIRMED | `CloudQueueTransport.php:24-26` vs `ManagedQueueConfig::agentAvailable` |

## Codex (GPT-6-Astra)

| Claim | Verdict | Evidence |
|---|---|---|
| Laravel emits `failed_job` before lifecycle `failed`; Symfony emits lifecycle `failed` first | CONFIRMED | `Foundation/Cloud/FailedJobProvider.php` `log()` emits then calls `finishProcessingJob`; `soc/src/Queue/QueueEventSubscriber.php` `onFailed` emits queue event then `failed_job` |
| `Str::finish` prevents double suffix; full URL passes through | CONFIRMED | `SqsQueue.php` `getQueue` (`FILTER_VALIDATE_URL`) and `suffixQueue` |
| Symfony 4xx surfaces as `RuntimeException`, tested with 409 | CONFIRMED | `soc/tests/AgentClientTest.php` `testReportSurfacesClientErrorAsRuntimeException` uses `new Response(409)` |
| `agent-server.php` fixture is a canned stub (fixed 200 body, no state/validation) | CONFIRMED | `soc/tests/Fixtures/agent-server.php` returns the same `integration-message` for every `GET /next` and empty 200 otherwise |
| Laravel supports explicit key/secret/token and provider options | CONFIRMED | `SqsConnector.php` `withCredentials` |
| SQS visibility maximum is measured from original receive time | EXTERNAL | AWS `ChangeMessageVisibility` API behavior; not provable from these repos |
| Python signal handler delayed by native code (20 ms requested, ~202 ms observed) | PARTIAL | Documented CPython behavior (handlers run between bytecodes); Codex's timing is its own experiment, not re-run here |
| Python 3.10 stdlib lacks `uuid.uuid7` | CONFIRMED | `uuid.uuid7` added in Python 3.14 |

## DeepSeek V4 Pro

| Claim | Verdict | Evidence |
|---|---|---|
| SQS limit is 256 KiB; Laravel's 1 MiB constant is overflow-only | WRONG | `SqsQueue.php:21-25` documents `MAX_SQS_PAYLOAD_SIZE = 1048576` as "maximum SQS payload size (1 MB)"; AWS raised the SendMessage limit to 1 MiB |
| Laravel quits on SIGQUIT/SIGTERM/SIGINT; SIGUSR2 pauses, SIGCONT resumes | CONFIRMED | `Worker.php:950-974` |
| Fatal PHP error emits `released` for the in-flight job | CONFIRMED | `Foundation/Cloud/QueueConnector.php` `register_shutdown_function` block |
| Worker defaults timeout 60, sleep 3, memory 128, backoff 0, rest 0, maxTries 1 | CONFIRMED | `Queue/WorkerOptions.php` constructor |
| Laravel `duration_ms` truncates; Symfony rounds and clamps at 0 | CONFIRMED | `Foundation/Cloud/Queue.php` `(int) diffInMilliseconds`; `QueueEventSubscriber::durationMs` `max(0, round(...))` |

## Gemini 3.1 Pro

| Claim | Verdict | Evidence |
|---|---|---|
| File:line citations (`CloudBootstrapper.php:1558`, `Events.php:1229`, `SqsQueue.php:2362`) | WRONG | files are 330, 234, 723 lines |
| `credential_cache` keys `enabled`, `cache`, `store` | WRONG | actual keys `enabled`, `store`, `fallback_store` (`Foundation/CloudBootstrapper.php:234-238`) |
| Build prompt omits exit code 124 | WRONG | `AGENT_BUILD_PROMPT.md` Timeout section: "Laravel baseline uses 124" |
| 4xx triggers terminal transport exceptions | WRONG | 4xx is non-fatal in both upstreams; only connection failure and 5xx are fatal |
| Blocking sync handler "deadlocks" the event loop | PARTIAL | It blocks, not deadlocks; the timeout/signal concern is valid and matches Codex S-C1 |
| Persistent socket with EOF check and reconnect | CONFIRMED | `Foundation/Cloud/Events.php` `connect`/`connected` |
| Suffix placed before `.fifo` via `Str::finish` | CONFIRMED | `SqsQueue.php` `suffixQueue` |

## Conclusion

No finding in the merged `README.md` depends on a WRONG claim. The two EXTERNAL/PARTIAL items (visibility window from receive; signal latency) are consistent with AWS and CPython documentation, but should be proved by tests rather than cited as upstream behavior.
