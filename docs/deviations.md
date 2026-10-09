# Deviations from the Laravel baseline

Every place where this package intentionally behaves differently from Laravel's queue
worker or SQS driver at the pinned baseline (`laravel/framework` v13.33.0,
`91188a17ceaa3dbace6e8a5f7abd0d042e466359`). Each row is a labeled deviation in the
conformance catalog ([`contract/catalog.json`](contract/catalog.json)); the conformance
report carries the label next to the feature result instead of pretending full
compatibility. "Follows" says whose behavior we adopted: `laravel/symfony-on-cloud`
(commit `50c945170b6cb5690370d15fd725c6f82495e9ba`) or a project decision in
[`decisions.md`](decisions.md). Upstream line numbers are in
[`contract/upstream-evidence.md`](contract/upstream-evidence.md).

Behaviors not listed here match Laravel.

## Configuration

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `managed-config-strict-shape` | Throws only on malformed JSON in `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`; a `driver` other than `cloud` silently skips managed-queue boot; a missing `connection` is auto-created | Malformed JSON, `driver != "cloud"`, or a missing `connection` or `region` is a `ConfigurationError` at startup | A silently unconfigured worker on Laravel Cloud would poll nothing. Symfony's lenient parser (malformed JSON = unconfigured) is deliberately not copied either | project (D13.4). `CloudBootstrapper.php` 217-241, 264-280; `Cloud/Queue.php` 577-582; `Connectors/SqsConnector.php` 22-38 |
| `credentials-explicit-only` | Accepts `ecs`/`instance` strings, provider objects and callables; with no `credentials` key it uses explicit `key`/`secret`, then the SDK default chain | Only the strings `ecs` and `instance` are accepted in managed mode; a missing or other value is a `ConfigurationError`. The boto3 default chain is never consulted in managed mode; in `sqs` mode only with `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default` | Laravel Cloud object storage places R2 keys and an R2 endpoint in `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`; the default chain would send SQS calls to R2 | project (D13.4). `Connectors/SqsConnector.php` 61-113; Symfony `Sqs/SqsClientFactory.php` 20-36 |
| `agent-socket-env-fallback` | Reads only `agent.socket`, default `/tmp/cloud-agent.sock` | `agent.socket`, else `LARAVEL_CLOUD_AGENT_SOCKET`, else `/tmp/cloud-agent.sock`. The variable never overrides an explicit config value | Lets local emulators and future platform changes move the socket without editing the injected document | symfony. `Cloud/Queue.php` 365-374, 392-408; Symfony `ManagedQueueConfig.php` 48-92, 196-199 |

## Envelope and payload

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `registry-only-resolution` | The payload names a PHP class (`Illuminate\Queue\CallQueuedHandler@call`) that is resolved through the container | Job names resolve only through the registry; value types only through codecs chosen by the handler's annotations. Payloads never name a Python type, module or callable; nothing is imported because a message asked for it. Decoding is bounded in size and depth | Message bodies are untrusted input; Python has no safe equivalent of a container-resolved class name | project (§8, D4, D14). No upstream counterpart |
| `local-size-rejection` | Uses the 1 MiB constant for batch chunking and as the cache-backed overflow threshold; a single oversized send is forwarded and fails at SQS | The encoded body is measured in UTF-8 bytes before sending; over 1,048,576 bytes on SQS-backed modes raises `PayloadTooLargeError` and nothing is sent. Overflow (`@pointer`) is not implemented; a received pointer body fails deterministically | A stable typed error before a network call is more useful than an SQS rejection; overflow needs a cache store the Python app does not have | project (§8). `SqsQueue.php` 20-25, 537-568 |

## Dispatch options

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `fresh-delay-cap` | Forwards any `DelaySeconds` and lets SQS fail | Delays over 900 s, negative or non-finite raise `InvalidQueueOptionError` before sending, in every mode; positive fractions round **up** to whole seconds (Symfony floors). Retry backoff is not subject to this cap | Fail before the network call; a 0.2 s delay should not become immediate | symfony. `SqsQueue.php` 579-636; `Support/InteractsWithTime.php` 17-24; Symfony `CloudQueueTransport.php` 233-248 |
| `fifo-delay-rejected` | Silently omits `DelaySeconds` on FIFO queues | A positive delay on a `.fifo` queue raises `InvalidQueueOptionError` | Silently dropping a requested delay hides a bug | symfony. `SqsQueue.php` 579-636; Symfony `CloudQueueTransport.php` 209-216 |
| `fifo-fair-cross-model-rejected` | Applies one message group to both queue types and ignores deduplication IDs on standard queues without error | FIFO options (`group`, `deduplication_id`) on a standard queue, or a fair-queue `message_group` on a `.fifo` queue, raise `InvalidQueueOptionError`. In `redis` mode every FIFO and fair-queue option is rejected | FIFO ordering groups and fair-queue tenant keys are different models; silently ignored attributes hide mistakes | symfony. `SqsQueue.php` 579-636; Symfony `CloudQueueTransport.php` 193-222 |

## Agent protocol (managed mode)

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `agent-rejects-multiple-queues` | Uses the agent only when the popped queue equals the worker queue (`--queue`, else the config `queue`); a multi-queue worker pops SQS directly and never uses the agent | With `agent.enabled` the worker queue is `--queue`, else the config `queue`, and it receives only through `GET /next`. A `--queue` naming several queues is a startup `ConfigurationError` (exit 2), not a silent fallback | Falling back to direct SQS would bypass the agent's visibility heartbeat | project. `Cloud/Queue.php` 242-253, 392-408 |
| `telemetry-queue-from-queue-url` | Ignores the agent's `queueUrl` for telemetry and normalizes the `pop()` argument | Normalizes the received `queueUrl`, falling back to the configured queue when missing | Identical in practice for a single assigned queue; the delivered URL is the more direct evidence | symfony. `Cloud/Queue.php` 260-287, 294-324, 365-374; Symfony `AgentClient.php` 44-76, `CloudQueueTransport.php` 300-318 |
| `missing-receive-count-is-one` | `SqsJob::attempts` indexes `ApproximateReceiveCount` without a fallback; a missing key becomes an `ErrorException` under the console error handler | Numeric strings are parsed (`"3"` is attempt 3); a missing or invalid value counts as attempt 1 | A malformed attribute should not crash the worker or turn into attempt 0 | symfony (D13.5). `Jobs/SqsJob.php` 132-135; `Console/Kernel.php` 120-127; `Bootstrap/HandleExceptions.php` 41-77; Symfony `CloudQueueTransport.php` 323-362 |
| `outcome-event-after-ack-rejection` | After a `/result` 4xx on the last permitted attempt, a successful handler can be marked failed, emitting `failed` without `failed_job` | A 4xx is logged as `AgentProtocolError`; the worker does not assume acknowledgement, sends no second outcome, emits the lifecycle event for the outcome it actually chose and continues | One delivery, one outcome; the agent stays authoritative for that message | project (D13.5). `Cloud/Queue.php` 332-358; `Cloud/AgentAwareLostConnectionDetector.php` 27-31; `Queue/Worker.php` 558-586; `Jobs/Job.php` 182-188; Symfony `AgentClient.php` 94-137 |

## Direct SQS receive

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `receive-long-poll-params` | `SqsQueue::pop` passes only `QueueUrl` and `AttributeNames`; the worker always sleeps `--sleep` after an empty pop | Single queue: `WaitTimeSeconds=20`, `MaxNumberOfMessages=1`, `VisibilityTimeout=lease` (60 s), no extra sleep after an empty long poll. Several `--queue` entries: `WaitTimeSeconds=0` in priority order, then `--sleep` only when all are empty | Long polling cuts empty receives and cost; sleeping again after a 20 s wait doubles idle latency | symfony for the single-queue parameters; project for priority polling and sleep rules (D13.5, D13.8). `SqsQueue.php` 644-657; `Queue/Worker.php` 233-308, 465-508; Symfony `CloudQueueTransport.php` 323-362 |
| `visibility-renewal-watchdog` | No visibility renewal outside the Cloud agent; a job longer than the queue's visibility timeout is redelivered while still running | A watchdog thread renews the message's visibility (and the Redis reservation) every third of a 60 s lease while the job runs, including while a sync handler blocks the loop. A failed renewal is a lost lease: no success is reported and the worker exits 1 | Worker clusters have no agent heartbeat; without renewal, long jobs run concurrently | project (D6, D7, D13.8). `Jobs/SqsJob.php` 66-86; `SqsQueue.php` 644-657 |

## Retry, timeout and failure handling

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `retry-delay-rounds-up` | Casts backoff with `(int)`, truncating toward zero, and never clamps | Positive sub-second retry delays round up to 1 s (explicit 0 stays 0); visibility and delayed values are clamped to 43,200 s (SQS maximum) | A 0.5 s backoff should not become an immediate retry; a value over 12 h would be rejected by SQS | symfony. `Queue/Worker.php` 817-827; `WorkerOptions.php` 107-134; Symfony `CloudQueueTransport.php` 151-173 |
| `timeout-window-handler-only` | Arms the alarm before `process()` and resets it after outcome reporting and `--rest`, so a timeout can fire during decoding, reporting or rest | The timer is armed after decoding and pre-run checks, around the handler and per-job teardown only, and disarmed before outcome reporting and `--rest` | A timeout during acknowledgement would produce ambiguous outcomes; the window measures the job, not the worker | project (D2, D13.2). `Queue/Worker.php` 268-296, 319-378, 729-740, 791-796, 1057-1073; `Jobs/Job.php` 182-225, 294-347; `Cloud/QueueConnector.php` 101-134; `Cloud/Queue.php` 473-497 |
| `deterministic-defects-terminal` | A malformed body, unknown class or `TypeError` during the handler is an ordinary exception and is retried when `tries > 1`; `@pointer` bodies are hydrated from the cache when overflow is enabled | Malformed envelopes, unsupported versions, unknown job names, codec failures, argument mismatches and `@pointer` bodies are terminal on **first** delivery, keeping the transport identity captured before decoding. Managed mode still emits `failed_job` then `failed` (Symfony deletes silently) | Retrying a deterministic defect burns attempts and delays the failure record without any chance of success | project (D13.5). `Jobs/Job.php` 96-103; `Queue/Worker.php` 648-687; `Jobs/SqsJob.php` 152-189; `Cloud/FailedJobProvider.php` 57-92; Symfony `CloudQueueTransport.php` 364-391 |

## Observability

| Label | Laravel | This package | Why | Follows / source |
|---|---|---|---|---|
| `completion-event-immediate` | Emits the completion event lazily at the next `pop`, on `WorkerStopping`, or right after `failed_job`, so `duration_ms` includes `--rest` and idle time before the next poll; `duration_ms` is a signed truncation | `processed`/`released`/`failed` are emitted immediately after the outcome is reported; `duration_ms` is truncated like Laravel but never negative (Symfony clamps). `failed_job` still precedes `failed` with the same timestamp | Durations should measure the job; a negative duration is a clock artifact | project (D13.2). `Cloud/Queue.php` 473-497, 535-551; `Cloud/QueueConnector.php` 101-134; `Queue/Worker.php` 268-296; Symfony `QueueEventSubscriber.php` 92-126, 245-290 |
| `failed-job-size-policy` | Sends the full payload and exception with no size handling (the collector drops lines over 16 KiB) | If the encoded line fits 16 KiB it is sent whole; otherwise `exception` is trimmed (head plus marker), then `payload`, adding `"replayable": false`. Symfony's payload projection (`uuid`/`displayName`/truncated body) is never applied | A dropped line loses the failure entirely; projecting the payload would discard the Python envelope's arguments even for small messages | project (D1). `Cloud/FailedJobProvider.php` 57-92; Symfony `QueueEventSubscriber.php` 39-69, 201-235 |

## Mode-specific behavior that is not a catalog deviation

These follow from project decisions rather than from a Laravel behavior being changed, and
are documented in the README:

- In `sqs` and `redis` modes no lifecycle or `failed_job` events are sent to the log
  socket (D12: Laravel Cloud ingests them for managed queues only). In every mode the worker
  logs a structured failure record **before** completing the message (D6b); managed mode
  then emits `failed_job` and `failed` after completion (Laravel order).
- Retry policy travels in the message (D4); Laravel reads it from the worker command and
  job class at run time.
- `JobContext.release()` always releases (Laravel parity); when attempts are exhausted the
  next delivery fails the pre-run check (D13.9).
- Explicit `PayloadTooLargeError` in `redis` mode above the 16 MiB decode ceiling (D14.2).
- Redis queue names ending in `:delayed`, `:reserved` or `:notify` raise
  `ConfigurationError` before any Redis command. Laravel allows them, but their pending key
  aliases another queue's internal key and corrupts that queue.
- Maximum job timeout of 604,800 s (D14.1); larger values would overflow `setitimer`.
