# Audit: `PROJECT_SCOPE.md` and `AGENT_BUILD_PROMPT.md` vs pinned upstream

Read-only. No repo files were modified.

Pinned sources checked:

- `laravel/framework` **v13.33.0** (`91188a17ceaa3dbace6e8a5f7abd0d042e466359`) at `/tmp/lf`
- `laravel/symfony-on-cloud` **50c945170b6cb5690370d15fd725c6f82495e9ba** at `/tmp/soc`

`git ls-remote --tags` for `v13.*` on `laravel/framework` still ends at `v13.33.0`, so scope §2’s “latest 13.x tag” claim holds as of this audit.

Line numbers below are from those trees. Scope and prompt citations use their section numbers.

---

## What the scope already gets right

These are easy to “fix” into a contradiction and should stay:

- Agent use is the injected `agent.enabled` flag, not “socket file exists”. Code agrees: `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:392-397`, `/tmp/soc/src/Queue/ManagedQueueConfig.php:187-198`. Stale comments in `/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:24-26` and `/tmp/soc/src/Queue/QueueFeature.php:26-28` still say “when the socket is present”. Do not follow those comments.
- FIFO URL shape `{prefix}/{base}{suffix}.fifo` matches `/tmp/lf/src/Illuminate/Queue/SqsQueue.php:705-708` and `/tmp/soc/src/Queue/ManagedQueueConfig.php:136-139`.
- Agent `GET /next` timeout **65s**, socket default `/tmp/cloud-agent.sock`: `Queue.php:298`, `Queue.php:370`.
- Log socket `LARAVEL_CLOUD_LOG_SOCKET` or `unix:///tmp/cloud-init.sock`: `/tmp/lf/src/Illuminate/Foundation/CloudBootstrapper.php:324-328`.
- Timeout exit **124**: `/tmp/lf/src/Illuminate/Foundation/Cloud/QueueConnector.php:107`.
- Worker default `maxTries = 1`: `/tmp/lf/src/Illuminate/Queue/WorkerOptions.php:114`.
- Attempt count is `ApproximateReceiveCount`: `/tmp/lf/src/Illuminate/Queue/Jobs/SqsJob.php:132-134`, `/tmp/lf/src/Illuminate/Queue/SqsQueue.php:648`.
- Sub-second retry delays round **up**, explicit 0 stays 0, clamp **43200**: this is Symfony, not Laravel. `/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:156-170`. Laravel floors with `(int)` at `/tmp/lf/src/Illuminate/Queue/Worker.php:826` and does not clamp in `/tmp/lf/src/Illuminate/Queue/Jobs/SqsJob.php:79-85`. Scope §12 correctly picks the Symfony rule. Keep that labeled as a deviation from the canonical framework.

---

# Part 1 — `PROJECT_SCOPE.md` vs upstream

## Critical

### 1. Failed-job and lifecycle schemas are not actually specified, and the two upstreams disagree

Scope §15 lists “concepts” (event type, UUIDv7, queue, started timestamp, attempts, payload, job name, exception preview, exception detail) and tells the implementer to inspect both trees and “choose the behavior that maximizes dashboard compatibility” if a log-line workaround exists. It never freezes the wire schema. Dashboard parity is a v1 blocker (§15, §30.12), so this is not optional detail.

Laravel emits a full event:

```70:87:/tmp/lf/src/Illuminate/Foundation/Cloud/FailedJobProvider.php
$this->events->emit([
    '_cloud_event' => 'failed_job',
    'id' => $id = Str::uuid7($timestamp)->toString(),
    'queue' => $processingJobDetails['queue'],
    'started_at' => $processingJobDetails['started_at']->toDateTimeString('microsecond'),
    'attempts' => $processingJobDetails['attempts'],
    'payload' => $payload,
    'exception_preview' => mb_substr( /* class: message in file:line, or class in file:line */ , length: 1001),
    'job_name' => (json_decode($payload) ?? [])['displayName'] ?? '',
    'exception' => (string) $exception,
]);
```

`id` is a UUIDv7 derived from that same failure timestamp (`FailedJobProvider.php:72`). `queue` is the normalized processing-job queue, not the `log()` argument (`FailedJobProvider.php:73`, `Queue.php:504-510`). `job_name` is the payload key `displayName` (`FailedJobProvider.php:85`). Preview length is **1001** UTF-8 characters (`FailedJobProvider.php:77-84`). The same timestamp is then used for the queue lifecycle `failed` event (`FailedJobProvider.php:89`, `Queue.php:473-491`).

Symfony’s event is a different contract, on purpose, because of the log pipeline:

- Caps: `MAX_EXCEPTION_BYTES = 4000`, `MAX_PAYLOAD_BODY_BYTES = 2000`, because containerd splits stdout at **16 KiB** and the collector does not reassemble CRI partials, so a fat line never reaches the `failed-job-events` topic. `/tmp/soc/src/Queue/QueueEventSubscriber.php:39-69`.
- Emitted fields are `_cloud_event`, `id` (`Uuid::v7()` with no shared timestamp), `queue`, `started_at` (`Y-m-d H:i:s.u`), `attempts`, trimmed `payload`, truncated `exception`. There is **no** `job_name` and **no** `exception_preview`. `/tmp/soc/src/Queue/QueueEventSubscriber.php:174-188`.
- Payload trim keeps `uuid`, `displayName`, a cut `body`, and `body_truncated`. `/tmp/soc/src/Queue/QueueEventSubscriber.php:201-219`.

Lifecycle events (both trees) are `_cloud_event: "queue"` plus `timestamp`, `type`, `queue`. `duration_ms` is only on `processed` / `released` / `failed`, not on `queued` or `started`.

- Laravel: `Queue.php:481-491` (finish), `Queue.php:518-526` (`queued`, no duration), `Queue.php:545-550` (`started`, no duration). Timestamp is Carbon `toDateTimeString('microsecond')` in UTC (`Queue.php:483`), which is `Y-m-d H:i:s.u`.
- Symfony: `QueueEventSubscriber.php:245-253` and `178`. Duration uses `round`, Laravel casts `diffInMilliseconds` to `int` (`Queue.php:490` vs `QueueEventSubscriber.php:276-282`).

Copying Laravel’s untrimmed `payload` + `exception` is what Symfony says the dashboard drops. Copying Symfony drops `job_name` and `exception_preview`, which is what Laravel’s provider sends. Scope §15 does not pick a winner, does not name `_cloud_event`, does not give the timestamp pattern, and does not give the 16 KiB / 4000 / 2000 budgets.

**Fix:** Freeze one schema in §15. Recommended: Laravel’s field set (`_cloud_event`, `id` UUIDv7 bound to the failure timestamp, `queue`, `started_at`, `attempts`, `payload`, `exception_preview`, `job_name`, `exception`) plus Symfony’s size budget so the line stays under 16 KiB. State timestamp as UTC `Y-m-d H:i:s.u` with no timezone suffix. State `duration_ms` as a non-negative integer present only on `processed`, `released`, and `failed`. Record this as an intentional deviation from raw Laravel `FailedJobProvider` (full strings) and from Symfony (missing `job_name` / `exception_preview`).

### 2. A 200 from `GET /next` with no usable `messageId` is an empty poll, not a fatal protocol error

Scope §11: “HTTP 200 must contain a valid structured message” and “unexpected status, malformed response, or unreachable agent is a transport-fatal condition.”

Both implementations return “no job” when `messageId` is missing or empty:

- `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:264-266`
- `/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:304-306`

What is actually fatal on receive:

- connection failure (`Queue.php:301-304`, `/tmp/soc/src/Queue/Agent/AgentClient.php:53-56`)
- status other than 204 or 200 (`Queue.php:307-314`, `AgentClient.php:61-66`)
- body that is not a JSON array (`Queue.php:317-320`, `AgentClient.php:69-72`)

Also coerced, not fatal: non-string `receiptHandle` becomes null (`Queue.php:268-269`, `CloudQueueTransport.php:308`); non-string `body` becomes `""` (`Queue.php:277`, `CloudQueueTransport.php:309`). Symfony then falls back to the configured queue URL if `queueUrl` is missing or empty (`CloudQueueTransport.php:313`). Laravel passes `$data['queueUrl'] ?? null` straight through (`Queue.php:281`).

**Fix:** Rewrite §11. Fatal: unreachable socket, non-204/200, non-array body. Empty/missing `messageId`: treat as no work. Document handle/body coercion. Decide and state the `queueUrl` fallback (Symfony’s fallback is the one that still has a queue to ack against).

### 3. `POST /result` 4xx does not kill the worker; 5xx and connection loss do. Lost-connection exit status is 0

Scope §11 and §12 say that if a processed/released result cannot be reported, the worker must stop, must not fetch another job, and must not assume ack. That matches connection loss and HTTP 5xx only.

`POST /result` body is `messageId`, `receiptHandle`, `status` (`processed` or `released`), optional `delay`. Null fields are omitted. `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:339-344`, `/tmp/soc/src/Queue/Agent/AgentClient.php:96-101`. Delay `0` is kept (filter is `!== null`). Symfony test: null handle is omitted (`/tmp/soc/tests/AgentClientTest.php:87-95`).

Failure split:

- Connect errors are retried, then fatal. Laravel call site: `retry(3, 100, … ConnectionException)` and `timeout(10)` (`Queue.php:336-338`). Symfony: up to 3 attempts, 100ms apart, then `TransportException` (`AgentClient.php:103-121`).
- HTTP **5xx** is immediately fatal (`Queue.php:349-353`, `AgentClient.php:127-128`). It is not retried.
- HTTP **4xx** is **not** `AgentUnreachableException`. Laravel rethrows `RequestException` (`Queue.php:349-356`). Symfony throws `RuntimeException` and the class doc says the consumer reports it and moves on (`AgentClient.php:86-92` and `131-132`; test at `AgentClientTest.php:119-124`).

Laravel only stops the worker for `AgentUnreachableException`:

- `/tmp/lf/src/Illuminate/Foundation/Cloud/AgentAwareLostConnectionDetector.php:28-30`
- `/tmp/lf/src/Illuminate/Queue/Worker.php:581-585` sets `lostConnection`
- `/tmp/lf/src/Illuminate/Queue/Worker.php:422` then stops with **`EXIT_SUCCESS` (0)**, not a crash status

`GET /next` is also retried in Laravel (`Queue.php:299`, `retry([0, 500], throw: false)`) and is **not** retried in Symfony (`AgentClient.php:45-72`). Scope mentions neither the GET retry nor the 4xx-vs-5xx split. §2 says the framework is canonical, which means: 4xx does not stop the worker, and agent loss exits 0.

**Fix:** Replace the blanket “any report failure is fatal” rule. Fatal (exit 0, same as Laravel lost-connection, do not pop again): connection exhaustion and HTTP 5xx. 4xx: surface a typed protocol error, do not assume ack, and state whether the worker continues (upstream) or stops (stricter product choice). If you keep “all failures fatal”, label it as a deviation from both `Queue.php:349-356` and `AgentClient.php:131-132`. State GET retry policy explicitly (Laravel retries; Symfony does not).

### 4. A timed-out retryable job is not released through the agent or `ChangeMessageVisibility`

Scope §14: by default a timed-out retryable job “should be released”. Elsewhere, release means `POST /result` `released` or `ChangeMessageVisibility` on the same message (§11, §12).

Laravel’s alarm handler does not call `release()`:

- It may `fail()` the job if this attempt already meets `maxTries` or `failOnTimeout` (`Worker.php:326-336`, `Worker.php:791-795`).
- Then it kills the process with `timedOutExitCode` (`Worker.php:347-350`), which Cloud sets to **124** (`QueueConnector.php:107`).
- The `WorkerStopping` listener emits lifecycle type `released` only as the **default label** when the job was not marked failed and was not actually released (`QueueConnector.php:120-123`, `Queue.php:484-487`).

`CloudJob::release()` is the method that reports `released` (`/tmp/lf/src/Illuminate/Foundation/Cloud/CloudJob.php:55-60`). The timeout path never calls it. The agent keeps heartbeating the message until it gets `/result` (`AgentClient.php:16-20`). A dead worker that never posts `/result` does not apply the job backoff; redelivery waits on agent/SQS visibility, and `ApproximateReceiveCount` increments on the next delivery.

`fail()` deletes. `CloudJob::delete()` reports **`processed`** (SQS delete), not a distinct failed status (`CloudJob.php:38-46`, `Job.php` `fail()` calls `delete()` at `/tmp/lf/src/Illuminate/Queue/Jobs/Job.php:217`). The dashboard “failed” signal is the observability event, after `hasFailed()` is set (`Queue.php:485`).

There is a second “released” emit on fatal PHP errors, after freeing a 32 KiB reserve (`QueueConnector.php:124-132`). Scope does not mention it.

**Fix:** Specify timeout as three observable facts, matching Cloud unless you deliberately deviate:

1. Process exits **124**.
2. Lifecycle event type is `released` unless the job was failed (`fail_on_timeout` or attempts already exhausted), in which case type is `failed` and the agent/SQS outcome is **delete / `processed`**.
3. Default timeout does **not** send `POST /result` `released` and does **not** apply backoff. Say that explicitly so an implementer does not “fix” it by releasing with the backoff list.

Also state the default worker timeout: **60 seconds** (`WorkerOptions.php:113`), overridable per job (`Worker.php:375-377`).

### 5. “Agent enabled” is not the whole receive rule, and §2 fights §11/§13

Scope §11: if the agent flag is enabled, receive through the agent. Scope §13: Cloud queue assignment is authoritative and a conflicting CLI queue must not override it. Scope §2: framework behavior is canonical; Symfony is only the adaptation precedent.

Laravel receive uses the agent only when all of these hold (`Queue.php:392-407`):

- `config['agent']['enabled']`
- the process is running `queue:work`
- `getQueue($popped)` equals `getQueue(--queue or config['queue'] or 'default')`

Otherwise it pops **SQS directly**, even in Cloud. `--queue` wins over `config['queue']` (`Queue.php:404-407`).

Symfony does the opposite on receive: if the agent flag is on, `getFromQueues()` ignores the requested names and always calls the agent (`CloudQueueTransport.php:67-70`). The agent is pinned to one queue (`CloudQueueTransport.php:58-61`).

Laravel also turns off worker restart and pause (`QueueConnector.php:103-104`). Scope never says Cloud workers must not honor an external restart/pause signal.

**Fix:** Pick the Symfony receive rule for the Python worker (one process, agent on means agent only, CLI queue cannot override `config["queue"]`) and mark it as a deviation from `Queue.php:392-407`. State that dispatch still always uses SQS (`CloudQueueTransport.php:116-117` class behavior; `Queue.php:182-188` `pushRaw` goes to the inner SQS queue). State that `config["queue"]` is the worker assignment and `config["queues"]` is the inventory used for size totals (`Queue.php:577-581`), not the worker’s poll list.

---

## High

### 6. Injected config is larger than the illustrative JSON, and cache overflow is already in this baseline

Scope §6’s sample omits keys the bootstrapper writes before the connector runs (`CloudBootstrapper.php:223-238`):

- `connection.after_commit` from `CLOUD_QUEUE_AFTER_COMMIT` (default false)
- `connection.overflow`: `enabled`, `store`, `always`, `delete_after_processing` (`CLOUD_QUEUE_OVERFLOW_*`, default enabled **false**, delete after processing **true**)
- `connection.credential_cache`: `enabled`, `store`, `fallback_store` (default fallback `file`)

`queues` may be a list **or** a map. A map contributes its keys, not its values (`Queue.php:577-581`). The sample only shows `"queues": []`.

Overflow is implemented, not hypothetical. Over the limit (or `always`), Laravel stores the body in cache and sends `{"@pointer":"laravel:sqs-payloads:{uuid}"}` (`SqsQueue.php:25`, `SqsQueue.php:537-567`). Receive hydrates that pointer when overflow is enabled (`SqsJob.php:158-188`). The agent path passes the same overflow config into `CloudJob` (`Queue.php:285`).

Scope §8 and §28 ban compression and S3 offload. They do not mention this cache pointer. An implementer told to match `SqsQueue.php` will build it. An implementer who only reads §28 will ignore `CLOUD_QUEUE_OVERFLOW_ENABLED=true` and then either reject or mis-decode `@pointer` bodies.

**Fix:** Add the real key set to §6. State v1 ignores `after_commit` and `overflow` even if present, does not write `@pointer`, and treats an incoming `@pointer` body as a terminal decode failure. Point the S3/compression roadmap at `SqsQueue::overflow` as the thing to revisit.

### 7. `credentials: "ecs"` is a provider mode, not a hint to use boto3’s default chain

Illustrative JSON shows `"credentials": "ecs"` (§6) and then tells the reader not to trust the illustration. The actual rule:

- Laravel maps the string `ecs` to `CredentialProvider::ecsCredentials()` and `instance` to instance profile. Any other string throws `Invalid credential provider` (`/tmp/lf/src/Illuminate/Queue/Connectors/SqsConnector.php:89-106`).
- If `credential_cache.enabled`, that provider is memoized through a cross-process cache so the Pod Identity agent is not rate-limited (`SqsConnector.php:109-114`, `CloudBootstrapper.php:244-258`).
- Symfony only special-cases `ecs`. Anything else, including its own fallback string `"default"` (`ManagedQueueConfig.php:85`), uses the SDK default chain (`/tmp/soc/src/Queue/Sqs/SqsClientFactory.php:27-32`). Missing region becomes `us-east-1` (`SqsClientFactory.php:24`). Laravel does not default the region.
- SQS client HTTP timeout and connect timeout are **60s** (`SqsConnector.php:47-53`).

A boto3 default chain will also pick up `AWS_ACCESS_KEY_ID` and shared profiles. That is not `credentials: "ecs"`.

**Fix:** In Cloud mode, `credentials == "ecs"` means the ECS container credential provider only. Unknown provider strings are configuration errors (follow Laravel, not Symfony’s fallthrough). Do not enable a second credential source. Document `credential_cache` as a known key; either implement a small cross-process cache or explicitly defer it and record the Pod Identity rate-limit risk. Local/direct mode is the only place key/secret/token or the default chain is allowed (§6 already allows local overrides; it should say they must not apply when the Cloud JSON is present).

### 8. There is no local/direct config schema, and “no prefix” does not mean the same thing in the two trees

Scope §6 says local overrides are allowed and does not define them. Symfony: empty prefix means “not configured” and send/receive throw (`ManagedQueueConfig.php:130-133`, `CloudQueueTransport.php:449-454`). Laravel still concatenates `rtrim(prefix,'/') + '/' + name`, so an empty prefix becomes a leading-slash name (`SqsQueue.php:703-711`).

Direct receive is also different:

- Laravel `pop` requests only `ApproximateReceiveCount`. No `WaitTimeSeconds`, no max-messages (`SqsQueue.php:646-649`). Empty polls then sleep **3s** (`WorkerOptions.php:115`, sleep use in `Worker.php:405`). A pop exception sleeps **1s** and continues unless it is a lost connection (`Worker.php:501-507`).
- Symfony direct receive uses `MaxNumberOfMessages: 1`, `WaitTimeSeconds: 20`, `MessageAttributeNames: All`, and `ApproximateReceiveCount` (`CloudQueueTransport.php:327-333`). Missing receive count becomes **1** (`CloudQueueTransport.php:359-361`). Laravel casts the attribute to int with no default (`SqsJob.php:134`), so a missing attribute is **0**, and the max-tries check treats 0 as “not over the limit” (`Worker.php:711`).

Scope §11 says “use long polling” and never picks 20 vs “queue attribute only” vs sleep 3.

**Fix:** Specify the local config: region, endpoint URL (LocalStack), keys or default chain, prefix, suffix, queue name. Require a prefix or an explicit queue URL in direct mode; do not emit `/{name}`. For direct receive, follow Symfony: wait 20s, one message, request `ApproximateReceiveCount`, default a missing count to 1. State SQS client timeouts as 60s.

### 9. Retry math is underspecified, and Symfony’s documented default is not Laravel’s

Scope §12 says default tries is 1, which matches `WorkerOptions.php:114`. It does not say:

- `maxTries === 0` means **unlimited** (`Worker.php:711`, `Worker.php:737` only fails when `maxTries > 0`).
- Before the handler runs, if there is no `retryUntil` and `attempts > maxTries`, the job fails immediately (`Worker.php:701-717`).
- On exception, if `attempts >= maxTries`, it fails instead of releasing (`Worker.php:729-738`).
- Backoff is a comma list. Index is `attempts - 1`. Past the end, the **last** value is reused. Then `(int)` floors (`Worker.php:817-826`). Default worker backoff is **0** (`WorkerOptions.php:109`), i.e. immediate visibility.
- The payload also carries `maxExceptions` and `retryUntil` (`Queue.php:181-185`, `Job.php:304-346`). `maxExceptions` is counted in cache (`Worker.php:750-765`). Scope §28 does not exclude them, and §12 says match Laravel retry semantics.

Symfony’s README default is Messenger `max_retries: 3`, meaning **3 retries after the first delivery** (`/tmp/soc/README.md:183-190`). Scope §2 invites “framework-neutral” choices from Symfony. An implementer can ship 4 total deliveries and still think they matched the secondary reference. That fails scope §30.9.

**Fix:** Spell the tries=1 table: first delivery runs; an exception on that delivery is terminal unless tries > 1; the next delivery with `attempts > tries` fails without running. Document index `attempts - 1`, stick on last backoff, default backoff 0, `tries = 0` unlimited or rejected (pick one; Laravel allows unlimited). Defer `retryUntil` and `maxExceptions` explicitly if v1 does not want them. Add one sentence: do not copy Symfony’s 3-retry default.

### 10. Job policy has to live in the message if a new deploy may run the worker

Scope §8 says a worker may be a different revision, and the envelope needs “queue metadata needed for execution”. It does not say whether `tries`, `backoff`, `timeout`, and `fail_on_timeout` are snapshotted at dispatch.

Laravel puts them in the payload and the worker reads those keys, not the current class (`Queue.php:176-186`, `Job.php:294-346`). Symfony leaves retry to the worker’s Messenger config (`README.md:171-190`), so a deploy changes in-flight retries.

**Fix:** Choose snapshot-on-dispatch (Laravel) or worker-local policy (Symfony) in §8. For Cloud compatibility, snapshot the effective policy into the v1 envelope and have the worker honor the message. Keep registry defaults only for fields the message omits.

### 11. Wire keys `uuid` and `displayName` are part of the Cloud contract

Scope §8 says “unique job UUID” and “display name” without key names. Laravel’s failed-job `job_name` is `payload.displayName` (`FailedJobProvider.php:85`). Symfony writes both keys because “the platform re-queues a failed job verbatim” and the dashboard identifies the job by name (`CloudQueueTransport.php:402-412`). The comment says those keys are observability metadata and must survive a verbatim requeue.

**Fix:** Require top-level `uuid` and `displayName` in envelope v1, plus the version field. `displayName` should be the human/job name (explicit name override or import path). Do not rename them.

### 12. Loud FIFO/fair rejection contradicts canonical Laravel, and the default group id includes `.fifo`

Scope §10 requires rejecting FIFO per-message delay, rejecting fair-queue groups on FIFO, and rejecting FIFO options on standard queues. That is Symfony (`CloudQueueTransport.php:193-248`, `README.md:112-116` and `149-157`).

Laravel does not reject. It omits `DelaySeconds` when the name ends in `.fifo` (`SqsQueue.php:589-592`) and only adds `MessageDeduplicationId` for FIFO (`SqsQueue.php:616-633`). A standard-queue `MessageGroupId` is sent when the job has one (`SqsQueue.php:601-614`) and is not treated as a conflict. `array_filter` then drops nulls (`SqsQueue.php:635`), so an empty dedup id is omitted and SQS content-based dedup can apply. The comment at `SqsQueue.php:616-618` says return empty to opt into content-based dedup.

Default FIFO group is the **logical queue name after route resolution, including `.fifo`** (`SqsQueue.php:581-612`), not the physical URL and not the stripped base. Symfony uses that same logical name (`CloudQueueTransport.php:206`). Default dedup is `Str::orderedUuid()` (`SqsQueue.php:629`), not Symfony’s `Uuid::v7()` (`CloudQueueTransport.php:207`). Both are unique. Scope §10’s “logical queue name” is right only if the implementer keeps the `.fifo` suffix.

Fresh-delay cap: Symfony throws above **900** seconds and floors milliseconds with `intdiv` (`CloudQueueTransport.php:241-247`). A sub-second fresh delay becomes 0 and is then dropped (`> 0` check). Laravel `secondsUntil` is not capped in `SqsQueue.php:589-592`; SQS errors. Scope’s “fail loudly over 900” matches Symfony and contradicts Laravel. Scope does not say what happens to a positive sub-second fresh delay (retry path rounds up; fresh path in Symfony floors away).

**Fix:** Keep the loud rejections and label them as deviations from `SqsQueue::getQueueableOptions`. State default `MessageGroupId` as the logical name including `.fifo`. State default dedup as a new unique id, and that an explicit empty dedup id means “omit the attribute, allow content-based dedup”. Reject fresh delays `> 900` with a typed error. Decide sub-second fresh delays (reject, or round up to 1). Do not apply the 900 cap to retry visibility.

### 13. Do not port `FailedJobProvider`’s HTTP retry API

Scope §12 says there is no Python failed-job database and Cloud owns retry. It still lists `FailedJobProvider.php` as required reading (§2) and §15 says to match that provider’s event.

The rest of that class is not an emit path. `find()` of an `https://` id downloads the body, decrypts it with the app encrypter, and follows `links.next` (`FailedJobProvider.php:122-199`). The request sends `Cloud-Payload-Version: 1` and `Cloud-Encryption-Cipher`. `forget()` emits a second `failed_job` event whose only time field is `retried_at` (`FailedJobProvider.php:220-225`). Symfony’s subscriber does not emit `retried_at`.

Porting `find()` / `fetchFailedJobs()` into Python is an authenticated download-and-decrypt client aimed at whatever URL showed up as an id. That is out of the product scope and is a security footgun. Scope never forbids it.

**Fix:** Say v1 only emits `failed_job` on terminal failure. Do not implement `find`, `flush`, `forget`, or the encrypted URL fetch. Do not emit `retried_at` unless a later dashboard contract requires it. Terminal SQS outcome remains delete / `processed`.

### 14. Payload limit is 1,048,576 bytes in this baseline, measured with `strlen`

Scope §8 says “the actual transport limit” and never states it. `SqsQueue::MAX_SQS_PAYLOAD_SIZE` is **1048576** (`SqsQueue.php:25`). The same constant caps a `SendMessageBatch` chunk (`SqsQueue.php:512`). Symfony does not preflight; it lets SQS fail (`CloudQueueTransport.php:251`).

The check is byte length of the body, not characters, and it does not include message attributes (`SqsQueue.php:537-544`).

**Fix:** Set `PayloadTooLargeError` at 1,048,576 bytes of the final encoded body, matching `SqsQueue.php:25`. Note that batches are out of scope (§28) so the 10-message batch cap (`SqsQueue.php:31`) is not a v1 feature. Mention SQS `MessageGroupId` / `MessageDeduplicationId` limits (1–128 characters) under FIFO “strict validation”; upstream does not enforce them locally, SQS does.

### 15. Poison messages are deleted, then the error is raised — and that may skip `failed_job`

Scope §7 and §8: unknown jobs and schema/codec failures are terminal and “reported clearly to Laravel Cloud”.

Symfony `decode()` on `MessageDecodingFailedException` reports **`processed`** (or SQS-deletes) and then rethrows (`CloudQueueTransport.php:370-376`). That happens inside `get()`, before the worker-handled failed subscriber runs (`QueueEventSubscriber.php:143-160` only sees `WorkerMessageFailedEvent` for envelopes that were received). So “reported clearly” is not automatically a `failed_job` event.

**Fix:** For deterministic decode/unknown-job/schema failures: delete via `processed` or `DeleteMessage`, emit `failed` + `failed_job` from the worker (do not rely on the agent to do it), and do not release. Add that sequence to §12 so it cannot be implemented as “exception, then normal retry”.

---

## Medium

### 16. Queue URL composition has three edge rules the `{prefix}/{queue}{suffix}` formula misses

`SqsQueue::getQueue` (`SqsQueue.php:687-711`):

- If the name is already a URL (`FILTER_VALIDATE_URL`), prefix and suffix are not applied.
- `Str::finish` appends the suffix only when the name does not already end with it. Naive `{queue}{suffix}` double-appends.
- Prefix is `rtrim`'d of trailing slashes.
- FIFO detection is `str_ends_with($queue, '.fifo')` on the logical name **before** suffixing (`SqsQueue.php:705`, `ManagedQueueConfig.php:100-102`).

Normalization strips prefix then suffix, and puts `.fifo` back (`Queue.php:559-569`, `ManagedQueueConfig.php:154-174`). Laravel normalizes by calling `getQueue()` first, so a logical name is expanded and then stripped (`Queue.php:564`). Passing a name that does not match the configured prefix leaves the expanded string in place.

**Fix:** Document URL pass-through, `Str::finish` idempotence, prefix slash trimming, and “normalize(logical) == logical” as the conformance check. Add a case where the logical name already ends with the suffix.

### 17. Malformed Cloud JSON: fail (Laravel) vs ignore (Symfony)

Scope §6 says fail fast and never silently reinterpret. That matches `json_decode(..., JSON_THROW_ON_ERROR)` (`CloudBootstrapper.php:223`). Symfony turns bad JSON into an empty config and “not configured” (`ManagedQueueConfig.php:52-55`). Scope should say “do not copy `ManagedQueueConfig::fromEnvironment`’s lenient parser.”

**Fix:** One sentence in §6. Invalid JSON, wrong `driver`, or missing `connection` while the env var is set is a configuration error. Absent env var means local/direct mode, not a crash (Symfony boots with no config: `QueueFeature.php:95-97`).

### 18. `LARAVEL_CLOUD_AGENT_SOCKET` exists only on the Symfony side

Laravel reads `config['agent']['socket']` with default `/tmp/cloud-agent.sock` (`Queue.php:370`). Symfony, if the JSON socket is missing, falls through `LARAVEL_CLOUD_AGENT_SOCKET`, then the same default (`ManagedQueueConfig.php:66-73`, `/tmp/soc/src/LaravelCloudBundle.php:46-48` and `102-104`). Scope §11 does not name that env var.

**Fix:** Honor `agent.socket` from the JSON first. If it is absent, honor `LARAVEL_CLOUD_AGENT_SOCKET`, then `/tmp/cloud-agent.sock`. Do not use the env var to override an explicit JSON socket.

### 19. Observability socket behavior is more than “newline-delimited JSON” and “short timeout”

`/tmp/lf/src/Illuminate/Foundation/Cloud/Events.php` (Symfony port is the same shape in `/tmp/soc/src/Observability/Events.php`):

- Connect timeout **2s**, `STREAM_CLIENT_PERSISTENT` (`Events.php:148-153`).
- Stream read/write timeout **2s** (`Events.php:160`).
- JSON flags: `UNESCAPED_SLASHES | UNESCAPED_UNICODE | PRESERVE_ZERO_FRACTION | INVALID_UTF8_SUBSTITUTE`, one object per line, trailing newline (`Events.php:117-125`).
- Failures are swallowed (`Events.php:57-65`). Empty payload is a no-op success (`Events.php:39-41`).
- Partial writes retry; 5 zero-length writes disconnect (`Events.php:77-106`).

Scope §15 says “short connection/write timeout” and “one JSON object per line” only.

**Fix:** Put 2s connect, 2s write, persistent connection, and those JSON flags in §15. Conformance should assert the flags only if the collector is strict; otherwise the important checks are NDJSON, `_cloud_event`, and “socket down does not fail the job”.

### 20. Security is absent as a section

Not optional polish. The pinned code defines a trust boundary the scope never states:

- Cloud JSON and ECS credentials must not be logged. `credentials: "ecs"` must not fall through to ambient keys (finding 7). `SqsConnector.php:101-106`.
- `failed_job.payload` is the raw job body (`FailedJobProvider.php:75`). That can contain user data. The 16 KiB trim is also a data-minimization control (`QueueEventSubscriber.php:46-60`).
- Do not implement the decrypting HTTPS fetch (`FailedJobProvider.php:183-199`).
- The agent socket is an unauthenticated local socket (`Queue.php:367-372`). Do not proxy it onto a network listener.
- LocalStack `endpoint_url` must be refused when the process is in Cloud mode (Cloud JSON present). Otherwise a poisoned env var becomes an SSRF/credential redirect. Symfony’s client has no endpoint override (`SqsClientFactory.php:20-34`).
- SQS TLS verification stays on. Nothing in upstream disables it.

**Fix:** Add a short §security with those six rules.

### 21. Worker-loop defaults that affect conformance tests are missing

| Behavior | Upstream | Scope |
|---|---|---|
| Empty-queue sleep | 3s (`WorkerOptions.php:115`) | “any small rest/sleep” (§13) |
| Rest between jobs | 0 (`WorkerOptions.php:118`) | same |
| `stopWhenEmptyFor` | seconds since last job or start (`Worker.php:427`) | flag name only |
| Memory limit | 128 MB, exit **12** (`WorkerOptions.php:112`, `Worker.php:39` and `424`) | not mentioned |
| Cloud memory exit override | set to `null`, and `??` still uses 12 (`QueueConnector.php:105`, `Worker.php:424`) | not mentioned |
| Multiple queues | comma-separated, first hit wins (`Worker.php:482-499`) | “where appropriate” (§13) |
| Pop error | sleep 1 and continue (`Worker.php:501-507`) | not mentioned |

Cloud sets `restartable` and `pausable` to false (`QueueConnector.php:103-104`). A Python port should not add cache-based restart.

**Fix:** Give direct-mode defaults: sleep 3, rest 0, memory limit out of v1 or explicitly 128/exit 12. Multiple queues: comma-separated priority order, direct mode only. On Cloud, ignore extra queue names (finding 5).

### 22. CI and packaging are acceptance lists without the decisions a matrix needs

Scope §5 and §26 require a 3.10+ matrix, `mypy --strict`, Ruff “unless”, LocalStack, agent emulator, socket tests, packaging proof that `demo/` is absent. They do not say:

- which interpreters (3.10 through which minor)
- whether macOS is a CI target or only a support claim (§27)
- build backend and how `demo/`, `tests/`, and emulators are excluded from the wheel **and** the sdist
- pinned floors for `boto3`, `anyio`, FastAPI, Pydantic v2
- which HTTP client speaks Unix sockets to the agent (not specified anywhere; boto3 does not)
- how the upstream-drift job detects new 13.x tags without moving the pin (§2)

None of that contradicts upstream. It is ambiguous enough that two implementers will ship different matrices and different agent HTTP stacks.

**Fix:** Name CPython 3.10–3.13 (or “latest 3.10+ available on CI”), Linux CI required, macOS not a required runner, hatchling or setuptools with an explicit package map, and `httpx` (or equivalent) with a Unix-socket transport. Drift check compares tags to `v13.33.0` and fails the job on a diff notification, without editing the pin.

---

## Low

### 23. `retried_at` failed-job variant

`FailedJobProvider.php:220-225` emits `_cloud_event: failed_job` with `id`, `queue`, `retried_at` when a failed job is forgotten/retried. Symfony never emits it. Scope’s “Cloud owns retry” is enough only if this variant is explicitly out of v1.

**Fix:** One line under §15: v1 does not emit `retried_at`.

### 24. Duration rounding

Laravel truncates via `(int) diffInMilliseconds` (`Queue.php:490`). Symfony rounds (`QueueEventSubscriber.php:282`). Sub-millisecond differences will flap a strict equality test.

**Fix:** Specify integer milliseconds, truncated toward zero, matching Laravel.

### 25. Region and SQS API version

Symfony defaults region `us-east-1` (`SqsClientFactory.php:24`). Laravel passes the config through (`SqsConnector.php:21-36`) with `'version' => 'latest'` (`SqsConnector.php:48`). Scope’s sample includes region and does not say it is required.

**Fix:** Region is required in Cloud JSON. Do not default it. Direct mode may default `us-east-1` for LocalStack convenience. SQS API version `latest`.

### 26. Group/dedup character set

Scope §10 says “strict validation” and does not define it. Upstream does not validate; SQS rejects bad ids (`SqsQueue.php:601-635` just string-casts).

**Fix:** Either validate 1–128 chars against the SQS alphabet, or say “SQS is the validator, surface its error as a typed transport error”.

### 27. Illustrative FastAPI snippet can collide with dispatch options

Not an upstream bug. §10 and §17 both say the snippet is illustrative and dispatch options must not collide with handler parameters, then still show `queue=` style calls. An implementer can ship that collision.

**Fix:** Pick one public shape in the scope (options object or `.delay().on_queue()` builder) so the prompt does not re-open it.

---

# Part 2 — `AGENT_BUILD_PROMPT.md` vs `PROJECT_SCOPE.md`, and one-pass gaps

The prompt says `PROJECT_SCOPE.md` is authoritative and to consult it for detail. That does not put a decision into the prompt. The gaps below are requirements the prompt drops or weakens, plus decisions neither document freezes that a single pass will guess.

## Critical

### P1. The completion gate is a subset of scope §30

Prompt “Release acceptance gate” checks the matrix, mypy, lint, package build, `py.typed`, `demo/` absent, unit/FastAPI/LocalStack/agent/observability tests, a JSON report, README, the S3 TODO, and “live Cloud not required”.

Scope §30 items that are **not** in that gate, even though earlier prompt sections mention some of them as work:

- §30.3 versioned envelope
- §30.4 FastAPI sync/async, `Depends()`, per-job cleanup, lifespan
- §30.5 separate process, one in-flight job
- §30.8 same-message visibility retry
- §30.9 default attempts 1
- §30.10 fresh delay, retry delay, FIFO, fair queues
- §30.11 deterministic failures do not loop
- §30.13 observability nonfatal vs agent report fatal
- §30.14 process-level timeout
- §30.15 graceful shutdown
- §30.16 prefix/suffix normalization
- §30.17 typed queue-not-found and payload-too-large
- §30.18 eager mode distinct from transport tests
- §30.19 trace context when optional tracing is installed
- §30.20 human-readable report as well as JSON
- §30.21 report rows include upstream file/baseline evidence

The prompt’s “Important proof cases” list covers some of these and still omits, relative to scope §21: sync and async handlers as separate probes, producer and worker as separate processes, direct callability, per-dispatch queue override, delayed standard job, explicit `JobContext.release`, explicit terminal fail, default `tries=1`, retry exhaustion, `fail_on_timeout` as its own case, FIFO group/dedup success (not only “validations”), observability event sequence, `failed_job` event, trace propagation, eager mode.

**Fix:** Replace the gate with scope §30 verbatim, and replace the proof-case list with scope §21’s bullet list. Keep the prompt’s extra rule that the live Cloud row is `skipped` with a reason.

### P2. The prompt never carries the §15 dashboard-decision rule

Scope §15: if Symfony’s log-line workaround differs from Laravel, document it, put it in the conformance report, and prefer whatever actually hits the dashboard. The prompt’s observability section says “inspect before finalizing fields” and stops there. It does not mention 16 KiB, `job_name`, `exception_preview`, or `_cloud_event`.

Combined with part 1 finding 1, a one-pass implementer who treats Laravel as canonical (prompt “Upstream baseline”: framework is canonical) will emit full exception strings and the dashboard will drop them (`QueueEventSubscriber.php:39-69`).

**Fix:** Paste the frozen schema from the §15 fix into the prompt. Call out the Symfony trim as mandatory for transport, and Laravel’s field names as mandatory for the dashboard.

### P3. Framework-vs-Symfony conflicts are unnamed, so “research, then implement” will guess

The prompt’s step 1 says to identify framework-vs-Symfony differences. It does not list the ones that change worker control flow. Those are part 1 findings 2, 3, 4, 5, 9, and 12:

- empty `messageId` is not fatal
- 4xx on `/result` is not fatal; agent-loss exit is 0
- timeout emits `released` and exits 124 without `POST /result`
- agent-on receive ignores CLI queue (deviate from `Queue.php:392-407`)
- tries default is Laravel’s 1, not Symfony’s 3 retries
- FIFO delay is a loud error (deviate from `SqsQueue.php:589-592`)

The prompt repeats scope’s incorrect blanket rules (“malformed 200 is implied fatal”, “any failed `/result` kills the worker”, “timeout releases”). An agent that follows the prompt and skips a line-level reread will implement the wrong contract and then write conformance tests that lock it in. The prompt also says “do not change expected behavior just to make the Python implementation pass”, which makes a wrong first test permanent.

**Fix:** Add a “resolved conflicts” table to the prompt with the decisions from part 1, each with file:line, before the implementation sequence. Tell the conformance catalog to encode those deviations as `partial` or as explicit expected behavior, not as a silent match.

---

## High

### P4. Several scope requirements the prompt simply does not restate

| Scope | Prompt |
|---|---|
| §11 async dispatch must not block the event loop on boto3 | boto3 is named; the offload rule is absent |
| §11 long-poll **65s** | “long poll” only |
| §11 document at-least-once duplicates | absent |
| §13 multiple named queues in direct mode, comma-style priority | “explicit queue selection” only |
| §22 do not mutate expectations so the suite goes green | final report says not to claim full compatibility; the suite rule is weaker |
| §23 CLI errors are package-level, not raw boto3/HTTP traces | absent |
| §24 exception hierarchy (config, queue not found, payload too large, codec, envelope version, unknown job, schema, invalid option, agent protocol, transport, explicit fail) | “define exception hierarchy” in the architecture step, no list |
| §6 malformed Cloud JSON fails; unknown fields kept; local overrides do not win over Cloud JSON | “validate clearly” only |
| §27 Windows agent-emulator support is not a v1 blocker | “Windows best effort” only |
| §5 decorator signatures stay usable under `mypy --strict` | “directly callable” only |
| §10 / §17 dispatch options must not collide with handler parameters | absent; the collision is the main FastAPI API fork |

**Fix:** Add those rows to the prompt’s non-negotiable list. For the API fork, choose a builder or options object in the prompt so Codex and Grok do not invent two public APIs.

### P5. One-pass decisions neither document makes

These are not in scope and not in the prompt. Upstream forces a choice:

1. **Timeout mechanism.** Scope §14 says process-level, not asyncio cancel, including sync handlers. It does not choose subprocess, fork, or `SIGALRM` in the main process. PHP uses `SIGALRM` plus a kill callback that `pcntl_exec`s `/bin/sh -c 'exit <status>'` so the status is 124 rather than 137 from `SIGKILL` (`QueueConnector.php:108-112`, `Worker.php:1057-1072`). Python needs an equivalent that still exits 124. Decide: run each job in a child process; parent enforces the timeout and the agent/SQS outcome.
2. **Agent HTTP client.** Unix socket to `http://localhost` (`Queue.php:367-372`, `AgentClient.php:141-147`). Name the library and the `CURLOPT_UNIX_SOCKET_PATH` equivalent.
3. **Numeric contracts** from part 1 if they are not patched into scope first: body limit 1048576, worker timeout 60, direct `WaitTimeSeconds` 20, visibility clamp 43200, delay cap 900, backoff index `attempts-1`, exit 124 vs exit 0, ECS credential provider.
4. **Envelope policy snapshot** (part 1 finding 10) and required keys `uuid` / `displayName` (finding 11).
5. **Local config keys** for endpoint, region, prefix, suffix, credentials (finding 8).
6. **`@pointer` / overflow ignored in v1** (finding 6).

**Fix:** Add a “frozen decisions” section. Do not leave these to the architecture step’s judgment; they are compatibility behavior.

### P6. `demo/` probe list and report schema are shorter than scope §21 and §22

The prompt requires a human report and JSON, and status values `pass|fail|partial|skipped|unsupported`. The per-row field list omits **`status`** as a field (it is listed above the bullets) and omits scope §21’s “environment metadata” as distinct from versions. Scope §22’s catalog (feature id → Laravel file and pin, optional Symfony file and commit, expected behavior, test, result) is only “record exact files/commits” in the prompt.

**Fix:** Point the demo section at scope §21 as a checklist that must all appear as feature ids, and require each JSON row to include `status`. Require the catalog file in the repo, not only inside the report.

---

## Medium

### P7. README and roadmap shrink scope §25 and §29

Prompt README list covers the happy path, FIFO, fair queues, LocalStack, demo, support matrix, limitations, compression/S3.

Scope §25 also wants the two install lines (core and `[fastapi]`), a statement that `demo/` is not in the wheel (the prompt has this), and Python-native wording rather than Laravel API names (mission statement only).

Scope §29 roadmap items the prompt does not require in the README: Django adapter, Flask adapter, live Cloud smoke once a runtime exists, “no chains/batches until the core is proven”, upstream-drift follow-up. The prompt’s gate only checks that the compression/S3 TODO exists.

**Fix:** Point the README section at scope §25 and §29 as the outline, not a shorter substitute.

### P8. Config and credential rules are too soft to survive contact with Symfony’s lenient parser

Prompt: “Parse it into typed configuration eagerly. Preserve unknown fields. Validate invalid configuration clearly.”

Missing versus scope §6 and versus upstream:

- `JSON_THROW` behavior, not `ManagedQueueConfig.php:52-55`
- `driver` must be `cloud` or local mode is selected by **absence** of the env var (`CloudBootstrapper.php:219-221`, `QueueConnector` boot only if driver is `cloud` at `CloudBootstrapper.php:266-268`)
- `queues` list vs map (`Queue.php:577-581`)
- do not copy socket-exists receive (`CloudQueueTransport.php:24-26` is wrong; `ManagedQueueConfig.php:187-198` is right)

**Fix:** Three bullets in the prompt configuration section.

### P9. Public vs internal modules and the 1.0 bar

Scope §26: distinguish public modules from internal ones, and do not ship 1.0 until the API and Cloud contract are stable. The prompt says leave the repo in a 0.x state and does not mention the module boundary. `mypy --strict` on a flat public package will freeze accidents.

**Fix:** One line: document the public import surface; underscore or private subpackages are not stable.

### P10. Eager mode and core vanilla API are easy to under-build

Scope §18’s core surface (registry, definition, sync/async dispatch, worker, policies, transports, codecs, testing, typed config/errors) and §20’s eager requirements (binding, ser/de, registration, FastAPI DI cleanup, “does not replace transport tests”) are compressed in the prompt to “eager executes binding, ser/de, registration, DI” and “standalone registry”. The “no fake FastAPI app for vanilla” rule is only implied.

**Fix:** Repeat scope §18 and §20 as acceptance bullets (also covered if P1 adopts §30).

---

## Low

### P11. Scaffold step tells the team to produce `PROJECT_SCOPE.md`

Execution step 3 includes `PROJECT_SCOPE.md` as something to create. The file is already the contract. A scaffold pass can rewrite the decisions this audit is about.

**Fix:** “Do not rewrite `PROJECT_SCOPE.md` except to record a deviation the research step proved.”

### P12. Typing and discovery nits from scope §5 and §7

Not restated: tests may relax typing locally; production code may not. Auto-discovery is opt-in and must not be a recursive import scan (§7). `Any` must be local and justified (§5).

**Fix:** One sentence each, or rely on scope if the prompt says “§5 and §7 are normative” instead of paraphrasing.

### P13. License, naming, and first-party tone

Both documents agree on MIT, `laravel-cloud-queues`, `laravel_cloud_queues`, no personal branding (scope §1; prompt only says the package might move to the Laravel org). No action unless the README template grows a personal byline. No upstream conflict.

---

## Suggested edit order

1. Freeze the conflict table (part 1 findings 1–5, 9, 12) inside `PROJECT_SCOPE.md` with file:line and an explicit “deviates from Laravel” mark where Symfony wins.
2. Add the missing numeric and security contracts (findings 6–8, 10–11, 13–15, 20).
3. Replace the prompt’s acceptance gate and proof list with scope §21 and §30, and paste the frozen conflict table so the build does not depend on a second research pass.
