# Laravel Cloud Queues — Scope/Build-Prompt Audit

Auditor: deepseek. Baseline: `laravel/framework` v13.33.0 (`/tmp/lf`) and
`laravel/symfony-on-cloud` 50c945170b6cb5690370d15fd725c6f82495e9ba (`/tmp/soc`).
Reviewed `PROJECT_SCOPE.md` (1257 lines) and `AGENT_BUILD_PROMPT.md` (688 lines).

---

## PART 1 — PROJECT_SCOPE.md findings vs upstream

Grouped by severity. Every claim cites upstream `file:line`.

### CRITICAL

**C1. "Agent /result failure is fatal" is over-broad and contradicts upstream.**
PROJECT_SCOPE.md §11 ("If reporting ultimately fails, treat the worker as fatally
unhealthy"), §15 ("Agent /result reporting … remains fatal when unavailable after
bounded retries"), and acceptance #13 ("agent result-report failure is fatal")
treat *any* /result failure as worker-fatal. Upstream only treats **connect
failures and 5xx** as fatal; a **4xx client rejection is non-fatal**:
- Laravel `Cloud\Queue::reportJobStatusToAgent` (`/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:332-358`): `->throw()` then `->retry(3, 100, ConnectionException only)`; a 4xx re-throws `RequestException` (surfaces as a normal job failure, worker continues), only 5xx becomes `AgentUnreachableException`.
- Symfony `Agent\AgentClient::report` (`/tmp/soc/src/Queue/Agent/AgentClient.php:94-137`): `ConnectException` retried up to 3 attempts × 100ms; `>=500` → `TransportException` (fatal); `>=400` → `RuntimeException` (non-fatal, "report and move on").
Fix: split the contract into *connect/5xx → fatal (stop, don't fetch, exit)* and *4xx → non-fatal (surface the rejection, continue)*; pin retry count (3), inter-attempt delay (100ms), and timeout (10s). Also note the retry scope is **connection exceptions only** — server 5xx is not retried.

**C2. Failed-job event schema is not pinned to the Laravel canonical shape; Laravel and Symfony disagree on fields.**
PROJECT_SCOPE.md §15 lists "concepts" ("event type; failure ID (UUIDv7); queue; started timestamp; attempts; payload; job name/display name; exception preview; exception detail") but never fixes the exact keys. The two upstreams differ materially:
- Laravel `Cloud\FailedJobProvider::log` (`/tmp/lf/src/Illuminate/Foundation/Cloud/FailedJobProvider.php:70-87`) emits: `_cloud_event`, `id`, `queue`, `started_at`, `attempts`, `payload`, `exception_preview` (≤1001 chars), `job_name`, `exception` (full, untruncated).
- Symfony `Queue\QueueEventSubscriber::onFailed` (`/tmp/soc/src/Queue/QueueEventSubscriber.php:174-187`) emits only `id`, `queue`, `started_at`, `attempts`, `payload`, `exception` (≤4000 bytes) — **no `exception_preview`, no `job_name`**.
Since Laravel Framework is the canonical reference (§2) and the dashboard must name/display Python failures, the Python implementation must emit Laravel's `exception_preview` + `job_name` + full `exception`, not the reduced Symfony subset. A multi-agent team that inspects both will most likely under-emit.
Fix: pin the exact Laravel field set and byte budget (`exception_preview` ≤1001, `exception` full) and document the Symfony divergence as a deliberate "canonical Laravel" choice.

**C3. `displayName` top-level envelope key is required but never named.**
Laravel derives the dashboard job name from the *raw payload*: `(json_decode($payload, true) ?? [])['displayName'] ?? ''` (`/tmp/lf/src/Illuminate/Foundation/Cloud/FailedJobProvider.php:85`); Symfony's `encode()` mirrors this by emitting `uuid` + `displayName` at the top level (`/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:407-412`). PROJECT_SCOPE.md §8 says the envelope must carry "display name useful to Laravel Cloud" but never states the wire key must be exactly `displayName` and top-level. If the Python envelope uses `display_name` (idiomatic Python) the dashboard names every failed Python job as empty.
Fix: require a top-level `displayName` key (document it as a Laravel-Could compatibility key, not a Python idiom) and add a conformance test asserting `job_name` is populated from it.

### HIGH

**H1. Payload-size limit value is ambiguous and Laravel's own constant is misleading.**
PROJECT_SCOPE.md §8 says "measure the fully encoded message body against the actual transport limit" but never states the number. Laravel's `SqsQueue::MAX_SQS_PAYLOAD_SIZE = 1048576` (1 MiB) (`/tmp/lf/src/Illuminate/Queue/SqsQueue.php:25`) is the *overflow* threshold, not the SQS cap; the real `SendMessage` limit is **256 KiB (262 144 bytes)**, and Laravel only reaches that 1 MiB threshold when overflow is enabled (`willOverflow`, `SqsQueue.php:537-545`). Symfony does no size check at all (relies on SQS).
Fix: pin the actual limit to 262 144 bytes for `PayloadTooLargeError`, note that Laravel's 1 MiB constant is overflow-only, and add a conformance note for the divergence.

**H2. `queues` config field semantics and "managed queue not found" detection are unstated.**
Laravel `managedQueues()` accepts `queues` as **either a list or an associative map** (`array_is_list($queues) ? $queues : array_keys($queues)`, `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:577-582`) and uses it for `isManagedQueue`/`totalSize`. PROJECT_SCOPE.md §6 shows `"queues": []` but never says what it means or whether the Python package needs it. Separately, "unprovisioned queue" is *not* pre-validated against that list in Laravel — it is detected by translating AWS `AWS.SimpleQueueService.NonExistentQueue` into `ManagedQueueNotFoundException` via SQS middleware (`/tmp/lf/src/Illuminate/Foundation/Cloud/QueueConnector.php:68-86`). The scope (§6, §24) names the error but not the detection mechanism.
Fix: state explicitly (a) whether `queues` is used for any `isManagedQueue`/size API in v1 (if not, mark it "parsed but unused" or drop it), and (b) that queue-not-found comes from SQS `NonExistentQueue` error translation, not list pre-validation.

**H3. Credential semantics and injected config extensions are omitted.**
- `credentials: "ecs"` means "use the ECS container credential provider"; any other value falls through to the SDK default chain (`/tmp/soc/src/Queue/Sqs/SqsClientFactory.php:22-33`; default `'default'` in `ManagedQueueConfig::fromEnvironment`, `/tmp/soc/src/Queue/ManagedQueueConfig.php:85`). PROJECT_SCOPE.md §6 shows `"credentials": "ecs"` with no explanation. For boto3 this maps to an ECS-only credential provider vs the default chain.
- Laravel also augments the injected config at boot: `connection.after_commit`, `connection.overflow`, `connection.credential_cache` (`/tmp/lf/src/Illuminate/Foundation/CloudBootstrapper.php:225-238`) and adds `credential_cache` to SQS connections (`CloudBootstrapper.php:248-259`). PROJECT_SCOPE.md omits all three. `credential_cache` (Pod Identity Agent rate-limit avoidance) and `after_commit` are behavioral; `overflow` is explicitly deferred (§8) but should still be *parsed* as unknown-preserved config.
Fix: document `ecs` vs default chain for boto3; preserve `after_commit`/`overflow`/`credential_cache` as unknown fields; note `credential_cache` as a roadmap item if rate-limiting matters.

**H4. FIFO/delay/fair-queue strictness is Symfony-only; Laravel is lenient — not flagged.**
Three places where the scope (§10) mandates "fail loudly" behavior that only Symfony implements; Laravel defers to AWS or silently passes through:
- Delay > 900s: Symfony throws (`CloudQueueTransport.php:233-248`); Laravel just computes `DelaySeconds` with no cap (`SqsQueue::getQueueableOptions`, `/tmp/lf/src/Illuminate/Queue/SqsQueue.php:589-592`) and lets AWS reject.
- FIFO + per-message delay: Symfony throws; Laravel simply omits `DelaySeconds` when FIFO (`SqsQueue.php:590`).
- Cross-model group/dedup rejection (FIFO attrs on standard, fair-group on FIFO): Symfony throws (`CloudQueueTransport.php:193-222`); Laravel's `getQueueableOptions` does not reject, it just sets `MessageGroupId`/`MessageDeduplicationId` (`SqsQueue.php:599-635`).
These are defensible product choices, but §22 requires "traceable to upstream source or an explicit project-level decision." None are flagged as Laravel-vs-Symfony divergences.
Fix: add an explicit "chosen strictness (Symfony) vs Laravel leniency" note in §10/§22 so the conformance catalog records them as intentional deviations rather than silent drift.

**H5. FIFO dedup default is unspecified and differs between upstreams.**
Laravel default dedup ID is `Str::orderedUuid()` (`/tmp/lf/src/Illuminate/Queue/SqsQueue.php:629`); Symfony uses `Uuid::v7()` (`/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:207`). PROJECT_SCOPE.md §10 says only "Default dedup ID should be unique when content-based dedup is not being explicitly relied upon" — it does not pin which UUID generation. (Failure IDs elsewhere are UUIDv7.) This matters only for cross-language/ordering, but the conformance catalog should pin it.
Fix: pick `uuid4`/`uuid7` explicitly and record the choice.

**H6. Worker-level defaults and CLI controls are incomplete.**
Laravel `WorkerOptions` defaults: `timeout=60`, `sleep=3`, `memory=128`, `backoff=0`, `rest=0`, `maxTries=1` (`/tmp/lf/src/Illuminate/Queue/WorkerOptions.php:107-133`). PROJECT_SCOPE.md §13 lists `--max-jobs`, `--max-time`, `--stop-when-empty`, `--stop-when-empty-for`, and "rest/sleep" but omits `--timeout` (worker default), `--backoff`, `--memory`, `--sleep`, `--force`/maintenance-mode, and `--name`. §14 references "worker-level default" timeout but never gives it a value or a CLI flag. The build team needs the timeout default (60s) to implement §14.
Fix: enumerate the worker defaults (timeout=60, sleep=3, memory=128, backoff=0) and the full CLI surface (`--timeout`, `--sleep`, `--backoff`, `--memory`, `--rest`, `--name`, `--force`), marking which are Cloud-authoritative.

### MEDIUM

**M1. Sub-second backoff rounding: Laravel floors, Symfony rounds up — scope picks Symfony silently.**
Laravel `Worker::calculateBackoff` casts to `(int)` → truncates/floor (`/tmp/lf/src/Illuminate/Queue/Worker.php:826`). Symfony rounds **up** with `ceil` and clamps to 43 200 (`/tmp/soc/src/Queue/Messenger/CloudQueueTransport.php:170`). PROJECT_SCOPE.md §12 says "positive sub-second delays should round upward" — correct *Symfony* behavior but the opposite of Laravel's. Not flagged as a divergence (§22 requires it).
Fix: annotate §12 as a deliberate choice of the Symfony protective behavior over Laravel's flooring.

**M2. `LARAVEL_CLOUD_AGENT_SOCKET` env override is missing.**
Symfony resolves the agent socket from the injected config, then `LARAVEL_CLOUD_AGENT_SOCKET`, then `/tmp/cloud-agent.sock` (`/tmp/soc/src/LaravelCloudBundle.php:101-104`; `ManagedQueueConfig::fromEnvironment`, `/tmp/soc/src/Queue/ManagedQueueConfig.php:66-73`). Laravel reads only `config['agent']['socket']` with a `/tmp/cloud-agent.sock` fallback (`/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:370`) — no env override. PROJECT_SCOPE.md §11 names the default socket but not the `LARAVEL_CLOUD_AGENT_SOCKET` override.
Fix: add the `LARAVEL_CLOUD_AGENT_SOCKET` override and note Laravel has none (Symfony-only convenience).

**M3. `queued` event wiring and shape are not fully specified.**
Laravel emits `queued` only when a job is dispatched on the cloud connection, via a `JobQueued` listener filtered by connection name (`/tmp/lf/src/Illuminate/Foundation/Cloud/QueueConnector.php:91-95` → `Queue::finishQueueingJob`, `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:518-526`). The `queued` event carries no `duration_ms`. Symfony emits `queued` in `onSend` only for the `CloudQueueTransport` sender (`QueueEventSubscriber.php:92-111`). PROJECT_SCOPE.md §15 lists the five types but doesn't state that `queued`/`started` have no `duration_ms` (only `processed`/`released`/`failed` do), which the collector conformance test must assert.
Fix: pin the per-event field shape (which events carry `duration_ms`).

**M4. `duration_ms` rounding differs (Laravel truncates, Symfony rounds-and-clamps).**
Laravel: `(int) $this->processingJobStartedAt->diffInMilliseconds($timestamp)` (`/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:490`) — truncates, can go negative on clock skew. Symfony: `max(0, round(...))` (`/tmp/soc/src/Queue/QueueEventSubscriber.php:275-284`) — rounds and clamps ≥0. Not flagged.
Fix: pick one (recommend `max(0, round())`) and record it.

**M5. Observability socket JSON flags and connection parameters are underspecified.**
Upstream encodes with `JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE` and connects with `STREAM_CLIENT_PERSISTENT`, 2s connect + 2s write timeout (`/tmp/lf/src/Illuminate/Foundation/Cloud/Events.php:124,149,153`; identical in `/tmp/soc/src/Observability/Events.php:117-164`). PROJECT_SCOPE.md §15 says only "newline-delimited JSON … short connection/write timeout." The JSON flags matter for byte-identical fixture comparison (e.g. trailing zeros, unicode).
Fix: pin the flags and the 2s timeouts + persistent-connect behavior.

**M6. Retried-failed-job event (`retried_at`) is missing.**
Laravel emits a second `_cloud_event: failed_job` variant on retry-from-failed (`forget`): `{id, queue, retried_at}` (`/tmp/lf/src/Illuminate/Foundation/Cloud/FailedJobProvider.php:220-225`). PROJECT_SCOPE.md §15 only models the failure event. Since v1 has no failed-job store/retry, this is defensibly out of scope — but §15 should state that explicitly so the conformance catalog records it as `unsupported`, not silently omitted.
Fix: add a one-line "retry-from-dashboard emits `retried_at`; out of scope in v1" note.

**M7. UTC is never stated.**
Both upstreams timestamp in UTC (`CarbonImmutable::now('UTC')`, `/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:479`; `new \DateTimeImmutable('now', new \DateTimeZone('UTC'))`, `/tmp/soc/src/Queue/QueueEventSubscriber.php:71,288`). PROJECT_SCOPE.md §15 says "timestamp precision/format" but not timezone.
Fix: pin UTC + `Y-m-d H:i:s.u` (6-digit microseconds).

**M8. Agent receive is gated on the worker's assigned queue in Laravel.**
Laravel uses the agent only when `runningConsoleCommand('queue:work')` **and** the popped queue equals the worker's `--queue`/configured queue (`/tmp/lf/src/Illuminate/Foundation/Cloud/Queue.php:392-397`). Symfony uses the `enabled` flag alone (`ManagedQueueConfig::agentAvailable`, `/tmp/soc/src/Queue/ManagedQueueConfig.php:196-199`). PROJECT_SCOPE.md §11 correctly says "config authoritative, don't probe socket," but omits the queue-match nuance (on Cloud the worker is pinned to one queue anyway, so this is minor — but the direct/CLI multi-queue mode must not accidentally route agent traffic).
Fix: note the Cloud worker is queue-pinned; local/direct mode never uses the agent.

### LOW

**L1. Suffix dedup and full-URL pass-through.**
Laravel `suffixQueue` uses `Str::finish($queue, $suffix)` (won't double-append) and `getQueue` returns a full URL verbatim when it's already a valid URL (`/tmp/lf/src/Illuminate/Queue/SqsQueue.php:687-712`). PROJECT_SCOPE.md §6 gives the rules but not these two edge behaviors; conformance should cover "name already ends in suffix" and "URL passed directly."

**L2. SIGQUIT also shuts down; SIGUSR2/SIGCONT pause/resume.**
Laravel listens `SIGQUIT, SIGTERM, SIGINT` for quit and `SIGUSR2`/`SIGCONT` for pause/resume (`/tmp/lf/src/Illuminate/Queue/Worker.php:954-974`). PROJECT_SCOPE.md §13 lists only SIGTERM/SIGINT. Pause/resume is arguably out of scope for v1; state it explicitly.

**L3. Fatal-error/OOM releases the in-flight job.**
Laravel reserves 32 KiB and registers a shutdown handler that emits `released` for the current job on `E_ERROR`/`E_CORE_ERROR`/`E_COMPILE_ERROR`/`E_PARSE` (`/tmp/lf/src/Illuminate/Foundation/Cloud/QueueConnector.php:125-133`). PHP-specific; note the *semantic* (unexpected fatal → release, not fail) as a Python design consideration in §14.

**L4. String jobs on standard queues carry no group/dedup.**
Laravel `getQueueableOptions` short-circuits for non-object, non-FIFO jobs (`/tmp/lf/src/Illuminate/Queue/SqsQueue.php:594-597`). Relevant only if the Python core supports a "raw/string job" path; if not, ignore.

---

## PART 2 — AGENT_BUILD_PROMPT.md gaps

Gaps are vs PROJECT_SCOPE.md, or items a multi-agent team needs to build the full feature set in one pass. Cited by PROJECT_SCOPE.md section (§) or build-prompt section (BP §).

### Missing scope items / decisions

1. **Typed exception hierarchy not enumerated (scope §24).** BP §"SQS transport" says "typed package errors" but never lists them. Scope §24 names ~11 concepts (config, managed-queue-not-found, payload-too-large, codec, unsupported-envelope-version, unknown-job, schema-mismatch, invalid-queue-option, agent-unreachable, transport, explicit-failure). A 3-agent team will diverge on naming without this list.
   Fix: paste the scope §24 hierarchy into the build prompt.

2. **Dispatch-options vs job-args collision (scope §10).** Scope explicitly flags that distinguishing job arguments from dispatch options "must be unambiguous and type-friendly," suggesting a builder/options object to avoid colliding with handler params. BP §"Job API" lists default-queue/override but never addresses this collision design decision. High risk of an API where `queue=`/`delay=` collide with real handler kwargs.
   Fix: add an explicit decision item (options object vs reserved-kwarg strategy).

3. **Mypy depth requirements (scope §5).** Scope §5 requires: minimize `Any`, localize+justify unavoidable `Any`, exported generics useful downstream, decorator signature preservation. BP says only "`mypy --strict`". The team needs these to avoid passing strict with pervasive `Any`.
   Fix: copy scope §5 typing requirements.

4. **Failed-job event exact field set (scope §15).** BP §"Observability" says "inspect the exact pinned framework and Symfony source before finalizing fields" but does not itself record the canonical Laravel shape (`exception_preview` ≤1001, `job_name` from `displayName`, full `exception`) or the Symfony divergence — see C2/C3. Without it, the implementer reads both and may pick Symfony's smaller schema.
   Fix: pin the Laravel failed-job schema and the `displayName` requirement in the prompt.

5. **Agent `/result` 4xx vs 5xx split (scope §11).** BP §"Cloud agent protocol" says "if a processed/released result cannot ultimately be reported: stop/terminate" — same over-broad fatality as scope C1. Must add the connect/5xx-fatal vs 4xx-non-fatal distinction and retry specifics (3×, 100ms, 10s).

6. **Queue URL/normalization edge cases (scope §6).** BP §"Queue semantics" lists prefix/suffix/normalization/queue-not-found but omits: FIFO suffix-before-`.fifo` placement test, suffix dedup (`Str::finish`), full-URL pass-through, and `queues` list-vs-map + `managedQueues`/`isManagedQueue` semantics (H2).

7. **Credential handling (scope §6).** BP omits `credentials: ecs` meaning and boto3 provider selection (H3).

8. **Worker defaults and CLI surface (scope §13/§14).** BP §"Worker model"/"Timeout" omit worker `--timeout` (default 60), `--sleep` (3), `--backoff`, `--memory`, `--rest`, `--name`, `--force` (H6).

9. **0.x/1.0 release posture (scope §26).** BP omits the 0.x-until-stable and 1.0 SemVer gating. Minor but part of "release-quality 0.x."

10. **Known deviations log (scope §22).** BP says "record the ambiguity, make the most defensible choice" but doesn't require a persistent `known deviations`/`partial` ledger mapping each Laravel-vs-Symfony choice (H4/H5/M1/M4) to a conformance feature ID. The team needs this to avoid re-litigating the same decisions.

### Missing acceptance items

11. **Acceptance criteria not mapped to feature IDs.** Scope §30 has 25 numbered acceptance criteria; BP's "Release acceptance gate" is a prose list that overlaps but doesn't enumerate several (e.g. scope #3 envelope versioning, #19 W3C/OTel propagation, #21 upstream evidence, #23 demo-excluded-from-artifacts, #25 live-pending). A checklist the whole team ticks is missing.
    Fix: paste the 25 criteria into the gate section.

### Coordination gaps

12. **No owner for the conformance catalog.** BP splits Claude (contract), Codex (impl), Grok (conformance scenarios), but §6 "conformance feature IDs" is under Codex's architecture step while §"Grok" builds scenarios. Unclear who owns the canonical feature-ID list and the "do not mutate expectations to pass" rule (scope §22). Assign one owner (Claude) for the catalog-of-record.

13. **No explicit decision owner for Laravel-vs-Symfony divergences.** Claude "challenges ambiguous semantics" but there is no stated rule that *Laravel Framework wins as canonical* (scope §2). The build prompt should repeat "Laravel is canonical; Symfony is secondary precedent" as the tiebreak rule for every H4/H5/M1/M4 conflict.

14. **Envelope `displayName`/`uuid`/version fields not in the serialization spec.** BP §"Serialization" lists "envelope version; unique job UUID; job wire name; display name" but not the exact top-level key names (`displayName`, `uuid`, `version`) or that `displayName` must be top-level for dashboard naming (C3).

15. **Python runtime decision on timeout enforcement (scope §14).** BP §"Timeout" says "tests must prove externally observable worker/message behavior" but doesn't specify the mechanism for enforcing a process-level timeout on blocking sync code (signal alarm vs watchdog subprocess vs SIGKILL fallback). Three agents could implement three different mechanisms. Scope §14 also leaves it open; the build prompt should force one design decision (e.g. "watchdog subprocess with SIGKILL fallback, exit 124") or at least require the decision be made and documented before coding.

16. **Optional-deps boundary (scope §4).** BP says "one PyPI package, FastAPI extra" but doesn't restate "do not expose nonfunctional Django/Flask extras" and the "reserve clean adapter boundaries" wording. Minor.

17. **`LARAVEL_CLOUD_AGENT_SOCKET` / log-socket env overrides (scope §15).** BP §"Observability" names `LARAVEL_CLOUD_LOG_SOCKET` but the agent-socket override (`LARAVEL_CLOUD_AGENT_SOCKET`) is missing (M2).

---

## Summary of highest-impact fixes

1. Correct the agent `/result` fatality rule (C1): connect/5xx fatal, 4xx non-fatal, retry 3×/100ms/10s.
2. Pin the failed-job schema to Laravel canonical (`exception_preview`, `job_name`, full `exception`) and require a top-level envelope `displayName` (C2/C3).
3. Pin SQS payload limit to 262 144 bytes and clarify Laravel's 1 MiB overflow constant (H1).
4. State `queues` semantics + queue-not-found via SQS `NonExistentQueue` translation (H2).
5. Flag every Laravel-vs-Symfony divergence (strict FIFO/delay/fair validation, dedup UUID, sub-second rounding, duration rounding) as intentional deviations (H4/H5/M1/M4).
6. Enumerate worker defaults + CLI surface (H6) and the typed exception hierarchy + mypy depth requirements in the build prompt.

DONE
