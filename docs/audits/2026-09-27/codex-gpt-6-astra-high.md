# Independent audit of the Python Laravel Cloud Queues specification

## Basis, limits, and citation convention

Read `PROJECT_SCOPE.md` (1,257 lines) and `AGENT_BUILD_PROMPT.md` (688 lines), then traced the pinned PHP implementations, their relevant tests, and Symfony's agent-server fixture. No repository files were edited. No other auditor's report was read and no sub-agents were used.

Verified local baselines:

- **LF** = `/tmp/lf`, `laravel/framework` tag `v13.33.0`, commit `91188a17ceaa3dbace6e8a5f7abd0d042e466359`.
- **SOC** = `/tmp/soc`, `laravel/symfony-on-cloud`, commit `50c945170b6cb5690370d15fd725c6f82495e9ba`.

`LF:src/...:123` and `SOC:src/...:123` below are upstream file:line citations within those checkouts. `S§n` refers to the actual numbered section in `PROJECT_SCOPE.md`. The build prompt has **no section numbers**: this audit assigns `B§1`–`B§22` to its level-two headings, in document order, and supplies a heading/line index in Part 2. This avoids inventing native section numbers.

This is a specification/source audit, not a completed implementation review. The PHP suites were inspected, not executed; no Cloud or AWS account was exercised. These repositories contain clients, not the proprietary Cloud agent, dashboard, collector, or control plane. Their behavior cannot prove those services' full contracts. Python runtime, security, packaging, and CI findings are explicitly engineering gaps inferred from the proposed Python design; PHP citations establish the corresponding upstream boundary, not facts about Python. Supplementary Python/AWS reference URLs are included where useful. Attempts to fetch those external pages failed due to unavailable DNS in this environment, so they are not represented as freshly verified sources.

Severity: **critical** = an unresolved design can invalidate a mandatory safety/recoverability promise; **high** = likely contract break or major acceptance gap; **medium** = material edge case or implementation ambiguity; **low** = clarification or maintenance gap. Recommendations preserve the stated v1 exclusions rather than expanding this into a general queue framework.

## Part 1 — PROJECT_SCOPE.md findings

### Critical

#### S-C1 — Hard timeouts, direct sync execution, and lifecycle guarantees need one implementable process model

**Scope:** S§13 “Async runtime”, “FastAPI lifespan”, “Per-job FastAPI dependency scope”; S§14; S§30 items 4, 14–15.

The scope simultaneously requires synchronous/native handlers to run directly in the worker process, hard timeout correctness even for blocking native code, a once-per-worker FastAPI lifespan, and timeout outcome reporting. A timeout task on the same AnyIO loop cannot run during a blocking sync handler. Python signal callbacks also cannot be assumed to execute promptly inside arbitrary native code. Cancelling a task or merely arming a Python signal is insufficient. A killed process cannot promise dependency/lifespan teardown or perform its own reliable terminal reporting afterward.

**Upstream evidence:** Laravel installs a `SIGALRM` callback, applies terminal-failure policy, and kills the worker in `LF:src/Illuminate/Queue/Worker.php:319–355`; Cloud configures exit 124 and an exec-based exit hook at `LF:src/Illuminate/Foundation/Cloud/QueueConnector.php:101–112`. This is PHP process behavior, not a portable Python implementation recipe.

**Local runtime evidence:** On Python 3.14.3/macOS, a child process armed `signal.setitimer(..., 0.020)` and called `hashlib.pbkdf2_hmac('sha256', b'password', b'salt', 3000000)`. The Python signal callback ran at **0.202 seconds**, after the native operation's delay, rather than at 20 ms. This directly disproves a universal signal-only deadline guarantee. See reproduction in the appendix.

**Recommended fix:** Freeze the process topology before implementation: a supervisor/watchdog independent of the executing interpreter, with one long-lived execution process that owns the app lifespan and executes one job at a time, is one defensible approach. Kill/join the execution process before allowing a retry; explicitly assign result-reporting ownership and bounded deadlines. Specify clean-shutdown teardown versus best-effort/no teardown after hard kill. Do not fork an already-running event loop or live boto/socket clients. Prove CPU-bound async code, sync Python loops, native blocking code, swallowed cancellation, and timeout/ack races in subprocess tests on Linux and macOS. Any architecture restriction or reduced guarantee must be recorded as a deviation, not hidden behind “process-level”.

#### S-C2 — Failed-job replay, truncation, and best-effort telemetry do not jointly guarantee recoverability

**Scope:** S§8 “Never silently truncate”; S§12 “let Laravel Cloud own ... retry workflows”; S§15 dashboard parity and temporary workaround; S§30 item 12.

The scope acknowledges the collector workaround but leaves the data-loss decision unresolved. Laravel emits the complete original payload. Symfony's workaround removes all but `uuid`, `displayName`, and `body`, truncates the body, and explicitly acknowledges that replay cannot reconstruct the job. Applying that helper to the proposed Python envelope would discard `args`, `kwargs`, version, and context even for small messages. Conversely, a complete SQS-sized payload can exceed the collector limitation described in the pinned Symfony source. Finally, a failed telemetry write followed by terminal deletion leaves no package-owned replay copy, because the failure store is explicitly excluded.

**Upstream evidence:** Full payload and exception: `LF:src/Illuminate/Foundation/Cloud/FailedJobProvider.php:70–87`. Collector workaround and constants: `SOC:src/Queue/QueueEventSubscriber.php:38–69`. Acknowledged replay limitation and projection: `SOC:src/Queue/QueueEventSubscriber.php:163–219`. Best-effort emission: `LF:src/Illuminate/Foundation/Cloud/Events.php:51–65`. Laravel's own failure path deletes before dispatching `JobFailed`: `LF:src/Illuminate/Queue/Jobs/Job.php:213–223`.

**Recommended fix:** Make an explicit release decision for full replayable payloads versus intentionally lossy dashboard records. Preserve the whole Python envelope when claiming retry support; never copy Symfony's field projection. Test final escaped NDJSON byte length, multibyte strings, and replay from captured failure data. Record collector-limited cases and unavailable live dashboard verification as `partial`/`skipped`. Document that terminal failure records can be lost during telemetry outages under the mandated best-effort/no-store policy. Do not claim guaranteed durable failed-job storage or dashboard replay based solely on a local socket collector.

### High

#### S-H1 — The illustrative configuration is not a complete parser or precedence contract

**Scope:** S§6, S§13 “Queue selection”.

`queue`, `connection.queue`, `queues`, `agent.enabled`, and explicit/local settings need distinct meanings. Laravel's SQS sender takes `connection.queue`; its worker fallback takes top-level `queue`. Symfony prefers `connection.queue`, then top-level `queue`, then `default`. Laravel accepts `queues` as either a list or an associative map and uses it for managed-queue enumeration; the provided code does not make it a dispatch allowlist. Rejecting every name absent from the example's empty `queues` would break valid dispatch. “Required fields” is unspecified for producer, agent consumer, direct consumer, and eager mode.

**Evidence:** `LF:src/Illuminate/Queue/Connectors/SqsConnector.php:27–35`; `LF:src/Illuminate/Foundation/Cloud/Queue.php:404–407,577–581`; `SOC:src/Queue/ManagedQueueConfig.php:48–91`.

**Recommended fix:** Add a field/type/default/precedence table and role-specific validation. Preserve list/map forms of `queues` without treating absence as proof that a queue is unprovisioned. Define missing versus empty versus malformed JSON, nulls, string booleans, driver validation, and when local overrides are accepted. Keep fail-fast malformed JSON: it matches Laravel's throwing decoder, whereas Symfony deliberately degrades malformed JSON to unconfigured (`LF:src/Illuminate/Foundation/CloudBootstrapper.php:219–240`; `SOC:tests/ManagedQueueConfigTest.php:98–103`). Label strict Python validation as a deliberate tightening where upstream coerces values.

#### S-H2 — `credentials: "ecs"` cannot be treated as an ordinary boto3 default session

**Scope:** S§6, S§11, S§27.

The production example promises ECS credentials without defining provider selection, refresh, overrides, or the richer shapes Laravel accepts. Laravel selects an explicit ECS/instance provider, accepts provider options, explicit key/secret/token, and optional caching. Symfony explicitly selects ECS for `ecs`, but otherwise falls back to the SDK chain. A naive boto3 default chain may choose environment/profile credentials ahead of the intended container role. Capturing temporary credentials once at startup can break a long-lived worker when they expire.

**Evidence:** `LF:src/Illuminate/Queue/Connectors/SqsConnector.php:62–113,144–168`; `LF:src/Illuminate/Foundation/CloudBootstrapper.php:234–255`; `SOC:src/Queue/Sqs/SqsClientFactory.php:20–34`; `SOC:src/Queue/ManagedQueueConfig.php:205–207`.

**Recommended fix:** Specify supported credential shapes and provider precedence, use refreshable SDK providers, and test mocked container credentials/rotation without real secrets. Preserve or clearly reject unsupported provider options. Define region and local endpoint configuration, SDK retry/connect/read deadlines, and client lifetime. Do not pass the PHP configuration wholesale into boto3. Laravel's cross-process credential cache is optional; document its omission and Pod Identity throttling implications rather than implementing a new cache speculatively.

#### S-H3 — Queue URL concatenation is wrong for already-suffixed names and incomplete for full URLs

**Scope:** S§6 “Queue URL/name rules”, S§10, S§30 item 16.

Laravel does not always perform literal `{queue}{suffix}` concatenation. It uses `Str::finish`, keeping an already-present suffix from being appended again, removes trailing slashes from the prefix, and passes a valid full URL through unchanged. Symfony's builder blindly appends the suffix and does not implement full-URL pass-through. For suffix `-env-x`, `orders-env-x.fifo` must not become `orders-env-x-env-x.fifo` under Laravel semantics. Normalization of `None`, logical names, physical names, and foreign URLs also needs a contract: Laravel first resolves through `getQueue`, Symfony normalizes the supplied string directly.

**Evidence:** `LF:src/Illuminate/Queue/SqsQueue.php:687–711`; `LF:src/Illuminate/Foundation/Cloud/Queue.php:559–569`; `SOC:src/Queue/ManagedQueueConfig.php:130–174`.

**Recommended fix:** Adopt a table of Laravel-derived input/output examples, including already-suffixed FIFO/standard names, default/empty input, trailing slashes, empty suffix, full URLs, and suffix-like substrings in the middle. Decide explicitly whether Python exposes full URLs only in direct mode; if restricted, record a supported-surface deviation. Keep configured prefix/suffix and logical queue identity separate; do not blindly normalize twice or strip arbitrary substrings.

#### S-H4 — Cloud receive selection is Symfony-like, not identical to Laravel's guard

**Scope:** S§6, S§11, S§13 “Queue selection”.

Laravel requires three conditions for agent use: enabled flag, running `queue:work`, and equality between the requested queue URL and the worker queue URL. Its worker queue is read from CLI `--queue` before top-level config. Symfony uses its `useAgent` flag and ignores requested queue names on that path. The scope chooses authoritative hosted assignment but does not identify the source of that assignment beyond an enabled flag or explain conflicting CLI values.

**Evidence:** `LF:src/Illuminate/Foundation/Cloud/Queue.php:392–407`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:67–81`; `SOC:src/Queue/Messenger/CloudQueueTransportFactory.php:45–55`.

**Recommended fix:** Define Python worker-mode detection and the assignment/default distinction. In agent mode reject conflicting CLI selections or explicitly report that the agent assignment wins; never silently switch to direct receive. Record this as the chosen framework-neutral adaptation, not exact Laravel guard parity. Use the received queue coordinates for message ownership and explicitly decide telemetry attribution when they differ from producer defaults.

#### S-H5 — Agent protocol needs an exact wire/error matrix; blanket statements differ from upstream

**Scope:** S§11; S§20 agent emulator.

Missing details can make clients incompatible:

- `POST /result` carries `messageId`, nullable/omitted `receiptHandle`, `status`, and nullable/omitted `delay`; zero delay must survive filtering. There is no `failed` status. Terminal failures use `processed`.
- Laravel GET uses 65 seconds and `retry([0, 500], throw:false)`; Symfony GET makes one request. Both accept 204 and require 200 for a message.
- A JSON array/object lacking a nonempty string `messageId` becomes no work in both transport wrappers, rather than the universal fatal malformed-response behavior promised by the scope. Non-string handles become null; non-string bodies become empty. Laravel and Symfony differ on missing queue URL/attributes handling.
- POST uses 10 seconds, three total connection attempts, 100 ms spacing. HTTP 5xx is fatal without that connection retry loop; 4xx is a different exception path, not automatically the same fatal-agent category. Symfony explicitly uses `RuntimeException` for 4xx and tests 409; Laravel rethrows `RequestException` for non-5xx. The scope's all-report-failures-fatal rule is a stricter project decision.

**Evidence:** `LF:src/Illuminate/Foundation/Cloud/Queue.php:260–285,294–373`; `LF:src/Illuminate/Foundation/Cloud/CloudJob.php:38–60`; `LF:src/Illuminate/Foundation/Cloud/AgentAwareLostConnectionDetector.php:27–30`; `SOC:src/Queue/Agent/AgentClient.php:44–75,94–146`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:300–317`; `SOC:tests/AgentClientTest.php:75–124`.

**Recommended fix:** Publish request/response fixtures, required/optional types, null/zero handling, accepted status codes, redirect policy, and per-operation retry budgets. Retain stricter fatal behavior if desired, but label it. Separate invalid HTTP/agent framing from a valid delivered message with a poison application body. Test lost response after accepted result, stale handles, 409, 5xx, malformed JSON, missing ID, null handle, and timeouts. Treat repeated POSTs as potentially ambiguous; the client sources do not prove agent idempotency.

#### S-H6 — Retryable timeout “release” confuses lifecycle telemetry with an SQS/agent operation

**Scope:** S§12, S§14, S§15, S§21 timeout probes.

On a retryable Laravel timeout, the alarm handler does **not** call `job->release(backoff)`. It checks terminal policies, dispatches timeout, and kills. Cloud's stopping listener emits a lifecycle event with default `released`; that method only emits telemetry. The agent/SQS recovery path then determines redelivery. On the final allowed attempt, timeout is terminal even when `failOnTimeout` is false. A spec that requires a released POST and ordinary backoff for every timeout is a deviation; a spec that releases the message before killing a still-running Python handler can cause overlapping execution.

**Evidence:** `LF:src/Illuminate/Queue/Worker.php:324–355,729–739,791–795`; `LF:src/Illuminate/Foundation/Cloud/QueueConnector.php:120–132`; `LF:src/Illuminate/Foundation/Cloud/Queue.php:473–496`.

**Recommended fix:** Specify timeout transitions separately from ordinary exceptions: retryable, attempts exhausted, explicit fail-on-timeout, crash, and report failure. Decide whether Python intentionally reports a release after stopping the execution process, or matches passive redelivery after exit. Give a timeout default (Laravel 60), override precedence, `0` semantics, timing window, teardown expectations, and exit code. Test default `tries=1` plus timeout as terminal, and `tries>1` as retryable until exhausted.

#### S-H7 — Delivery budgets and policy ownership are not fully specified

**Scope:** S§8, S§12, S§14.

Only default `tries=1` and “effective policy” are fixed. Missing are pre-execution exhaustion checks, explicit-release budget consumption, `tries=0`, list-backoff indexing/tail behavior, job-versus-worker precedence, and the source of policy when producer/worker revisions differ. Checking attempts only when a handler throws permits over-budget execution after crashes, timeouts, or repeated explicit release. Laravel serializes job policy in the payload and checks attempts before invoking the handler.

**Evidence:** `LF:src/Illuminate/Queue/Queue.php:174–185`; `LF:src/Illuminate/Queue/Jobs/Job.php:294–346`; `LF:src/Illuminate/Queue/Worker.php:598–617,701–717,729–739,817–826`; `LF:src/Illuminate/Queue/WorkerOptions.php:107–119`. Symfony counts **retries**, not total tries: `SOC:tests/CloudRetryIntegrationTest.php:30–62,65–88`.

**Recommended fix:** Freeze a transition/policy table: total delivery count, pre-run `attempt > tries`, terminal ordinary failure at `attempt >= tries`, explicit release consuming a delivery, `0` unlimited if supported, default backoff 0, index `attempt-1`, repeat final list entry. Reject invalid/empty/nonfinite policy values. Choose dispatch-time policy snapshot or worker-registry policy explicitly. Scope `retryUntil`, `maxExceptions`, and global non-retryable exception rules as supported or deferred; Laravel has them, but they are not implied implementations merely by saying “match Laravel”.

#### S-H8 — Visibility lease ownership and the usable 12-hour bound are missing

**Scope:** S§10–14, S§20.

The agent is described upstream as heartbeating a held message, whereas direct receive has no heartbeat in these implementations. The scope does not require a direct-mode visibility timeout longer than execution and acknowledgement, nor define renewal. Thus a default 60-second handler can outlive a shorter queue visibility window and run concurrently elsewhere despite one job per worker. A literal clamp to 43,200 is also not sufficient to guarantee a valid SQS visibility update after time has elapsed since receipt: the AWS API bounds visibility by the remaining maximum period from receive.

**Evidence:** Agent heartbeat ownership: `SOC:src/Queue/Agent/AgentClient.php:13–20,79–90`. Direct operations, without renewal: `LF:src/Illuminate/Queue/SqsQueue.php:644–655`; `LF:src/Illuminate/Queue/Jobs/SqsJob.php:66–85`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:264–282,323–349`. Symfony's literal 43,200 clamp is at `:170`, and its test asserts SDK arguments rather than a real elapsed lease (`SOC:tests/CloudRetryIntegrationTest.php:91–104`). Supplementary service reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html .

**Recommended fix:** Define direct receive visibility configuration and require it to cover timeout plus completion/report margin, or implement a renewal mechanism that remains alive during sync/native execution. Specify lease loss as a transport condition, not a handler exception. Account conservatively for elapsed lease time when bounding long visibility resets, and acknowledge that the agent's original receive timestamp/behavior is not fully exposed here. Add long-running, lease-expiry, stale-handle, and near-12-hour boundary simulations; label service assumptions that cannot be proved locally.

#### S-H9 — Freeze the exact dashboard event contract, including differences in ordering

**Scope:** S§8, S§15, S§20–22.

“Concepts such as” is insufficient for a release-blocking wire protocol. Laravel lifecycle events use `_cloud_event: "queue"`, `timestamp: "YYYY-MM-DD HH:MM:SS.ffffff"` in UTC (no `T` or timezone suffix), `type`, and normalized `queue`. Only completion events include integer `duration_ms`. Failed jobs use `_cloud_event: "failed_job"`, a **new failure UUIDv7**, `queue`, `started_at`, integer `attempts`, raw **string** `payload`, `exception_preview`, `job_name`, and `exception`. Preview is up to 1,001 Unicode characters; job name comes from top-level `displayName`. Message ID, payload UUID, and failure UUID are separate identities.

Laravel emits `failed_job` before its finishing `queue/failed`; Symfony emits `queue/failed` first, omits `job_name`/`exception_preview`, trims the payload/exception, and rounds duration where Laravel casts elapsed milliseconds. Laravel generally flushes completed lifecycle telemetry at the next pop or worker stop; Symfony emits on worker lifecycle events.

**Evidence:** `LF:src/Illuminate/Foundation/Cloud/Queue.php:242–250,473–496,518–550`; `LF:src/Illuminate/Foundation/Cloud/FailedJobProvider.php:67–89`; `SOC:src/Queue/QueueEventSubscriber.php:128–187,245–283`; `SOC:tests/QueueEventSubscriberTest.php:145–204`.

**Recommended fix:** Check in exact JSON fixtures/schema and an event-state table with Laravel as canonical. Specify top-level `uuid` and `displayName` in the Python envelope for dashboard metadata, even though PHP job execution interoperability is excluded. Define ordering/timing deviations, zero/negative elapsed handling, no duplicate terminal event, no `failed_job` on an ordinary retry, and emission on final-worker exit. Cover pre-execution poison failures and telemetry outages, not only handler exceptions.

#### S-H10 — Poison messages must retain transport identity before decoding

**Scope:** S§7–8, S§11–12, S§15.

The scope requires malformed/unknown/schema-invalid jobs to be terminal and visible, but does not say how failure handling works before a job object or display name can be decoded. A decoder that throws before the worker retains the raw body, queue URL, receipt, and receive count cannot both report and acknowledge the poison message. A broad transport-fatal handler for all malformed “responses” would instead restart forever on a valid agent delivery carrying an invalid application body.

**Evidence:** Symfony creates a received stamp before decoding, deletes/reports processed on `MessageDecodingFailedException`, and rethrows at `SOC:src/Queue/Messenger/CloudQueueTransport.php:364–390`. Its normal observability requires that stamp on a successfully produced envelope (`SOC:src/Queue/QueueEventSubscriber.php:143–149,256–267`); the decode catch itself does not emit `failed_job`. It therefore is not sufficient precedent for the scope's stronger poison-message observability guarantee.

**Recommended fix:** Have a transport delivery record independent of application decoding. Start timing and retain raw evidence before parsing; use fallback job identity when needed, report a deterministic terminal error, and complete the delivery exactly once. Test malformed JSON, unsupported versions, unknown codec/job, type errors, and unavailable failure socket through a real worker in both receive modes. Never dynamically import an unknown wire job to “repair” registration.

#### S-H11 — Dependency teardown is ordered after acknowledgement without a failure policy

**Scope:** S§13 shutdown list, dependency cleanup; S§17; S§30 items 4, 15.

The shutdown list says report outcome, then run job dependency cleanup. A `yield` dependency may commit a transaction or flush data on exit. If success is acknowledged first and teardown fails, the message is already gone despite incomplete application work. The scope also leaves override/cache/subdependency/`Annotated` behavior and request-only dependency rejection undefined. “Depends where practical” is too weak to serve as a complete integration contract.

**Evidence/context:** Upstream makes terminal transport actions real at `LF:src/Illuminate/Foundation/Cloud/CloudJob.php:38–60` and `LF:src/Illuminate/Queue/Jobs/SqsJob.php:93–111`; the PHP adapters provide no Python/FastAPI teardown policy. This finding is a Python-specific ordering gap, not a claim that Laravel has FastAPI DI.

**Recommended fix:** Define job success as handler completion **plus successful per-job dependency teardown**, then acknowledge. Specify teardown exceptions, teardown after explicit release/fail, cancellation shielding and a bounded cleanup deadline. Test `dependency_overrides`, cached subdependencies, sync/async yield dependencies, and failure paths. Keep injected values outside the serialized argument surface and reject caller attempts to supply privileged `JobContext`/service dependencies through payload data. State direct-call behavior: ordinary Python calls do not automatically resolve `Depends`.

#### S-H12 — Registry and codecs need a trust-boundary contract, not only “no pickle”

**Scope:** S§7–8, S§17–18, S§24.

JSON can still trigger unsafe imports/constructors or resource exhaustion if wire type tags are resolved through arbitrary Python import paths. Automatic discovery must be based on configured packages, never message content. Dictionary/type-tag collisions, deep recursive values, huge metadata, NaN/infinity, duplicate keys, enum identity, timezone treatment, and bool-versus-int coercion are undefined. Binding with `inspect.signature` alone does not validate annotations or restore dataclass/enum types.

**Upstream evidence/context:** PHP constructs/unwraps its different envelope at `SOC:src/Queue/Messenger/CloudQueueTransport.php:393–429`; Laravel embeds serialized PHP command metadata at `LF:src/Illuminate/Queue/Queue.php:174–190`. Neither is a Python safe-codec contract. The Python-only safety requirement follows from S§8's explicit departure from those payload formats.

**Recommended fix:** Resolve jobs and codecs through startup allowlists; reject duplicate wire-name registrations and unsupported types deterministically. Define a small precise v1 tagged encoding, supported annotation subset and coercion policy, resource limits, and collision escaping. Validate on producer and worker. Prohibit importing classes from untrusted envelope data and keep runtime/DI objects non-serializable. Add hostile-envelope tests and core-only round trips without Pydantic. Do not broaden to arbitrary Python object serialization.

#### S-H13 — SQS send limits and fractional-delay semantics are not frozen

**Scope:** S§8 payload size, S§10 delays/FIFO/fair, S§20 tests.

“Actual transport limit” omits a pinned value and boundary behavior. The pinned Laravel source has a **1,048,576-byte** SQS maximum, not the historical 256 KiB limit. Define UTF-8 byte measurement, any queue-specific lower maximum, and whether attributes count if introduced. Fresh-delay fractions are unspecified: Symfony floors milliseconds for fresh sends but rounds up retry delays; e.g. a positive subsecond fresh delay becomes `DelaySeconds=0`, and 900.999 seconds floors to 900 before its cap check. Laravel omits FIFO delay rather than rejecting it. The scope's stricter behavior is sensible, but not exact Laravel/Symfony parity.

**Evidence:** `LF:src/Illuminate/Queue/SqsQueue.php:20–25,579–635`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:151–170,214–247`; `SOC:tests/CloudQueueTransportTest.php:254–265`.

**Recommended fix:** Pin the baseline size and test limit−1/limit/limit+1 using final encoded UTF-8 bytes, including escaping. Define integer-only fresh delays or a documented rounding rule and validate before lossy conversion. Specify negative/nonfinite values and FIFO `None` versus explicitly supplied zero. Validate group/dedup identifiers against SQS's limits (including the 128-character identifier bound) rather than calling validation “strict” without a rule. Use the AWS SendMessage reference to freeze character/size rules: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html . Preserve package errors for service-side lower limits without classifying arbitrary `InvalidParameterValue` errors as payload-too-large.

#### S-H14 — FIFO deduplication defaults can disable the content-based mode the scope alludes to

**Scope:** S§10 FIFO/fair, S§12, S§21.

An explicit generated deduplication ID overrides content-based deduplication. Content-based hashing of the complete Python envelope is also ineffective for identical business calls when every envelope receives a fresh UUID or changing trace metadata. “Unique when content-based dedup is not ... relied upon” therefore needs an actual opt-in omission mode and a definition of what is deduplicated. The default queue-name group serializes the entire FIFO queue. Fair grouping offers no ordering or deduplication guarantee.

**Evidence:** Laravel permits an empty dedup callback result to omit the ID and otherwise generates one (`LF:src/Illuminate/Queue/SqsQueue.php:610–635`). Symfony always chooses a supplied ID or UUIDv7 (`SOC:src/Queue/Messenger/CloudQueueTransport.php:202–207`) and its stamp is deliberately non-sendable (`SOC:src/Queue/Messenger/CloudFifoStamp.php:26–37`). Fair semantics are explained in `SOC:src/Queue/Messenger/CloudMessageGroupStamp.php:17–25`. Supplementary API reference: AWS SendMessage, linked in S-H13.

**Recommended fix:** Choose default unique ID plus explicit business dedup ID; either define an intentional content-based mode with its envelope caveat or explicitly defer it. Generate IDs once per logical send operation and reuse them across internal network retries. Document FIFO group ordering scope, dedup window, ambiguous send outcomes, and continued idempotency requirements. Visibility retries retain original SQS FIFO/fair attributes; dashboard re-dispatch is a new message and needs a separate metadata/replay decision.

#### S-H15 — Acknowledgement failures need a state machine distinct from handler retries

**Scope:** S§9, S§11–14, S§24.

The fatal agent-result rule is good but does not define direct SQS ack/release failures, cancellation of an in-flight boto3 call, or racing timeout/shutdown/result completion. A broad `except Exception` around handler plus completion can treat an acknowledgement timeout as a handler error and issue a contradictory release/fail. The sync `JobContext.release/fail` API also needs a way to short-circuit safely without starting blocking agent/boto I/O inside an async handler.

**Evidence:** Outcomes are transport-specific in `LF:src/Illuminate/Foundation/Cloud/CloudJob.php:38–68` and `LF:src/Illuminate/Queue/Jobs/SqsJob.php:66–111`; Laravel guards automatic release with deleted/released/failed state at `LF:src/Illuminate/Queue/Worker.php:671–683`. Symfony separately no-ops rejection after a recorded retry (`SOC:src/Queue/Messenger/CloudQueueTransport.php:97–112`).

**Recommended fix:** Freeze delivery states and their single owner: received → running → chosen outcome → reporting → completed/ambiguous. Handler failures, deterministic decode failures, and completion transport failures are separate categories. Permit only retries of the same chosen report; no second contradictory outcome and no fetch after an ambiguous fatal completion. Define explicit release/fail as control flow or a deferred outcome recorded for the worker, including behavior if user code catches it. Bound direct SDK retries and report/cleanup deadlines; document at-least-once behavior for producer sends as well as consumer acknowledgements.

### Medium

#### S-M1 — AnyIO backend, sync/async bridges, and client lifetimes need explicit decisions

**Scope:** S§9, S§11, S§13, S§16–18, S§20.

Using AnyIO does not establish whether Trio is supported; FastAPI/user libraries may require asyncio. Nor does it define how sync dispatch behaves inside an active event loop, how eager sync dispatch executes an async handler, or what cancellation means while a boto3 operation continues in a thread. Blocking observability and credential refresh can also stall the application loop even if send/receive was offloaded. Long-lived sockets/clients must not be reused across an unsafe fork or wrong event-loop lifetime.

**Evidence/context:** The upstream transport is synchronous (`SOC:src/Queue/Messenger/CloudQueueTransport.php:115–253,323–349`), and event emission can perform blocking socket I/O (`LF:src/Illuminate/Foundation/Cloud/Events.php:71–106,141–168`). These do not solve Python concurrency semantics.

**Recommended fix:** Declare asyncio as the initial backend unless Trio is deliberately tested. Use one async core and a documented sync boundary or equivalent shared pipeline; forbid nested loop runners and never silently run `asyncio.run` inside a live loop. Define task cancellation, offload capacity, context propagation, and who owns/ closes SDK and HTTP resources. Cover concurrent producer dispatch, cancellation during send/report, and core/eager APIs inside and outside a running loop. Protect persistent telemetry writes against interleaving from concurrent producer callers.

#### S-M2 — Graceful shutdown has no receive-race or deadline contract

**Scope:** S§13–14, S§23, S§27.

“Stop fetching, finish the current job” does not say what to do if SIGTERM arrives while a 65-second agent GET is pending and the request then hands over a message. Abruptly cancelling that request can leave the agent holding work the worker never executes. AnyIO async signal consumption also cannot progress on a loop blocked by a sync handler. Worker max-time and empty-period options need polling granularity and reset semantics, not just names.

**Evidence:** Agent long-poll duration: `LF:src/Illuminate/Foundation/Cloud/Queue.php:297–300`; Laravel signal handling sets a quit flag (`LF:src/Illuminate/Queue/Worker.php:950–961`); stop/max-time/empty checks happen between iterations (`LF:src/Illuminate/Queue/Worker.php:419–429`).

**Recommended fix:** Specify shutdown during idle receive, delivery handoff, active job, teardown, and result reporting. Define disposition of a late-arriving message and bounded shutdown/report deadlines. Distinguish worker max-time from a hard per-job timeout; set a documented grace period compatible with platform termination. Test actual signals, repeated signals, and late GET completion. Restore signal handlers for any embedded/core worker API; declare main-thread/platform restrictions. SIGQUIT parity may be deferred explicitly; SIGKILL cannot promise cleanup.

#### S-M3 — Socket behavior needs framing, bounded writes, recovery, and local security tests

**Scope:** S§11, S§15, S§20, S§27.

A Unix socket is a byte stream; one `send` need not write one complete line. The scope names NDJSON and short timeouts, but not partial writes, reconnection after EOF, stream backpressure, concurrent emits, oversized agent responses, or local socket trust. User-controlled payloads/exception strings must remain JSON-escaped and must not inject extra lifecycle records. Diagnostics should not dump configuration credentials, receipt handles, or entire job payloads by default.

**Evidence:** Laravel explicitly loops over partial writes, disconnects on failure, bounds repeated zero-byte writes, uses two-second connection/write timeouts, and reconnects on EOF (`LF:src/Illuminate/Foundation/Cloud/Events.php:71–106,117–125,141–186`); Symfony carries the same logic (`SOC:src/Observability/Events.php:71–181`). Agent HTTP is bound to a configured Unix socket at `LF:src/Illuminate/Foundation/Cloud/Queue.php:365–373`.

**Recommended fix:** Use bounded full-frame writes with synchronized access, short timeouts and reconnect on the next event; locally log degradation without recursive telemetry failure. Bound HTTP response sizes and disable unneeded redirect/proxy behavior. Treat socket paths and SQS endpoints as trusted deployment config, never payload data; use private temporary directories/permissions for test sockets and avoid deleting arbitrary existing paths. Document that Cloud failure records contain job data and exceptions, and keep secrets out of local diagnostic logs. Do not claim payload signing is an upstream protocol requirement; IAM/container/socket trust is the actual boundary shown here.

#### S-M4 — Framework-native typing needs downstream checks and a DI-aware callable model

**Scope:** S§5, S§7, S§10, S§17.

`mypy --strict` on the package does not prove that users get useful decorator signatures. A wrapper can remain internally strict yet expose `Callable[..., Any]`. A ParamSpec can preserve the original signature but cannot automatically subtract arbitrary `Depends` parameters for dispatch. The direct call may return an awaitable or ordinary result, while both dispatch methods return receipts. The queue/options parameter collision is recognized but not decided. Instance-bound methods, nested functions, callable objects, default names, and duplicate registration also need supported-surface limits.

**Upstream context:** The PHP implementation separates dispatch metadata from user message data through stamps (`SOC:src/Queue/Messenger/CloudQueueTransport.php:175–207`); its PHP type surface is not proof of a Python decorator contract.

**Recommended fix:** Choose the smallest public API that can type-check real downstream samples: typed job wrapper/decorator overloads and a separate bound options/builder surface if necessary. Define sync/async return preservation and which parameters are serialized versus injected. Add positive and expected-error mypy fixtures for direct calls, positional/keyword dispatch, options collisions (`queue`, `timeout`, etc.), wrong argument types, and optional dependency absence. Explicitly scope unsupported declaration shapes instead of accepting unstable wire names.

#### S-M5 — Python 3.10 compatibility affects more than the CI interpreter label

**Scope:** S§1, S§5, S§13, S§15, S§17.

UUIDv7 is required/preferred for failure identity, but Python 3.10's stdlib lacks `uuid.uuid7`; newer asyncio timeout/task-group APIs, `typing.Self`, modern type alias syntax, and exception-group syntax also cannot be assumed available. “Current supported FastAPI generation” may not remain compatible with Python 3.10. The scope does not fix dependency version floors/ceilings or the installation mechanism for standalone Pydantic support.

**Evidence/context:** Failure UUIDv7 is concrete upstream behavior at `LF:src/Illuminate/Foundation/Cloud/FailedJobProvider.php:70–75` and `SOC:src/Queue/QueueEventSubscriber.php:174–185`; Symfony explicitly declares its UID/runtime dependencies (`SOC:composer.json:6–18`). Python-specific reference: https://docs.python.org/3/library/uuid.html .

**Recommended fix:** Freeze supported Python minors and FastAPI/Pydantic/AnyIO/boto3 versions. Choose a reviewed small UUIDv7 dependency/backport or document a UUID-version deviation, with tests on 3.10. Use AnyIO/typing extensions where needed rather than accidental newer syntax. Define whether Pydantic support arrives via its own working extra or detection of an installed compatible Pydantic version. Test core-only import/install without FastAPI/Pydantic/OTel and a separate extras matrix.

#### S-M6 — Unknown config preservation needs an unsupported-feature policy

**Scope:** S§6, S§8, S§28–29.

The pinned Laravel baseline already implements `connection.after_commit`, `overflow`, and `credential_cache`; overflow is not merely hypothetical future work. Preserving unknown keys without using them is not equivalent to honoring them. An enabled unsupported setting could otherwise appear accepted while behavior differs.

**Evidence:** `LF:src/Illuminate/Foundation/CloudBootstrapper.php:223–238`; `LF:src/Illuminate/Queue/Connectors/SqsConnector.php:27–35`; overflow stores a cache-backed payload and sends `{"@pointer": ...}` at `LF:src/Illuminate/Queue/SqsQueue.php:537–567`, with hydration/deletion at `LF:src/Illuminate/Queue/Jobs/SqsJob.php:119–188`. This is a cache-store pointer implementation, not necessarily S3.

**Recommended fix:** Inventory recognized-but-unsupported keys. Decide which are harmless preserved metadata and which active settings require an actionable startup error or documented deviation. Explicitly scope transactional after-commit dispatch and overflow-pointer consumption out of v1 if that is intended. Keep oversize refusal and the future S3 roadmap, but accurately describe the pinned Laravel implementation as existing cache-backed overflow. No need to implement excluded offload merely for parity.

#### S-M7 — Best-effort failure reporting differs from guaranteed Cloud retry tooling

**Scope:** S§12, S§15, S§23, S§26, S§30.

Even after choosing full payloads (S-C2), “Cloud owns ... retry workflows” does not specify the mechanism that triggers a Python replay. Laravel's failed-job provider can fetch encrypted failed-job payloads from HTTPS URLs, negotiate `Cloud-Payload-Version`, paginate, and emit `retried_at`; normal Laravel retry CLI code depends on that provider. Symfony provides an envelope intended for verbatim platform requeue but does not establish a matching Python worker/CLI integration with Cloud's control plane.

**Evidence:** `LF:src/Illuminate/Foundation/Cloud/FailedJobProvider.php:121–177,183–199,208–225`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:402–412`; the Symfony truncation caveat at `SOC:src/Queue/QueueEventSubscriber.php:163–199`.

**Recommended fix:** Define whether v1 promises only failure ingestion and display, or also actual dashboard-triggered replay. Document who resends the raw envelope, what happens to delivery count/job UUID/FIFO metadata, and whether a Python retry command is required. Do not port Laravel's encryption/URL-fetch workflow without a confirmed platform requirement. Mark unverified Cloud control-plane operations pending while still testing replay of an intact locally captured envelope.

#### S-M8 — LocalStack and the canned agent fixture cannot certify the real platform

**Scope:** S§2, S§20–22, S§30.

The local emulator has to be stronger than the cited Symfony fixture. That fixture responds 200 to GET with a canned body, responds 200 to other requests, reads the HTTP head, and does not implement result validation, message holding, receive-count evolution, heartbeat, state, or visibility. Symfony retry tests synthesize receive counts and inspect mocked AWS commands. Those tests are valuable client checks but do not prove SQS redelivery, server-side fairness, or Cloud collector/dashboard behavior.

**Evidence:** `SOC:tests/Fixtures/agent-server.php:33–70`; `SOC:tests/InteractsWithRetryTransport.php:46–55,80–110`; `SOC:tests/CloudRetryIntegrationTest.php:39–62`; fair-queue dispatch only sets an attribute at `SOC:src/Queue/Messenger/CloudQueueTransport.php:224–230`.

**Recommended fix:** Give the Python emulator a small explicit state model and independent protocol fixtures; use its fault injection to test an actual worker subprocess. Freeze LocalStack version/capabilities and distinguish AWS-argument conformance from real scheduling behavior. A fair-group attribute test may pass while server-side fairness remains unverified. Keep live Cloud skipped as requested; report evidence tier (`unit`, `socket`, `emulated SQS`, `live`) and do not infer a platform guarantee from a mock. Do not implement unspecified proprietary agent internals as if observed.

#### S-M9 — Release status rules permit mandatory features to be explained away

**Scope:** S§15, S§21–22, S§26, S§30.

The report permits `partial`, `unsupported`, and `skipped`, but acceptance does not define which statuses are allowed for mandatory features or what a missing feature record means. Dashboard parity is a release blocker, yet live verification is excluded. Without a fixed manifest and evidence levels, a team can produce a green report with fewer probes or mark a required feature partial and still claim completion.

**Evidence/context:** The upstream tests distinguish actual socket capture from mocked retry commands (`SOC:tests/QueueEventSubscriberTest.php:145–204`; `SOC:tests/InteractsWithRetryTransport.php:80–110`). They do not supply this project's release policy.

**Recommended fix:** Require one catalog record for every mandatory scope capability, reject duplicate/missing IDs, and make report exit status enforce the approved status for each feature. Only listed exceptions, such as `cloud.live`, may be skipped. Define “dashboard parity” now as pinned wire-contract conformance, with actual platform ingestion/replay pending. Do not make live Cloud a new current release gate or permit explanations alone to waive required local behavior.

#### S-M10 — Packaging tests must verify an isolated installed product, not only archive contents

**Scope:** S§4–5, S§18–19, S§23, S§26.

Excluding `demo/` is clear, but `laravel-cloud-queues conformance` is a product CLI family while demo/dev-only infrastructure must not ship. The location of the usable catalog/report runner is undecided. Package smoke tests can falsely pass by importing the checkout or using editable installs. A wheel-only test will not expose an sdist that cannot build a complete wheel, and a present `py.typed` marker does not establish usable installed type exports.

**Evidence/context:** Symfony separates production and test autoload/dependencies in `SOC:composer.json:17–28`; PHP sources cannot specify Python build-backend behavior. This is a Python packaging gap relative to the scope, not an upstream protocol contradiction.

**Recommended fix:** Choose one build backend and explicit file inclusion rules. Build wheel and sdist, rebuild/install from sdist in a fresh environment outside the repository, and test core plus FastAPI installs, console entry point, metadata/license, public exports and `py.typed`. Decide whether installed conformance runs packaged lightweight probes or gives clear repository-tooling instructions. Do not leave a shipped CLI importing excluded `demo`/tests. Keep dev-only dependencies out of core and do not expose empty Django/Flask extras.

#### S-M11 — CI matrix and reproducibility requirements are not operationally defined

**Scope:** S§2, S§5, S§17, S§22, S§27, S§30.

“Python 3.10+ matrix” is open-ended; Linux/macOS support is promised without saying which OS runs subprocess/signal/Unix-socket proofs. There is no pinned LocalStack image, supported FastAPI dependency range, minimal/latest dependency lane, offline baseline-fixture policy, deterministic clock/test timeout scheme, or artifact collection requirement. Upstream drift must remain separate from release conformance and must not rewrite golden expectations.

**Evidence/context:** Symfony explicitly enumerates major dependency compatibility in `SOC:composer.json:6–18`; its socket fixture has finite accept/termination behavior (`SOC:tests/Fixtures/agent-server.php:33–38,70–74`). These are useful precedents, not a Python CI specification.

**Recommended fix:** Enumerate currently supported stable Python minors, OS lanes, dependency bounds, and where each required gate runs. Pin emulator images and fixture checksums/commits; make missing Docker/services a gate failure in the required integration job rather than a silent skip. Upload conformance JSON and process/socket logs on failure. Isolate credentials/network from unit tests. Give a named owner/schedule to drift detection and record changed source paths for both pinned projects; baseline changes require reviewed fixture/doc updates. Avoid unnecessarily multiplying every OS × Python × dependency × infrastructure combination.

### Low

#### S-L1 — Explicitly distinguish supported hosted semantics from the entire Laravel queue surface

**Scope:** S§3, S§12–13, S§23, S§28.

Broad “match Laravel” language could pull in unrequested features: memory-based worker recycling, queue size/purge/inspection, aliases/forwarding, after-commit dispatch, interrupt callbacks, retry deadlines, exception counters, and failed-job CLI APIs. Some are absent; some are ambiguous; none should be silently assumed implemented. `batches` in the exclusion list also does not clarify Laravel job orchestration batches versus the transport's `SendMessageBatch` API.

**Evidence:** `LF:src/Illuminate/Queue/WorkerOptions.php:107–119`; `LF:src/Illuminate/Queue/Worker.php:419–429,983–1001`; `LF:src/Illuminate/Queue/SqsQueue.php:361–384,665–677`; `LF:src/Illuminate/Queue/QueueRoutes.php:93–103`.

**Recommended fix:** Add a supported/deferred surface table. Clarify `inspect` as registry/config inspection versus destructive or provider queue operations, and explicitly defer bulk dispatch/advanced worker controls unless needed. “Laravel operational compatibility for the listed features” is a testable claim; “all Laravel semantics” is not.

#### S-L2 — Upstream comments are demonstrably stale; code/test precedence should be explicit

**Scope:** S§2 and S§22.

The scope correctly says inspect code, but the precedent itself contains outdated prose. Symfony's transport class comment says it chooses the agent by socket presence, while the config/factory actually use the injected flag. Its fair stamp comment mentions retries by sending a copy, while `send()` releases the original message. These comments can mislead research agents or generate incorrect conformance expectations.

**Evidence:** `SOC:src/Queue/Messenger/CloudQueueTransport.php:24–26` versus `SOC:src/Queue/ManagedQueueConfig.php:186–198` and `SOC:src/Queue/Messenger/CloudQueueTransportFactory.php:45–55`; `SOC:src/Queue/Messenger/CloudMessageGroupStamp.php:27–29` versus `SOC:src/Queue/Messenger/CloudQueueTransport.php:151–170`.

**Recommended fix:** Record code > executed path > tests > comments/docs as the evidence hierarchy, while remembering tests may encode a limited or incorrect service assumption. Date claims such as “latest 13.x release”; the verified compatibility promise is the pinned tag, not an evergreen “latest” claim.

## Laravel versus Symfony — differences that must not be merged into one imaginary baseline

| Topic | Laravel at the pin | Symfony at the pin | Recommended Python decision |
|---|---|---|---|
| Malformed config | Throwing JSON decode | Treats malformed JSON as unconfigured | Keep scope's fail-fast behavior; S-H1 |
| Queue defaults | Sender uses nested queue; worker fallback top-level/CLI | Nested queue → top-level → default | Freeze role-specific precedence; S-H1/H4 |
| Queue URL | Full URL pass-through; idempotent suffix finish | Literal builder, no URL pass-through | Laravel URL fixtures or explicit restricted surface; S-H3 |
| Agent selection | Enabled + worker command + matching worker queue | Enabled flag; ignores requested queues | Explicit Python hosted-assignment policy; S-H4 |
| GET retry | `[0,500]` retry schedule | Single request | Choose and test; S-H5 |
| POST errors | Connection-only bounded retry; 5xx agent exception; other request errors separate | Same connection retry shape; 5xx transport error; 4xx runtime error | All-fatal is a deliberate stronger policy; S-H5 |
| Missing message ID | No work | No work | Scope's strict fatal parsing is a deviation; S-H5 |
| Missing receive count | Direct attribute cast/access | Defaults/clamps to at least 1 | Define malformed-count behavior; S-H7/H10 |
| Default attempts | 1 total try | Messenger defaults: 3 retries (4 deliveries) | Keep Laravel's 1, not Symfony's default; S-H7 |
| Backoff | Job/worker integer/list, last entry repeats; no SQS clamp in release | Messenger backoff; ceil ms retry delay; 12-hour clamp | Document combined Python policy; S-H7/H8 |
| Fresh FIFO delay | Omits DelaySeconds | Rejects positive delay | Keep loud rejection as intentional tightening; S-H13 |
| Fresh fractional delay | Integer/date-based queue helper path | Floors ms before cap validation | Freeze Python rule; S-H13 |
| Group/dedup controls | Same group concept for standard/FIFO; empty dedup may omit | Distinct stamps with cross-model rejection; generated fallback dedup | Typed Python options with explicit content-based decision; S-H14 |
| Retryable timeout | Emits released telemetry, kills; no release call in timeout handler | This adapter does not implement Laravel's alarm/exit behavior | Separate timeout policy from normal retry; S-H6 |
| Failed events | Full payload, preview/name, failure record then lifecycle completion | Trimmed payload/exception, missing preview/name, lifecycle first | Laravel wire schema plus explicit collector limitation; S-C2/H9 |
| Direct receive | Requests attempt attribute; no explicit 20s wait in this call | One message, 20s wait, attributes | Long polling is a Python/Symfony choice; S-H8 |
| Queue not found | Dedicated translation for `AWS.SimpleQueueService.NonExistentQueue` | Generic transport wrapper | Keep dedicated Python error; validate botocore error aliases; S-H1/H15 |
| Fatal agent exit | Lost-connection path can exit success (`Worker.php:422`) | Transport exception intended to terminate consumer | Specify Python unhealthy exit code, not vague parity |

Sources for the final two rows: `LF:src/Illuminate/Foundation/Cloud/QueueConnector.php:68–85`; `SOC:src/Queue/Messenger/CloudQueueTransport.php:468–474`; `LF:src/Illuminate/Queue/Worker.php:419–423`; `SOC:src/Queue/Agent/AgentClient.php:33–38`. All other rows are supported by the cited findings above. Symfony default retry prose is at `SOC:README.md:171–190`, with the 3-retry/4-delivery behavior exercised in `SOC:tests/CloudRetryIntegrationTest.php:30–62`.

## Part 2 — AGENT_BUILD_PROMPT.md gaps and one-pass build readiness

The build prompt already says the scope is authoritative (`AGENT_BUILD_PROMPT.md:7`). Therefore a missing repeated bullet is **not** permission to omit that feature. The findings distinguish missing reminders/proof obligations from genuinely unresolved decisions and coordination gaps. Features explicitly excluded by S§28 are not being reintroduced.

### Build section index used in citations

| Audit section | Actual heading | Starting line |
|---|---|---:|
| B§1 | Mission | 9 |
| B§2 | Upstream baseline | 17 |
| B§3 | Non-negotiable engineering constraints | 55 |
| B§4 | Repository deliverables | 77 |
| B§5 | Required architecture | 97 |
| B§6 | Job API | 168 |
| B§7 | Serialization | 202 |
| B§8 | Queue semantics | 234 |
| B§9 | Retry and failure policy | 272 |
| B§10 | Worker model | 299 |
| B§11 | Timeout behavior | 348 |
| B§12 | Observability | 363 |
| B§13 | Tracing | 385 |
| B§14 | Eager/application testing mode | 395 |
| B§15 | Local infrastructure and conformance | 412 |
| B§16 | `demo/` compatibility application | 441 |
| B§17 | CLI | 488 |
| B§18 | README is mandatory | 500 |
| B§19 | Scope exclusions | 531 |
| B§20 | Suggested multi-agent ownership | 552 |
| B§21 | Execution sequence | 587 |
| B§22 | Release acceptance gate | 665 |

### Critical

#### B-C1 — The architecture phase can finish without resolving the two feasibility blockers

**References:** B§21 step 2 (lines 595–601), B§10–12; S§13–15 and S§30 items 4, 12, 14.

The architecture checklist names interfaces, config, envelope, exceptions and feature IDs, but does not require a proven hard-timeout topology or a decision on replayable failure records versus the collector workaround. All three team members could implement internally consistent pieces that cannot meet the combined scope. These are the inherited blockers S-C1 and S-C2, not two new unrelated features.

**Recommended fix:** Before parallel implementation, require a small subprocess timeout proof and a golden failed-job/replay fixture, plus a written decision on report ownership, teardown after kill, supported dashboard claims, and trimming. Do not accept a diagram or `asyncio` cancellation test as timeout proof. Allow approved project deviations to be recorded explicitly while preserving the current exclusion of live Cloud verification.

### High

#### B-H1 — Nonblocking SDK isolation and sync/async dispatch equivalence disappeared from the execution checklist

**References:** B§3 boto3/AnyIO bullets, B§5 “SQS transport”, B§6 sync/async support; S§9 and S§11 “Dispatch path” (lines 404–409, 483–487).

The scope explicitly requires async APIs to isolate blocking boto3 calls and both dispatch paths to share validation, serialization, routing, tracing, and transport semantics. The build prompt names boto3 and both APIs but omits that invariant. Merely making `dispatch_async` an `async def` can block FastAPI under load, and independent sync/async implementations can diverge.

**Recommended fix:** Add an explicit offload/shared-pipeline task and event-loop-responsiveness acceptance test. Include SDK credential resolution and telemetry I/O, not just `send_message`. Define sync dispatch/eager behavior when called under an active event loop (S-M1).

#### B-H2 — The collision-free, statically useful dispatch API requirement is omitted

**References:** B§6 and B§21 step 2; S§5 “Typing”, S§7 direct callability, S§10 lines 436–446, S§17 lines 821–840.

B§6 requires queue override and job kwargs but loses S§10's warning that transport keywords must not collide with handler parameters. It also lacks the scope's useful downstream decorator/generic signature requirements. Each implementer can choose a different wrapper/options shape, and all may pass internal strict mypy while breaking downstream applications or FastAPI injection.

**Recommended fix:** Freeze exact public decorator, direct call, dispatch, options, `JobContext`, and registry interfaces before splitting work. Require mypy samples proving a handler may legitimately take `queue`, `timeout`, or `delay` as data. Decide which dependency parameters appear in each callable signature; do not fake type safety with broad casts or `Any`.

#### B-H3 — Retry details and transport completion rules are too compressed for independent implementations

**References:** B§5 agent/SQS subsections, B§9, B§11; S§11–14 and S§30 items 6, 8–11, 13–14.

B§9 omits an explicit scalar/list backoff contract and precedence, despite the list example in S§12. B§5 does not spell out direct success deletion, terminal deletion, receive long polling, or required fatal malformed-agent-response behavior from S§11. It calls release delay “optional” without freezing null versus zero. The prompt carries forward “retryable timeout releases” without separating the timeout telemetry issue. Bounded agent retries are named but not made exact.

**Recommended fix:** Add the chosen delivery/policy transition table and protocol fixtures as mandatory shared inputs, with cases from S-H5–H8 and S-H15. Test same message ID **and latest receipt handle**, receive-count increments, no send on visibility retry, no duplicate terminal operations, pre-run budget exhaustion, report ambiguity, and final-attempt timeout. Mark deliberate upstream deviations, especially 4xx fatality and timeout release timing.

#### B-H4 — The conformance checklist is a subset of the mandatory demo matrix

**References:** B§15 proof list (lines 424–439), B§16, B§21 step 7; S§20–21 and S§30.

“Implement every required feature probe” relies on readers remembering a separate long list. The build prompt's enumerated proofs do not explicitly cover all S§21 required cases, including:

- both sync and async handlers; direct decorated callability;
- named queue and per-dispatch override; a valid delayed standard send;
- explicit `JobContext.release()` and `fail()`;
- default `tries=1` and retry exhaustion as separate probes;
- fail-on-timeout separately from retryable timeout;
- valid FIFO group/dedup and valid standard fair grouping, in addition to invalid combinations;
- the typed queue-not-found error;
- failed-job schema/replay evidence separately from a generic lifecycle event;
- optional tracing enabled **and absent**, plus isolation between jobs;
- eager mode preserving payload validation and DI semantics.

The prompt does mention several of these elsewhere as features, and B§16 does require separate producer/worker processes. The gap is traceable acceptance coverage, not absence of every feature from the whole document.

**Recommended fix:** Turn S§21's entire list into the initial catalog with IDs and owners before coding. Link every S§30 item to at least one test/probe. Extend fault cases using Part 1 without substituting them for happy-path coverage. Fail CI for a missing record; keep `cloud.live` explicitly skipped.

#### B-H5 — The release gate allows explained failures in required features

**References:** B§22 line 681 “no unexplained conformance failure remains”; B§16 statuses; S§15 dashboard blocker, S§22 methodology, S§30 all acceptance items.

An explained `fail`, or an `unsupported`/`partial` label on a required feature, can satisfy that sentence without satisfying the authoritative scope. Nothing explicitly prevents absent IDs, a report generated from stale evidence, or a local stub result labeled as live service parity.

**Recommended fix:** Require all mandatory local features to pass at their declared evidence tier; allow non-pass statuses only for a named, reviewed exception/deferred-feature list. Make report generation record tested revision/environment and return nonzero for missing/unapproved non-pass results. Do not allow an implementation agent to edit catalog expectations solely to turn its tests green. Resolve the local-wire-versus-live-dashboard distinction from S-M9.

#### B-H6 — Multi-agent roles overlap, but concrete artifact ownership and handoffs are missing

**References:** B§20; B§21 steps 1–10; S§4, S§22, S§30.

Codex owns nearly all implementation and tests; Grok also builds conformance tests; Claude proposes public architecture and reviews docs. There is no single owner for emulator state, observability collector, golden fixtures, CI/drift workflows, README final assembly, dependency choices, package export surface, or the final integration commit. “All agents review one another” does not specify what evidence a reviewer must inspect or who resolves a disagreement. Shared-file collisions and incompatible transport/delivery interfaces are predictable.

**Recommended fix:** Assign one writer per artifact/module and a named integration owner, with explicit handoff outputs and dependencies. Freeze a small shared contract pack (config model, delivery record, envelope, policy/state machine, public errors, event fixtures, CLI targets, feature IDs). Reviewers can propose changes but should not concurrently rewrite an owner's shared files. Require source-backed review of modules not authored by that reviewer and a single clean full-gate run after integration. The roles are responsibilities; execution should not depend on every named vendor/model being available.

#### B-H7 — Observability requirements omit the most important field and workaround decisions

**References:** B§12 lines 375–383, B§21 steps 1–2; S§15 lines 743–782, S§8 metadata.

The build prompt reduces the failed-job contract to “data sufficient ... inspect exact ... fields.” It drops S§15's explicit queue normalization, microsecond timestamp/duration requirements, field inventory, short persistent-socket behavior, local degradation logging, and requirement to document Symfony's temporary workaround. B§7 also does not freeze the dashboard-significant spelling `displayName`. Different agents can build a producer envelope, worker event, and collector that agree with each other but disagree with Laravel.

**Recommended fix:** Make exact event/envelope fixtures and socket behavior part of the contract pack, owned jointly through review by the transport and conformance leads. Add a release check for Laravel-required failed-job fields, raw payload string preservation, UTC microseconds, event sequencing, partial writes/reconnect, and the chosen collector-limit behavior. Treat dashboard replay as its own documented verification status (S-C2, S-H9, S-M7).

### Medium

#### B-M1 — Several explicit scope details are omitted or weakened, even though the scope remains authoritative

**References and fixes:**

| Build location | Scope requirement not repeated clearly | Recommended addition |
|---|---|---|
| B§5 configuration | S§6: supported drivers/required-field validation, local explicit env/settings, never auto-create hosted queues | Add precedence/support table and explicit no-provisioning acceptance check |
| B§6 job API | S§7: explicit names are preferred across module refactors; no required recursive scanning | Add registration/discovery behavior and duplicate-name tests/documentation |
| B§7 serialization | S§8: explicit JSON/list/dict/tuple round trips and pre-execution callable argument/schema validation | Name the supported codec/annotation matrix; binding alone is insufficient |
| B§14 eager mode | S§20 explicitly requires payload validation, not only binding/serialization | Require eager mode to use the real validation/codec path |
| B§13 tracing | S§16: tracing failure must not break execution | Add nonfatal absence/failure tests and context reset after each job |
| B§17 CLI | S§23: actionable package-level errors instead of default raw SDK/HTTP tracebacks | Define exit codes and user-facing diagnostics, with optional debug tracebacks |
| B§7/B§18 roadmap | S§8: offload hydration **and cleanup**, configurable thresholds | Retain complete roadmap wording without implementing it in v1 |
| B§4/B§19 adapters | S§4: do not expose nonfunctional extras, not just incomplete adapters | Test metadata does not advertise empty Django/Flask extras |

These are checklist omissions, not new scope requests. Tie each addition to its cited scope section rather than duplicating the full scope in prose.

#### B-M2 — Vanilla worker bootstrap and installed CLI/conformance ownership are undefined

**References:** B§6 standalone registry, B§10 app target, B§17 CLI, B§4 demo exclusion; S§18, S§23, S§4.

All concrete worker targets are FastAPI `module:app`. The prompt does not define what a standalone registry target exports, whether factories are supported, startup/shutdown hooks for vanilla mode, or how `inspect` behaves without opening credentials/network connections. It also does not resolve how an installed conformance command works when the demo/emulators are deliberately excluded.

**Recommended fix:** Specify one minimal core target protocol/object and a working vanilla script example with no FastAPI import. Decide installed-versus-repository conformance behavior and keep CLI errors actionable when optional tooling is absent. Test console commands from an isolated wheel installation, not an editable checkout.

#### B-M3 — Public errors have no complete shared inventory or classification owner

**References:** B§5 typed errors, B§21 step 2 “exception hierarchy”; S§24 and S§8, S§11–12.

The build prompt explicitly names only some errors and leaves transport/protocol/config/serialization/schema/unknown-job/explicit-failure families to independent implementers. It does not define which errors are terminal job defects, retryable handler failures, fatal worker failures, or caller dispatch errors. Shared catch clauses can swallow control-flow signals or classify an acknowledgement problem as a handler failure.

**Recommended fix:** Translate S§24's inventory into a compact shared hierarchy/classification table before implementation. Define exception chaining without exposing secrets and when SDK errors are wrapped. Include `ManagedQueueNotFoundError` and `PayloadTooLargeError` stable fields; test invalid options, unsupported envelope versions, protocol errors, and explicit controls. Reuse this table in CLI, eager mode, worker, and transport code.

#### B-M4 — Build and dependency validation gates need concrete commands and environments

**References:** B§3, B§21 steps 3 and 9, B§22; S§4–5, S§17, S§26–27.

The prompt repeats the major gates but not their runnable commands, Python minors, OS/dependency lanes, lower-bound compatibility, or artifact isolation. It tests `py.typed` presence but not downstream use; build/install can mean an editable checkout. No gate prevents FastAPI, Pydantic, OTel, test tools, or emulator dependencies from leaking into core imports/requirements.

**Recommended fix:** Add a minimal command matrix and artifact checks from S-M5, S-M10 and S-M11. Include core-only installation, FastAPI extra, optional tracing/Pydantic paths, sdist-to-wheel installation, actual CLI invocation, and downstream mypy fixtures. Keep pinned dev/CI inputs separate from reasonable library dependency ranges. Specify 0.x metadata/public-module policy from S§26; do not automate publication merely because building succeeds.

#### B-M5 — Long-lived dependency/resource/context isolation is not an explicit acceptance item

**References:** B§10 FastAPI/shutdown, B§13, B§14; S§13, S§16–17, S§20.

Lifespan/DI cleanup is named, but a single successful job cannot prove per-job isolation. Missing are two sequential jobs with distinct `JobContext`/trace/dependency state, dependency teardown failure before acknowledgement, startup failure, clean shutdown ordering, and direct call/eager behavior for DI-bearing jobs.

**Recommended fix:** Add a small two-job integration scenario and the failure-path variants: lifespan starts once and stops once on clean exit, per-job yield dependencies close each time, context resets after success/release/failure, request-only injection fails clearly, and app state remains available. Run it through a real worker target with separate producer/worker processes. Define hard-kill exceptions to cleanup guarantees rather than pretending teardown is always possible.

#### B-M6 — Drift checking and reproducible independent evidence have no assigned deliverable

**References:** B§2 line 53, B§20 roles, B§21 steps 1 and 9, B§22; S§2 upstream drift and S§22 catalog.

Drift detection is required in the body but omitted from the final gate and concrete ownership list. No workflow path, schedule/manual command, dependency snapshot, source hash, or report output is specified. The independent reviewer can also end up validating an emulator and Python client built from the same mistaken assumption.

**Recommended fix:** Assign ownership of drift workflow and baseline fixtures. Require pinned upstream revisions in the catalog and known-good request/event fixtures taken independently from code/tests, not generated only by the Python implementation. Gate presence and deterministic operation of the checker, while keeping new-upstream detection advisory until a reviewed baseline update. Have the reviewer trace at least the highest-risk expected results back to source; retain evidence artifacts.

### Low

#### B-L1 — “Produce PROJECT_SCOPE.md” risks accidental scope rewriting

**References:** B§4 deliverables, B§21 step 3 line 610, introductory line 7; S§22 “must not mutate expectations ... to make ... pass”.

The repository already contains the authoritative scope. Listing it among scaffold outputs without a preservation rule invites a team to regenerate a smaller scope matching what it implemented.

**Recommended fix:** Say “retain the existing authoritative scope; propose explicit reviewed amendments for resolved ambiguities.” Maintain a decision/deviation record with source evidence rather than silently overwriting acceptance requirements.

#### B-L2 — Stable report schema/version and section references would reduce coordination errors

**References:** B§16 lines 456–472, B§20–22; S§21 report fields and S§22 catalog.

The report specifies allowed statuses, but its per-record field list does not explicitly include `status`; schema version, top-level environment/run metadata, evidence artifact paths, and validation rules are not fixed. The prompt itself has no section numbers despite being used as a coordination/acceptance document.

**Recommended fix:** Version the report/catalog schema, make `status` mandatory per feature, validate it, and distinguish run-level metadata from feature evidence. Add stable section/requirement IDs or keep an external traceability table. This can be a small JSON schema plus the feature manifest, not a new reporting framework.

## Minimal decisions and artifacts needed before the one-pass build

1. **Contract pack:** supported config/credential forms, role-specific precedence, URL examples, exact agent request/error matrix, delivery state machine, retry/timeout policy, and explicit upstream deviations.
2. **Runtime proof:** chosen supervisor/executor topology and subprocess tests for hard native/sync timeout, outcome ownership, shutdown races, and clean versus forced cleanup.
3. **Public API/serialization proof:** downstream typing samples, collision-free options, DI-versus-payload parameters, safe registered codecs, versioned envelope including dashboard identity fields.
4. **Failure/telemetry proof:** exact Laravel JSON fixtures, intact replay fixture, collector-size decision, best-effort loss statement, and honest live-platform limitations.
5. **Feature/ownership matrix:** every S§21 probe and S§30 acceptance item mapped to an owner, source/decision, runnable test, allowed status, and independent reviewer.
6. **Release commands:** concrete Python/OS/dependency gates, pinned LocalStack/emulator inputs, isolated wheel/sdist verification, report artifacts, and a separate advisory drift workflow.

These decisions are prerequisites to implementing the already requested system. They do not require adding PHP payload compatibility, Django/Flask implementations, a production failure database, general middleware, internal multi-job concurrency, compression, or S3 offload.

## Appendix — Python signal observation

Executed in an isolated child process under an outer 15-second subprocess timeout. No repository file was created:

```python
import hashlib
import signal
import time

started = time.monotonic()

def alarm(*_):
    print(time.monotonic() - started, flush=True)

signal.signal(signal.SIGALRM, alarm)
signal.setitimer(signal.ITIMER_REAL, 0.020)
hashlib.pbkdf2_hmac("sha256", b"password", b"salt", 3_000_000)
```

Observed callback time: approximately **0.202 seconds**, requested time: **0.020 seconds**. Exact durations are machine-dependent; the observation demonstrates that native work can defer a Python signal callback. Supplementary language reference: https://docs.python.org/3/library/signal.html . This is not a benchmark or a proof of any proposed worker implementation.

## Audit conclusion

The scope is broad and already captures many important invariants: one job per worker, SQS-native visibility retries, default one try, no unsafe pickle, authoritative agent enablement, fatal unavailable agent reporting, nonfatal telemetry, strict typing, and exclusion of demo artifacts. The principal risks are unresolved contracts and false equivalence between Laravel, Symfony, and what local emulators can prove. Resolve the critical process/replay decisions, freeze the high-risk contracts, and make the build prompt enforce a complete source-linked feature matrix before claiming the requested release-quality system is complete.
