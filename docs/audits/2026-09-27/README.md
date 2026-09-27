# Cross-lab audit — 2026-09-27

Independent audits of `PROJECT_SCOPE.md` and `AGENT_BUILD_PROMPT.md` against the pinned upstream sources:

- `laravel/framework` v13.33.0 (commit `91188a17ceaa3dbace6e8a5f7abd0d042e466359`)
- `laravel/symfony-on-cloud` 50c945170b6cb5690370d15fd725c6f82495e9ba

Neither audited document has been edited yet. This file is the merged, deduplicated summary. It was revised after Astra's adversarial verification (`verification-by-astra.md`); corrections are listed at the end.

## Reports

| File | Auditor | Harness | Notes |
|---|---|---|---|
| `claude-opus-5.5.md` | Claude Opus 5.5 | Claude Code (orchestrator) | Has an errata section from Astra's verification |
| `grok-4.7-high.md` | Grok 4.7 High | Cursor Agent via Solo | 27 scope + 13 prompt findings |
| `codex-gpt-6-astra-high.md` | GPT-6-Astra High | Codex via Solo | 46 findings; its 105 citations passed a file/line-bounds check (structural only, not semantic); includes a Python signal-latency experiment |
| `gemini-3.1-pro.md` | Gemini 3.1 Pro Preview | OpenCode Zen via Solo | Several invalid citations and wrong claims; see caveats |
| `deepseek-v4-pro.md` | DeepSeek V4 Pro | OpenCode Go via Solo | Several wrong claims; see caveats |
| `verification-by-claude.md` | Claude Opus 5.5 | — | Checks claims raised by only one or two auditors |
| `verification-by-astra.md` | GPT-6-Astra High | Codex via Solo | Adversarial check of Claude's report and this README |
| `platform-findings.md` | Claude Opus 5.5 | Live Laravel Cloud probe + CLI | Python containers get no queue config, agent or AWS credentials; the API refuses managed queues for FastAPI |

Kimi K3 was launched but hit its provider usage limit before writing a report and was dropped.

Auditor tags: **C** Claude, **G** Grok, **X** Codex, **M** Gemini, **D** DeepSeek. A tag means the auditor **raised the topic**; where auditors recommended different fixes, the bullet says so. "Editorial" marks a resolution proposed in this summary that no single auditor fully specified.

## Verified caveats about individual reports

- **Gemini:** line citations `CloudBootstrapper.php:1558`, `Events.php:1229`, `SqsQueue.php:2362` are out of range (files are 330, 234, 723 lines). `credential_cache` keys are wrong (actual: `enabled`, `store`, `fallback_store`). It wrongly says the build prompt omits exit code 124 (`AGENT_BUILD_PROMPT.md` Timeout section includes it) and wrongly says `/result` 4xx is terminal.
- **DeepSeek:**
  - H1 is wrong: it claims SQS is limited to 256 KiB and Laravel's 1 MiB constant is overflow-only. Pinned source documents `MAX_SQS_PAYLOAD_SIZE = 1048576` as the maximum SQS payload size and also uses it for batch sizing.
  - Its Symfony failed-job field list omits `_cloud_event` and describes 4,000 bytes as a final exception bound (it is 4,000 bytes plus a truncation marker).
  - It says the build prompt never states Laravel is canonical (it does, in "Upstream baseline").
  - It says `demo/` exclusion is missing from the release gate (it is present).
- **Duration rounding:** Grok says truncate (Laravel); DeepSeek says `max(0, round())` (Symfony). Editorial recommendation: truncate like Laravel and clamp at 0.

## Open decisions (block editing the docs)

1. **Failed-job event: size, replay, and durability.**
   - The reports agree there is a schema mismatch. G, X and D list Laravel's full field set: `_cloud_event`, `id`, `queue`, `started_at`, `attempts`, `payload`, `exception_preview`, `job_name`, `exception`. C and M argue for keeping `job_name`/`exception_preview`.
   - Size positions: G and M favor Symfony-style trimming (documented rationale: a 16 KiB log-line limit in the collector; the deployed collector itself is not in these repos). D favors Laravel's full event. C and X note trimming breaks replay.
   - Symfony's trim helper keeps only `uuid`, `displayName`, `body`, `body_truncated`. Applied to a Python envelope it would drop args, kwargs, version and context **even for small messages** (X). Do not copy it as a generic byte-budget helper.
   - Failed-job records can be lost even when they fit: event writes are best-effort, and Laravel deletes the message before the failure record is written (X). With no Python failed-job store, a telemetry outage loses the record.
   - Options:
     - (a) Trim; dashboard retry is lossy.
     - (b) Keep the full payload; large failures may never reach the dashboard.
     - (c) *(Editorial recommendation)* Keep the full payload when the whole line fits; trim the exception first; mark oversized failures as not replayable. Document the best-effort loss boundary either way.
2. **Timeout enforcement mechanism** (X M G D C raise it; they propose different mechanisms).
   - X's experiment: a 20 ms `SIGALRM` handler ran at ~202 ms because native code delayed Python's handling of the signal.
   - Proposals differ: X suggests a supervisor watching one long-lived executor process; G a child process per job; M a process pool or threads; D asks for a decision.
   - *Editorial recommendation, informed by X:* a supervisor process watches one long-lived executor that owns the app lifespan and runs one job at a time. The supervisor kills on timeout and exits 124. Kill/join, outcome-reporting ownership and race handling must be decided alongside.
3. **Failure display only, or real dashboard retry?** (X C G). The claim that Cloud re-queues failed payloads verbatim comes from a Symfony source comment, not from any code or test here. G recommends forbidding a port of `FailedJobProvider`'s fetch workflow; X says not to port it without a confirmed platform requirement.
4. **Retry policy location** (G X). Snapshot into the message at dispatch (Laravel stores `maxTries`, `backoff`, `timeout`, `failOnTimeout`, `retryUntil`, `maxExceptions` in the payload) or use worker-side configuration (Symfony uses Messenger's retry strategy config).
5. **Dispatch API shape** (G X D). Options object or builder, so `queue`, `delay` and `timeout` never collide with handler parameters.

## Upstream corrections to the scope

These describe pinned upstream behavior. Whether Python copies each one is noted where auditors differ.

- **`/result` errors** (C G X D).
  - A 4xx raises a non-agent exception in both upstreams (Laravel `RequestException`, Symfony `RuntimeException`), not the fatal lost-agent path. Symfony's comment says the consumer "reports it and moves on"; no test proves end-to-end continuation.
  - Connection failures get 3 attempts, 100 ms apart, 10 s timeout per attempt. A 5xx is fatal without retry. Symfony also wraps any other Guzzle error as a fatal `TransportException`.
  - When Laravel stops for a lost agent connection it exits 0.
  - G and X both allow keeping the stricter "every report failure is fatal" rule if labeled a deviation.
- **`GET /next`** (C G X).
  - Laravel makes up to **3 attempts** (retries after 0 ms and 500 ms). Symfony makes one.
  - Fatal: unreachable socket, status other than 200/204, or a body that does not decode to a PHP array. Note a JSON array such as `[]` passes that check.
  - A 200 whose `messageId` is missing, empty or not a string is an empty poll. The worker then follows its normal lifecycle options (for example, stop-when-empty).
  - Non-string `receiptHandle` becomes null; non-string `body` becomes `""`.
- **Timeouts** (C G X).
  - Laravel's timeout path never calls `release()` or applies backoff, and never posts `released` to the agent.
  - Order: failure checks run first (attempts exhausted, `retryUntil` expired, `maxExceptions` reached, or fail-on-timeout), then the worker is killed.
  - The lifecycle event is `failed` if the job was failed, otherwise `released`.
  - Cloud configures exit 124; if the exec hook does not replace the process, the worker falls back to SIGKILL.
  - A failed job is deleted, which the agent receives as `processed`. Later redelivery of an unfailed job depends on agent/SQS recovery outside these repos.
- **Envelope keys** (C G X D). Both upstreams write top-level `uuid` and `displayName`; Laravel derives `job_name` from `displayName`. Recommendation: Python uses the same keys.
- **Config** (C G X M D raise it; fixes differ).
  - Laravel's bootstrapper adds `connection.after_commit`, `connection.overflow` and `connection.credential_cache`. Laravel actively uses all three, including cache-backed overflow with an `@pointer` body that the worker hydrates.
  - Fixes differ: G ignores `after_commit`/`overflow` and implements or defers `credential_cache` separately. X requires a policy for active-but-unsupported settings (startup error or documented deviation). D preserves them as unknown fields. M recommends implementing caching.
  - *Editorial:* v1 must not silently accept an **active** unsupported setting. Treat an incoming `@pointer` as unsupported by v1 policy (it is valid upstream).
  - `queues` may be a list or a map (Laravel inventory, not a dispatch allowlist).
  - Malformed Cloud JSON throws in Laravel and becomes "unconfigured" in Symfony; keep the scope's fail-fast rule.
  - Queue defaults differ by role: Laravel's sender uses `connection.queue`; its worker uses `--queue`, then top-level `queue`, then `default`. Symfony uses `connection.queue`, then `queue`, then `default` (X).
- **Credentials** (G X D; M on caching only).
  - `credentials: "ecs"` selects the ECS container provider explicitly, not the SDK default chain. Laravel also accepts `instance`, provider option arrays, and explicit key/secret/token; unknown provider strings throw.
  - Long-lived workers need refreshable credentials (X).
  - `credential_cache` exists to avoid Pod Identity rate limits (Laravel's documented motivation). Implement or defer is open.
- **Agent receive** (G chooses Symfony's rule; X asks to choose; C and D describe the difference).
  - Laravel uses the agent only in `queue:work`, and only when the requested queue matches the worker queue; otherwise it pops SQS directly.
  - Symfony uses the agent whenever enabled and ignores requested queue names.
  - *Editorial:* follow Symfony (matches the existing scope) and label it a deviation.
  - "The agent serves only its assigned queue" comes from Symfony comments; the agent is not in these repos.
  - `/result` carries `messageId`, `receiptHandle`, `status`, `delay`. It has **no** `queueUrl`.
  - Telemetry queue identity differs: Symfony normalizes the delivered `queueUrl`; Laravel normalizes the queue argument passed to `pop()`.
  - Fresh dispatch always goes to SQS; Symfony intercepts retry re-sends and turns them into releases.
- **Retry math** (G X C; D covers worker defaults).
  - With no `retryUntil`, Laravel's pre-run check lets the job run when `maxTries === 0` or `attempts <= maxTries`; otherwise it fails before running.
  - After an exception it fails when `maxTries > 0 && attempts >= maxTries`. `retryUntil` takes precedence over both.
  - Backoff is indexed `attempts - 1`, the last value repeats, and values are cast to int. Default backoff is 0.
  - `retryUntil` and `maxExceptions` exist in Laravel; C and X say support **or** defer explicitly. Neither mandates deferral.
  - Symfony's documented default is 3 retries (4 deliveries); Laravel's is 1 try.
- **FIFO / fair queues** (G X D).
  - Symfony rejects: positive FIFO delay, >900 s fresh delay, FIFO attributes on a standard queue, and a fair-queue group on FIFO.
  - Laravel instead omits FIFO delay and omits dedup IDs on standard queues. It supports group IDs on both queue types and forwards >900 s delays for SQS to reject. So Laravel does not simply drop everything Symfony rejects.
  - Keep Symfony's rejections as labeled deviations.
  - Default FIFO group is the resolved queue argument; for logical names that includes `.fifo`.
  - Dedup:
    - Laravel filters out an empty dedup ID, enabling content-based dedup (G recommends exposing this). Symfony keeps an empty string as an explicit ID.
    - X cautions that content-based dedup hashes the whole envelope, whose fresh UUID and trace data defeat business-level dedup. Choose IDs once per logical send and reuse them across network retries.
- **Payload limit** (C G X). Pinned baseline is 1,048,576 bytes of encoded body. A queue can have a lower `MaximumMessageSize`, so map SQS rejections to a typed error (X).
- **Visibility** (C X).
  - Direct receive has no heartbeat. A job that outlives the visibility timeout **may** be redelivered and run concurrently.
  - Configure visibility to cover execution plus completion margin, or renew it independently of the executing code (X).
  - The claim that the 12-hour maximum counts from receive is AWS service behavior not provable from these repos; test it or cite AWS.
- **Undecodable messages** (G X). Symfony stamps transport identity before decoding, then deletes (or reports `processed`) and rethrows; it does not emit `failed_job` on that path. *Recommendation:* Python also emits `failed` and `failed_job` from the worker for these.
- **Security** (split by author; these are project policy, not upstream rules).
  - Job names and type tags resolve only through the registry, never by importing from payload data (C G X).
  - Do not port `FailedJobProvider`'s download-and-decrypt fetch (G; X: not without a confirmed platform requirement).
  - Refuse a LocalStack endpoint when Cloud config is present (G).
  - Do not log config, credentials, receipt handles or full payloads by default (X; G).
  - Beyond registry-only names, bound payload and response sizes, validate arguments against the handler before execution, and define None/zero/negative/non-finite rules and fresh-delay rounding (X).
- **Ordering, state and concurrency** (X).
  - FastAPI `yield` teardown completes before acknowledgement.
  - Handler failure, decode failure and acknowledgement failure are distinct states. No second contradictory outcome, and no new fetch after an ambiguous completion.
  - Define behavior when SIGTERM races a successful 65 s poll, when an offloaded SDK call continues after cancellation, and when a timeout races acknowledgement.
  - Define sync dispatch inside a running loop, sync dispatch of async handlers, context propagation, and client/resource lifetimes. Eager mode must use the real codec and argument validation.
- **Log socket** (C G X D).
  - 2 s connect/write timeouts, persistent connection, reconnect after EOF, and identical JSON flags in both upstreams.
  - Writes loop over partial writes and give up after repeated zero-byte writes. A socket write is not an atomic NDJSON record, so concurrent producers need synchronized, bounded full-line writes (X).
  - Timestamps are UTC `Y-m-d H:i:s.u`. `duration_ms` appears only on `processed`/`released`/`failed`.
  - Event order differs: Laravel emits `failed_job` before lifecycle `failed`; Symfony emits them the other way round.
  - The failure `id`, payload `uuid` and SQS `MessageId` are distinct identities. `payload` is the original string. `exception_preview` is at most 1,001 characters (not bytes).
- **Other medium items.**
  - Queue URL edge cases (Laravel only): full-URL pass-through, suffix appended once, prefix slash trimmed.
  - `LARAVEL_CLOUD_AGENT_SOCKET` fallback (Symfony only).
  - Laravel worker defaults: timeout 60, sleep 3, memory 128 MB with exit 12.
  - `retried_at` exists upstream; excluding it is a v1 scope choice.
  - Python decisions, not upstream facts: asyncio-only backend, build backend, CI matrix.
  - Python 3.10 lacks `uuid.uuid7`, which was added in 3.14.
  - Symfony's `agent-server.php` is a canned stub used only by `AgentSocketIntegrationTest`; `CloudRetryIntegrationTest` uses mocked SQS.

## Build prompt fixes

1. Replace the release gate with scope §30 and the proof list with scope §21. Report generation fails on missing or unapproved non-pass records, closing the "no unexplained conformance failure" loophole (X).
   - Each record states its evidence tier (unit / socket / emulated / live), revision and environment, and uses a unique required ID. A `MessageGroupId` assertion cannot prove server-side fairness or dashboard ingestion (X).
2. Add a table resolving each Laravel-vs-Symfony conflict, with file:line. The prompt already states Laravel is canonical; what is missing is the per-conflict decisions and labeled deviations.
3. Add a contract pack frozen before parallel work: config, delivery state machine, envelope, error classification, event fixtures, feature IDs (X G).
4. One owner per file; named owners for integration, conformance catalog, emulator, CI/drift; a disagreement tiebreaker (X C D).
5. Restore scope items the prompt drops or compresses:
   - boto3 offload from the event loop;
   - 65 s long poll;
   - full error list;
   - package-level CLI errors;
   - vanilla worker target;
   - full 0.x/1.0 posture ("release-quality 0.x" is already present);
   - no empty Django/Flask extras.
6. Concrete typing and isolation gates (X):
   - downstream mypy samples, positive and expected-error, covering direct-call/dispatch types, option collisions, and injected vs serialized parameters;
   - a two-job worker test proving the lifespan runs once and dependency, trace and `JobContext` state are per job.
7. Installed-product checks (X): build wheel and sdist, install each outside the checkout, verify `py.typed`, public exports and absence of optional dependencies, and ensure shipped CLI commands do not import the excluded `demo/`.
8. Change scaffold step 3 to "keep `PROJECT_SCOPE.md`; propose amendments" instead of producing a new one (G X).

## Corrections made after Astra's verification

- `GET /next` retries: one retry after 500 ms → 3 total attempts.
- Fatal body check: "non-object" → "does not decode to a PHP array" (JSON arrays pass).
- Removed the claim that the agent message `queueUrl` routes `/result`; separated Laravel and Symfony telemetry queue identity.
- Timeout sequence: failure checks first; `failed` vs `released`; SIGKILL fallback; additional terminal conditions.
- Retry inequalities rewritten to match `maxTries === 0` unlimited and `retryUntil` precedence.
- "Delivered twice" → "may be redelivered and run concurrently".
- Symfony retry policy: Messenger retry strategy, not a "worker registry".
- Removed the unsupported "older queues" qualifier on lower queue limits.
- `@pointer`: valid upstream; rejecting it is a v1 policy choice.
- FIFO/fair: Laravel does not silently drop everything Symfony rejects.
- "Fabricated" → "out of range"; DeepSeek's additional inaccuracies listed; "105 citations checked" qualified as structural only.
- Consensus bullets split by author where recommendations differed (config ignoring, agent receive choice, retry deferral, empty dedup ID, security rules, timeout topology).
- Added material omissions from Codex's report: failure-record loss when records fit, Symfony projection erasing small envelopes, event ordering and identity, config precedence and credential refresh, visibility margin, dedup envelope caveat, cancellation and shutdown races, sync/async and resource ownership, trust-boundary validation, socket framing, evidence tiers, typing and two-job isolation gates, installed-product checks.
