# Laravel Cloud Queues for Python — Project Scope

## 1. Project identity

**Package name:** `laravel-cloud-queues`  
**Import namespace:** `laravel_cloud_queues`  
**License:** MIT  
**Initial maturity:** `0.x` until the public API and Cloud compatibility contract are proven stable  
**Python:** 3.10+  
**Primary framework target:** FastAPI  
**Architecture target:** framework-agnostic core with framework-native adapters  
**Initial framework implementation scope:** core/vanilla Python + FastAPI  
**Future adapters:** Django and Flask, designed for but not implemented as production adapters in the first release

This project is intended to be credible as a future first-party Laravel package. Avoid personal branding, project-specific assumptions, and APIs that would be awkward to transfer into the Laravel organization.

The package must feel native to Python and to each supported Python framework. It is **not** an attempt to reproduce Laravel's PHP queue API in Python. Laravel compatibility is required at the Laravel Cloud Managed Queues infrastructure and runtime-contract layer.

The producer and consumer are two execution modes of the same Python application: the web/application process dispatches work and a separately deployed queue worker for that same application processes it. Cross-language PHP/Python job payload interoperability is out of scope.

### Governing documents

- `docs/decisions.md` records resolved decisions (D1–D6). This scope incorporates them; if the two ever disagree, the decision record wins until this scope is corrected.
- `docs/references.md` lists the pinned public sources and the private Laravel repositories to verify platform behavior against. Private repositories are cited by name only; never copy their code or internal details into this public repository.
- `docs/audits/2026-09-27/` holds the cross-lab audits, their verification reports, and `platform-findings.md` (live Laravel Cloud evidence).

### Platform status (verified 2026-09-27)

Laravel Cloud runs Python 3.10–3.14 applications, including FastAPI, but **managed queues are not yet available for Python**. The Cloud API rejects managed-queue creation for FastAPI applications, and Python containers receive no `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`, no agent socket and no AWS credentials. Worker clusters do run Python. The observability log socket (`/tmp/cloud-init.sock`) is present in Python containers. See `docs/audits/2026-09-27/platform-findings.md`.

Consequences for this project:

- Managed-queue mode is built and tested against the pinned upstream contract, LocalStack and a local agent emulator. Live verification waits until Laravel Cloud enables managed queues for Python.
- Worker-cluster modes (self-managed SQS and Redis/Valkey, §6) work on Laravel Cloud today and are first-class v1 features.

---

## 2. Compatibility baseline and source of truth

The reproducible compatibility baseline for the initial implementation is:

- `laravel/framework` **v13.33.0**
- `laravel/symfony-on-cloud` commit **50c945170b6cb5690370d15fd725c6f82495e9ba**

At the time this scope was written, Laravel Framework v13.33.0 is the latest 13.x tagged release.

### Primary Laravel Framework references

Agents must independently inspect the pinned source and encode observed behavior into conformance tests. At minimum review:

- `src/Illuminate/Foundation/Cloud/Queue.php`
- `src/Illuminate/Foundation/Cloud/QueueConnector.php`
- `src/Illuminate/Foundation/Cloud/CloudJob.php`
- `src/Illuminate/Foundation/Cloud/FailedJobProvider.php`
- `src/Illuminate/Foundation/Cloud/Events.php`
- `src/Illuminate/Foundation/CloudBootstrapper.php`
- `src/Illuminate/Queue/SqsQueue.php`
- `src/Illuminate/Queue/Jobs/SqsJob.php`
- `src/Illuminate/Queue/Worker.php`
- `src/Illuminate/Queue/WorkerOptions.php`
- Laravel 13.x queue documentation where it clarifies public queue semantics

### Secondary cross-framework reference

Use `laravel/symfony-on-cloud` as the main precedent for adapting Laravel Cloud features to a non-Laravel framework. At minimum inspect:

- `src/Queue/Agent/AgentClient.php`
- `src/Queue/ManagedQueueConfig.php`
- `src/Queue/Messenger/CloudQueueTransport.php`
- `src/Queue/QueueEventSubscriber.php`
- `src/Observability/Events.php`
- `README.md`
- relevant tests under `tests/`

Laravel Framework behavior is the canonical compatibility reference. `symfony-on-cloud` is the secondary implementation reference where framework-neutral adaptation decisions are needed.

Where this scope deliberately follows Symfony or a project decision instead of Laravel, it says so. The conformance catalog must record each such case as a labeled deviation with its source reference, not as a silent match.

### Evidence hierarchy

When sources disagree, trust them in this order: executed code path > tests > code comments > documentation. Upstream comments are known to be stale in places (for example, the Symfony transport class comment says the agent is chosen by socket presence, while its config and factory use the injected flag). Statements about proprietary Cloud services (agent, collector, dashboard) that appear only in comments are assumptions to verify, not facts.

### Upstream drift policy

Pin the baseline above for deterministic CI and conformance results. Add a separate upstream-drift check that detects newer Laravel 13.x releases and relevant changes without silently changing the baseline. Updating the compatibility baseline must be an intentional change with corresponding test and documentation updates.

---

## 3. Core product objective

Provide a PyPI package that lets a Python application use Laravel Cloud Managed Queues as a first-class hosted queue backend.

The package must:

1. Dispatch jobs from Python application processes to Laravel Cloud Managed Queues.
2. Run dedicated Python worker processes/containers that can execute on separate hardware from the producer.
3. Consume through the Laravel Cloud in-container queue agent when that agent is enabled.
4. Support Laravel Cloud **worker clusters** today with two self-managed backends: the customer's own SQS queues, and Redis/Valkey (for example Laravel Valkey attached to the environment).
5. Support direct SQS and Redis locally for development, LocalStack/Valkey testing and conformance.
6. Match Laravel Cloud queue lifecycle, retry, queue-name, FIFO, fair-queue, failure, timeout, and observability semantics closely enough that Python jobs behave operationally like first-class Laravel Cloud Managed Queue jobs.
7. Provide a framework-native FastAPI experience while keeping the transport/runtime core independent from FastAPI.
8. Provide a conformance harness that objectively reports what matches the Laravel baseline and what does not.

Laravel Cloud Managed Queues remain the product center. Self-managed SQS and Redis/Valkey exist so Python applications on Laravel Cloud worker clusters have a supported queue today. They follow Laravel's `sqs` and `redis` queue-driver semantics; the package is not positioned as a general-purpose job framework.

---

## 4. Package and repository layout

Use a `src/` package layout.

Suggested high-level structure:

```text
.
├── pyproject.toml
├── README.md
├── LICENSE
├── PROJECT_SCOPE.md
├── src/
│   └── laravel_cloud_queues/
│       ├── __init__.py
│       ├── py.typed
│       ├── config/
│       ├── codecs/
│       ├── jobs/
│       ├── registry/
│       ├── transports/
│       │   ├── agent/
│       │   ├── sqs/
│       │   └── redis/
│       ├── worker/
│       ├── observability/
│       ├── testing/
│       ├── cli/
│       └── fastapi/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conformance/
├── demo/
│   └── ...
├── probe-app/
└── docs/
```

`probe-app/` is a throwaway FastAPI app deployed to Laravel Cloud to inspect what the platform injects. It is repository-only tooling like `demo/`.

The precise internal layout can change if the implementation team finds a cleaner separation, but the public boundaries should remain clear.

### Published distribution

The top-level `demo/`, `probe-app/` and `docs/` directories are repository-only and **must not be included in the PyPI wheel or source distribution payload unless there is a compelling packaging reason explicitly approved later**. Use `hatchling` with explicit include rules, and add a packaging test proving these directories are absent from both the wheel and the sdist.

Tests, local emulators, and development-only assets should likewise not accidentally inflate the runtime wheel.

Packaging verification must build the wheel and the sdist, then install each into a fresh environment **outside** the repository checkout (never an editable install) and check: core import without optional extras, the `[fastapi]` extra, the console entry point, `py.typed`, public exports, and that no shipped module imports `demo/`, `probe-app/` or test tooling.

### Optional dependencies

One PyPI distribution is required.

Initial intended install shape:

```text
pip install laravel-cloud-queues
pip install "laravel-cloud-queues[fastapi]"
pip install "laravel-cloud-queues[redis]"
pip install "laravel-cloud-queues[otel]"
```

Core depends on `boto3` and `anyio`. The Redis/Valkey backend needs the `[redis]` extra (`redis-py`). OpenTelemetry propagation needs the `[otel]` extra. Pydantic model support activates when a compatible Pydantic v2 is installed (FastAPI already brings it); it does not need its own extra.

The architecture must reserve clean adapter boundaries for future:

```text
laravel-cloud-queues[django]
laravel-cloud-queues[flask]
```

Do not expose nonfunctional framework extras merely for appearance. Add Django/Flask extras when the adapters are actually implemented and tested.

---

## 5. Python and quality requirements

### Python versions

Support **Python 3.10+**.

CI must exercise CPython 3.10, 3.11, 3.12, 3.13 and 3.14 on Linux. macOS is a supported development platform but not a required CI runner. The implementation should avoid unnecessarily raising the minimum version.

Python 3.10 constraints to respect: no `uuid.uuid7` (added in 3.14; use a small reviewed dependency or backport for UUIDv7), no `asyncio.TaskGroup`/`asyncio.timeout`, no `typing.Self` or `type` alias syntax without `typing_extensions`, no `except*`.

### Async backend

Core async orchestration uses AnyIO **on the asyncio backend**. Trio is not supported in v1.

### Typing

`ty check` (with the strict rule set in `pyproject.toml`) plus Ruff's `ANN`/`PYI` rules is a release gate for shipped package code. ty does not flag missing annotations, so Ruff enforces them.

Requirements:

- all public APIs fully typed;
- preserve useful callable signatures through decorators where practical;
- minimize `Any`;
- unavoidable `Any` must be localized and justified;
- include `py.typed`;
- exported generic types should be useful to downstream applications;
- public decorator and dispatch typing should give strong IDE and type-checker behavior;
- tests may use narrowly scoped typing relaxations where justified, but production package code must remain strict;
- downstream typing samples (positive and expected-error) must be type-checked in CI: direct calls, `dispatch`/`dispatch_async` argument checking, `.options(...)` types, handler parameters named `queue`/`delay`/`timeout`, and injected versus serialized parameters.

### Linting/formatting

Use a modern, minimal Python toolchain. Ruff is a suitable default for linting/formatting unless the agents identify a concrete reason otherwise.

### CI release gates

A release must pass:

- Python 3.10+ matrix;
- `ty check` and Ruff `ANN`/`PYI`;
- lint/format checks;
- unit tests;
- FastAPI integration tests;
- LocalStack/SQS tests;
- Redis/Valkey tests (Valkey and Redis containers in CI);
- Cloud-agent emulator tests;
- observability socket tests;
- compatibility/conformance suite;
- downstream typing samples;
- package build/install smoke tests from isolated wheel and sdist installs;
- `py.typed` verification;
- verification that `demo/`, `probe-app/` and `docs/` are excluded from published artifacts.

Required integration services (LocalStack, Valkey/Redis, emulator) must run in CI. A missing service is a gate failure, not a silent skip. Pin the LocalStack and Valkey image versions. Upload the conformance JSON and process/socket logs as CI artifacts on failure.

Local development may use any Redis-compatible server; Laravel Herd's bundled Valkey on `127.0.0.1:6379` works.

Real Laravel Cloud Python deployment verification is expected later but is explicitly **not** a current release gate.

---

## 6. Configuration contract

### Backend modes (D6, D6a)

| Mode | Selected when | Broker | Receive |
|---|---|---|---|
| `managed` | `LARAVEL_CLOUD_QUEUES_BACKEND=managed`, or unset while `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present | Laravel Cloud SQS | Agent when `agent.enabled`, else direct SQS |
| `sqs` | `LARAVEL_CLOUD_QUEUES_BACKEND=sqs` | Customer's own SQS | Direct SQS |
| `redis` | `LARAVEL_CLOUD_QUEUES_BACKEND=redis` | Redis/Valkey | Redis transport |

- When `LARAVEL_CLOUD_QUEUES_BACKEND` is unset and no managed config is present, fail with a configuration error.
- The presence of `REDIS_URL` or `AWS_*` variables never selects a backend. Laravel Cloud injects `REDIS_URL` for attached Valkey caches and `AWS_*` for attached object storage.
- Every setting may also be passed in code; code wins over the environment.
- Laravel Cloud-injected assignment remains authoritative in managed mode.

### Managed configuration

`LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is the managed-mode configuration source. The package auto-detects it and needs no manual AWS or queue wiring.

Shape at the pinned baseline (`CloudBootstrapper.php` adds the `after_commit`, `overflow` and `credential_cache` keys):

```json
{
  "driver": "cloud",
  "queue": "default",
  "queues": [],
  "connection": {
    "driver": "sqs",
    "prefix": "https://sqs.<region>.amazonaws.com/<account>",
    "suffix": "-<environment-specific-suffix>",
    "queue": "default",
    "region": "<region>",
    "credentials": "ecs",
    "after_commit": false,
    "overflow": {"enabled": false, "store": null, "always": false, "delete_after_processing": true},
    "credential_cache": {"enabled": false, "store": null, "fallback_store": "file"}
  },
  "agent": {
    "enabled": true,
    "socket": "/tmp/cloud-agent.sock"
  }
}
```

Rules:

- Malformed JSON, `driver` other than `cloud`, or a missing `connection` while the variable is set is a configuration error. This follows Laravel's throwing decoder; do **not** copy Symfony's lenient parser, which treats malformed JSON as "not configured".
- `queues` may be a JSON list or an object; an object contributes its keys. It is an inventory, not a dispatch allowlist.
- Queue defaults: the sender uses `connection.queue`; the worker assignment is top-level `queue`, else `default`.
- `region` is required. Do not default it.
- `credentials: "ecs"` selects the ECS container credential provider **explicitly**, with refreshable credentials. `instance` selects the instance-profile provider. Any other string is a configuration error. Never fall back to boto3's default credential chain: Laravel Cloud object storage places R2 keys in `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` and an R2 endpoint in `AWS_ENDPOINT_URL`, which the default chain and endpoint resolution would pick up.
- `after_commit`, `overflow` and `credential_cache` are parsed and preserved. v1 does not implement them. If `overflow.enabled` or `credential_cache.enabled` is true, log a startup warning that the setting is not supported and state the consequence (overflow: oversized payloads are rejected; credential cache: each worker resolves its own credentials).
- Agent socket: `agent.socket` from the config, else `LARAVEL_CLOUD_AGENT_SOCKET`, else `/tmp/cloud-agent.sock`. The environment variable never overrides an explicit config value.
- Unknown fields are preserved for forward compatibility.
- Never log the config's credentials, receipt handles or full job payloads by default.

### Self-managed SQS configuration (`sqs` mode)

| Variable | Meaning |
|---|---|
| `LARAVEL_CLOUD_QUEUES_SQS_PREFIX` | Queue URL prefix, e.g. `https://sqs.us-east-2.amazonaws.com/<account>` (required) |
| `LARAVEL_CLOUD_QUEUES_SQS_SUFFIX` | Optional queue name suffix |
| `LARAVEL_CLOUD_QUEUES_SQS_QUEUE` | Default queue (default `default`) |
| `LARAVEL_CLOUD_QUEUES_SQS_REGION` | Region (required) |
| `LARAVEL_CLOUD_QUEUES_SQS_KEY` / `_SQS_SECRET` | Credentials (required unless `_SQS_CREDENTIALS=default`) |
| `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS` | Set to `default` to opt into boto3's default credential chain (local profiles, IAM roles). Never implicit, because on Laravel Cloud the default chain holds object-storage keys |
| `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` | Endpoint override (LocalStack) |

All values are passed to the boto3 client explicitly. The client must ignore `AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_SQS` unless `_SQS_ENDPOINT` is set. Refuse `_SQS_ENDPOINT` when `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is also present.

### Redis/Valkey configuration (`redis` mode)

| Variable | Meaning |
|---|---|
| `LARAVEL_CLOUD_QUEUES_REDIS_URL` | Connection URL; falls back to `REDIS_URL` only in `redis` mode. `rediss://` enables TLS |
| `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE` | Default queue (default `default`) |
| `LARAVEL_CLOUD_QUEUES_REDIS_PREFIX` | Key prefix (default `laravel-cloud-queues:`) |

### Queue URL/name rules

Match Laravel's `SqsQueue::getQueue` / `suffixQueue` semantics:

- standard: `{prefix}/{queue}{suffix}`;
- FIFO (logical name ends in `.fifo`): `{prefix}/{base}{suffix}.fifo`;
- the suffix is appended only if the name does not already end with it (`Str::finish`);
- trailing slashes are trimmed from the prefix;
- a value that is already a full URL passes through unchanged.

Queue normalization for observability inverts those rules and returns the logical queue name. Conformance must include already-suffixed names, full URLs, empty suffix, trailing slashes and FIFO names. Laravel Cloud queue names are at most 39 characters including `.fifo`.

Dispatching to a nonexistent queue raises `ManagedQueueNotFoundError`, translated from the SQS error code `AWS.SimpleQueueService.NonExistentQueue` (and boto3's `QueueDoesNotExist`). Laravel does not pre-validate against the `queues` inventory; neither does this package. Never silently fall back to the default queue and never auto-create queues.

---

## 7. Job declaration and registry model

### Canonical FastAPI style

FastAPI uses decorator-first declaration.

Illustrative API:

```python
from fastapi import FastAPI
from laravel_cloud_queues.fastapi import LaravelCloudQueues

app = FastAPI()
queues = LaravelCloudQueues(app)

@queues.job
async def send_email(user_id: int) -> None:
    ...
```

The exact syntax may be refined based on typing constraints, but it should remain framework-native and low ceremony.

### Direct callability

Decorated jobs remain normal callable Python objects/functions.

```python
await send_email(123)
await send_email.dispatch_async(123)
```

The decorator should preserve the original callable signature for direct calls and useful static analysis.

### Stable wire names

Default job identity may derive from the import path, e.g.:

```text
myapp.jobs.send_email
```

Allow explicit stable override:

```python
@queues.job(name="emails.send")
async def send_email(...) -> None:
    ...
```

Document explicit names as preferable when teams want queued messages to survive Python module/function refactors.

### Registration/discovery

Support both:

1. explicit module/job inclusion — canonical production/default approach;
2. opt-in automatic package discovery of configured packages — convenience feature.

Do not require recursive implicit scanning. Discovery is driven only by application configuration, **never** by message content: a worker must never import a module or look up a callable because a payload names it. Duplicate wire names are a registration error.

Core/vanilla Python can use a standalone registry. FastAPI uses an explicit application integration. Workers accept an import target such as `myapp.main:app`.

Unknown/unregistered job names are deterministic terminal failures. Do not retry them.

---

## 8. Job payload and serialization

### v1 payload model

Use a **versioned JSON envelope**.

No pickle/cloudpickle by default.

The envelope should include enough data for:

- envelope version;
- stable job wire name;
- unique job UUID;
- display name useful to Laravel Cloud;
- positional arguments;
- keyword arguments;
- queue metadata needed for execution/debugging;
- the job's retry policy (D4);
- trace/context metadata;
- future extension without breaking v1.

Required top-level keys, spelled exactly as Laravel's payload spells them, because Laravel Cloud reads them:

- `uuid`: unique job UUID;
- `displayName`: the job's wire name (explicit name or import path); Laravel derives the failed-job `job_name` from it.

The remaining v1 fields (version, wire name, args, kwargs, policy, context) may use package-chosen names under a versioned structure.

### Retry policy in the message (D4)

At dispatch, store the effective `tries`, `backoff`, `timeout` and `fail_on_timeout` in the envelope. The worker follows the message, and worker defaults apply only to fields the message omits. A deploy therefore never changes the rules for jobs already queued, and dashboard retries keep their original rules. The envelope reserves room for `retry_until` and `max_exceptions`, which are deferred from v1.

### Dashboard retry (D3)

Laravel Cloud's dashboard can retry failed jobs. The envelope must survive being re-queued verbatim: a re-queued envelope is a new delivery whose attempt count starts again at 1. Conformance must re-send a captured failed payload and verify the worker runs it as a fresh first attempt. The live check stays `skipped` until Laravel Cloud supports managed queues for Python.

### Trust boundary

Message bodies are untrusted input.

- Resolve job names only through the registry and value types only through registered codecs.
- Never import modules, look up callables or instantiate classes named by payload data.
- Bound nesting depth and total decoded size; reject NaN/Infinity, duplicate keys and type-tag collisions deterministically.
- Validate decoded arguments against the handler signature and supported annotations **before** execution; binding alone is not validation.
- Runtime objects (`JobContext`, injected dependencies) are never serializable and cannot be supplied through payload data.

The producer and worker are the same Python application at different execution points, but workers may run on separate hardware and on a different deployment revision. Never rely on local filesystem state, memory, or process-local objects in a job payload.

### Supported value types

Core should be native-Python first:

- JSON primitives;
- lists/dicts;
- tuples where a round-trip representation is defined;
- dataclasses;
- enums;
- UUID;
- date/datetime and other deliberately supported scalar codecs;
- explicit custom codec extension points.

Pydantic v2 models should round-trip when Pydantic support is installed, but Pydantic must not become a mandatory core dependency.

### Calls

Support both positional and keyword dispatch:

```python
await job.dispatch_async(123, template="welcome")
```

Recommend keyword arguments in documentation because they are more resilient to signature changes.

### Schema failures

Validate the decoded arguments against the registered callable before execution.

Malformed envelopes, unsupported envelope versions, unknown jobs, codec failures, and argument/schema incompatibility are non-retryable terminal failures. Report them clearly to Laravel Cloud rather than spending retry attempts on deterministic defects.

The envelope must be designed so future payload migrations/version adapters can be added without breaking v1.

### Payload size

Before sending, measure the fully encoded message body in UTF-8 bytes against the transport limit: **1,048,576 bytes** for SQS (Laravel's `SqsQueue::MAX_SQS_PAYLOAD_SIZE`, and Laravel Cloud's documented job payload limit). The Redis backend has **no package-imposed size limit**; only Redis/Valkey server limits apply. Document that moving a Redis-backed application to SQS or managed queues reintroduces the 1 MiB limit.

If oversized, raise a stable typed `PayloadTooLargeError` containing useful size/limit information. A queue can have a lower `MaximumMessageSize`; map that SQS rejection to `PayloadTooLargeError` too, without classifying unrelated `InvalidParameterValue` errors as size errors.

Never silently truncate, compress, or offload in v1.

Laravel already implements overflow at the pinned baseline: when enabled, oversized bodies are stored in a cache and the message carries `{"@pointer": "laravel:sqs-payloads:<uuid>"}`. v1 does not implement overflow. A received `@pointer` body is a deterministic, non-retryable failure reported as unsupported overflow.

Add a prominent roadmap/TODO for:

- transparent compression;
- S3/object-storage large-payload offload;
- configurable thresholds;
- transparent worker hydration/cleanup.

Laravel's existing cache-backed overflow (`SqsQueue::overflow`, `SqsJob` pointer hydration) is the starting point to review when that work begins.

---

## 9. Dispatch API

Support both synchronous and asynchronous dispatch.

Illustrative API:

```python
receipt = send_email.dispatch(user_id=123)
receipt = await send_email.dispatch_async(user_id=123)
```

Both paths must share the same validation, serialization, routing, tracing, and transport semantics through one pipeline.

- `dispatch_async` must never block the event loop. Offload every blocking boto3 or Redis call, including credential resolution, and telemetry socket writes.
- `dispatch` (sync) called while an event loop is running in the same thread must not start a nested loop; document that async code should use `dispatch_async`.
- Choose SQS FIFO deduplication IDs once per logical dispatch and reuse them across internal network retries.
- Laravel Cloud web requests are cut off after 20 seconds (`NGINX_HTTP_TIMEOUT=20`). Dispatch and eager execution inside a request must stay well within that.

### Dispatch receipt

Return a lightweight typed receipt, not a result/future handle.

At minimum:

```python
@dataclass(frozen=True)
class DispatchReceipt:
    message_id: str
    queue: str
```

Additional stable metadata may be included if useful without leaking unnecessary AWS-specific implementation detail.

### Fire-and-forget

v1 jobs are fire-and-forget.

Handler return values are ignored. There is no job result backend, result polling, distributed RPC contract, or production Python failed-job database.

---

## 10. Queue selection and message options

A job can declare a default queue and dispatch can override it through the `.options(...)` builder (D5):

```python
@queues.job(queue="emails")
async def send_email(user_id: int) -> None:
    ...

send_email.dispatch(user_id=1)
await send_email.options(queue="priority", delay=30).dispatch_async(user_id=1)
```

- `.options(...)` returns a typed copy of the job carrying dispatch options: `queue`, `delay`, FIFO `group` and `deduplication_id`, fair-queue `message_group`.
- `dispatch` and `dispatch_async` keep exactly the job's own parameter signature, so options never collide with handler parameters (a handler may legitimately take `queue`, `delay` or `timeout`).
- `ty` checks both the option types and the job's argument types.

### Delays

Fresh-message delays on standard SQS queues must respect the SQS per-message maximum of 900 seconds.

- Accept `int` seconds or `timedelta`. A positive fractional delay rounds **up** to the next whole second, matching the retry-delay rule, so a short delay never becomes immediate.
- Reject negative, non-finite and >900-second delays with `InvalidQueueOptionError` before sending. (Symfony rejects >900; Laravel forwards and lets SQS fail. Labeled deviation.)
- FIFO queues do not support per-message delay: reject a positive delay on a `.fifo` queue. (Symfony rejects; Laravel silently omits `DelaySeconds`. Labeled deviation.)
- The Redis backend applies the same 900-second cap so application behavior is portable.

Retries are different from fresh delays: retry backoff uses message visibility and can extend up to SQS's 12-hour visibility limit.

### FIFO

Queues ending in `.fifo` use FIFO semantics.

Support:

- message group ID;
- deduplication ID;
- sensible defaults compatible with Laravel/SQS;
- strict validation.

- Default FIFO group: the logical queue name **including** `.fifo` (Laravel `SqsQueue::getQueueableOptions`). Document that this serializes the whole queue.
- Default dedup ID: a new unique ID per logical dispatch.
- Content-based deduplication: supported only when the caller explicitly passes an empty dedup ID, in which case the attribute is omitted (Laravel's convention). Document that the envelope contains a fresh `uuid` and trace data, so content-based dedup will not collapse identical business calls; use an explicit business dedup ID for that.
- Validate group and dedup IDs against SQS rules (1–128 characters, allowed character set) before sending.

### Fair queues

For standard queues, support SQS message groups as fair-queue tenant keys.

Do not conflate standard-queue fair message groups with FIFO ordering groups. Reject FIFO-only options on standard queues and fair-queue groups on FIFO queues, rather than silently ignoring attributes. (Symfony behavior; Laravel does not reject. Labeled deviation.)

The Redis backend has no FIFO or fair-queue semantics: FIFO and fair-queue options raise `InvalidQueueOptionError` in `redis` mode.

---

## 11. Transport model

### Dispatch path

Publishing uses SQS with the Cloud-provided queue URL configuration.

Use `boto3` as the canonical direct SQS SDK.

Synchronous calls can use boto3 directly. Async-facing APIs must isolate blocking boto3 operations so they never block the FastAPI/AnyIO event loop.

### Receive path on Laravel Cloud

When the injected agent config says the agent is enabled, the worker receives **only** through the in-container Laravel Cloud agent over its Unix socket, for its assigned queue. It ignores requested queue names on this path. (Symfony behavior. Laravel additionally requires the requested queue to match the worker queue and otherwise pops SQS directly. Labeled deviation.) A conflicting CLI queue selection in agent mode is a startup error, not a silent override.

Do not decide agent use by probing socket existence; the injected config is authoritative.

HTTP over the Unix socket uses `httpx` with a Unix-socket transport, base URL `http://localhost`, redirects disabled, and a bounded response size.

#### `GET /next`

- Request timeout **65 seconds**, which outlasts the agent's poll cycle.
- Retries: up to **3 attempts**, retrying after 0 ms and 500 ms on connection errors (Laravel's `retry([0, 500])`; Symfony makes one attempt).
- **204:** no work.
- **200:** decode JSON. If the body does not decode to a JSON object or array, that is fatal. If `messageId` is missing, empty or not a string, treat it as an empty poll. A non-string `receiptHandle` becomes null; a non-string `body` becomes `""`.
- **Any other status, or an unreachable socket after retries:** fatal.

Message fields: `messageId`, `receiptHandle`, `body`, `attributes` (containing `ApproximateReceiveCount`), `queueUrl`. `queueUrl` is used for telemetry queue normalization; if missing, fall back to the configured queue.

#### `POST /result`

The agent owns the SQS operation for agent-delivered messages.

- Body: `messageId`, `receiptHandle` (omitted when null), `status` (`processed` or `released`), `delay` (seconds; omitted when null; `0` is kept). There is no `queueUrl` and no `failed` status: a terminal failure reports `processed`.
- Timeout **10 seconds** per attempt; up to **3 attempts**, 100 ms apart, on connection errors only.
- **5xx, or connection failure after retries:** the agent is unhealthy. Do not fetch another job, do not assume acknowledgement, exit the worker, and let agent/SQS visibility redeliver.
- **4xx:** the agent rejected this outcome for this message. Log it as a typed `AgentProtocolError`, do not assume acknowledgement, do not report a second outcome, and continue with the next job (Laravel and Symfony behavior). The agent remains authoritative for that message.
- Treat a repeated POST after a lost response as possibly already applied; never report a contradictory second outcome.

Exit status when the worker stops because the agent is unhealthy: **0**, matching Laravel's lost-connection path, so the platform treats a Python worker exactly like a PHP one. Log the agent loss clearly before exiting.

The agent extends visibility in three-minute increments while a job runs (Laravel Cloud docs). Its source is not available to this project (see `docs/references.md`); treat its behavior beyond the documented protocol as unverified.

At-least-once execution and duplicate possibility after ambiguous acknowledgement must be documented. Applications should design jobs to be idempotent.

### Direct SQS receive mode

Used in `managed` mode when the agent is disabled, and in `sqs` mode.

- `ReceiveMessage` with `WaitTimeSeconds=20`, `MaxNumberOfMessages=1`, and `ApproximateReceiveCount` requested. A missing receive count is treated as 1.
- Success deletes the original message.
- Retry calls `ChangeMessageVisibility` on the original message with the latest receipt handle.
- Terminal failure deletes after failure reporting.
- Never create a duplicate message to implement retry.
- **Visibility renewal:** there is no agent heartbeat outside the agent path. While a job runs, a watchdog outside the handler extends visibility before it lapses, so it keeps working even while a sync handler blocks the event loop. Loss of the lease is a transport condition, not a handler exception. Without renewal a long job may be redelivered and run concurrently.
- The 12-hour visibility maximum is measured by SQS from the original receive. Compute retry visibility conservatively and handle an SQS rejection explicitly.

### Redis/Valkey transport (`redis` mode)

Follows Laravel's `RedisQueue` semantics, implemented with atomic Lua scripts:

- **Keys** (under the configured prefix): `queues:<name>` pending list, `queues:<name>:delayed` sorted set, `queues:<name>:reserved` sorted set.
- **Reserve:** atomically migrate due delayed and expired reserved jobs back to pending, pop one job, increment its attempts, and add it to the reserved set with an expiry of now + job timeout + margin.
- **Success:** remove from the reserved set.
- **Retry:** atomically move from reserved to delayed with the backoff; same job ID, never a duplicate.
- **Terminal failure:** remove from reserved after failure reporting.
- **Delay:** add to the delayed set.
- **Reservation renewal:** the same watchdog extends the reserved expiry while a job runs.
- Attempt count comes from the job's reservation counter, the Redis equivalent of `ApproximateReceiveCount`.
- TLS via `rediss://`. Blocking pop with a bounded wait; no busy polling.

---

## 12. Retry and failure semantics

Match Laravel/SQS-native retry semantics.

### Attempts

`ApproximateReceiveCount` (SQS) or the reservation counter (Redis) is the source of truth for the delivery attempt count.

Default `tries` is **1**, matching Laravel's worker default unless overridden by job/worker policy. Do not copy Symfony Messenger's default of 3 retries (4 deliveries).

Attempt checks, matching Laravel `Worker`:

- **Before running:** if `tries > 0` and `attempt > tries`, fail the job without running it. This catches deliveries that exceeded the budget through crashes or timeouts.
- **After an exception:** if `tries > 0` and `attempt >= tries`, the failure is terminal; otherwise release for retry.
- `tries = 0` means unlimited attempts.
- An explicit `JobContext.release()` consumes an attempt like any other delivery.

### Retry policy

Support both:

- decorator shorthand for common configuration;
- reusable typed policy objects underneath.

Example:

```python
@queues.job(
    tries=5,
    backoff=[1, 5, 30, 120],
    timeout=60,
    fail_on_timeout=True,
)
async def send_email(...) -> None:
    ...
```

The exact public naming may be refined if a more idiomatic Python/FastAPI representation is superior, but the semantics are required.

### Unhandled exceptions

An ordinary unhandled exception retries according to the effective retry policy.

If the current delivery is still retryable:

1. emit/report released lifecycle semantics;
2. release the **same** SQS message by changing visibility or reporting `released` to the agent;
3. use configured backoff;
4. do not send a new queue message.

When attempts are exhausted, the failure is terminal.

### Explicit control

Provide a typed non-serialized `JobContext`, available via framework integration/dependency injection, with at least:

- attempt;
- queue;
- message ID;
- explicit `release(delay=...)`;
- explicit terminal `fail(...)`.

Calling explicit release/fail must short-circuit normal success behavior safely and exactly once.

### Backoff

`backoff` is an integer or a list. The delay for a retry is `backoff[attempt - 1]`; past the end of the list, the last value repeats. Default backoff is **0**.

Visibility timeout is whole seconds and must not accidentally floor positive sub-second retry delays to zero. Positive sub-second delays round upward; explicit zero stays zero. (Symfony behavior; Laravel truncates. Labeled deviation.)

Clamp retry visibility to 43,200 seconds (12 hours).

Deferred from v1 (D4): `retry_until` deadlines and `max_exceptions` counters. Both exist in Laravel; the envelope reserves room for them.

### Terminal failed jobs

There is no independent Python production failed-job store.

On terminal failure in `managed` mode:

- emit the `failed_job` event (§15, D1);
- complete the message: `processed` to the agent, or `DeleteMessage` in direct mode;
- let Laravel Cloud own failed-job inspection and retry.

On terminal failure in `sqs` and `redis` modes (D6b):

1. write the full failure record as one structured JSON line to the worker's log output (visible in Laravel Cloud's Logs tab);
2. delete the message.

No `failed_job` or lifecycle events are sent to the log socket in these modes (D12).

There is no dead-letter queue or retry command in v1; re-running a failed job means dispatching it again.

Deterministic decode failures (malformed envelope, unsupported version, unknown job, codec or argument errors, `@pointer` bodies) keep the transport identity (message ID, receipt handle or reservation, queue, receive count) captured **before** decoding. They are terminal on first delivery: emit `failed` and `failed_job` from the worker, then complete the message. Never import a module to "repair" an unknown job.

Local harnesses may capture failures for assertions only.

Do not port Laravel's `FailedJobProvider` fetch path, which downloads and decrypts failed payloads from URLs (D3).

### Delivery state machine

Each delivery has exactly one outcome owner and moves through: `received → running → outcome chosen → reporting → completed | ambiguous`.

- Handler failure, decode failure and acknowledgement failure are distinct categories.
- An acknowledgement failure is never treated as a handler error and never triggers a second, contradictory release or fail.
- After an ambiguous completion (for example a lost `/result` response), the worker stops instead of fetching again.
- `JobContext.release()` and `fail()` record the chosen outcome for the worker; they do not perform blocking I/O inside the handler. If user code catches the control-flow signal, the recorded outcome still wins over success.

---

## 13. Worker architecture

### Dedicated process

Production queue workers are separate dedicated processes/containers and may run on entirely separate hardware from the FastAPI web application.

Never require shared process state.

### One in-flight job

v1 worker concurrency is exactly one in-flight job per worker process.

Scale horizontally using Laravel Cloud worker instances rather than an internal task pool.

Do not build multi-message concurrent execution into v1.

### Async runtime

Use **AnyIO** (asyncio backend) for core async orchestration.

- async handlers execute naturally;
- sync handlers execute directly in the worker process's main thread, so the `SIGALRM` timeout (§14) can interrupt them;
- do not use a thread pool for sync handler execution merely for throughput;
- process-level failure/timeout semantics remain enforceable.

A sync handler blocks the event loop while it runs. Anything that must keep working during a job (visibility or reservation renewal, §11) runs on a watchdog thread, not on the loop.

### FastAPI lifespan

A FastAPI worker must enter the application's normal lifespan once per worker process so app-level resources are initialized consistently with the web application.

Run app lifespan shutdown on clean worker termination.

### Per-job FastAPI dependency scope

FastAPI jobs support `Depends()` where practical.

Each job receives a fresh dependency scope.

Requirements:

- resolve dependencies per job;
- support `yield` dependency teardown;
- ensure cleanup on success/failure/release paths;
- teardown completes **before** the outcome is acknowledged: success means handler completion plus successful teardown. A teardown exception turns success into a handler failure (retry policy applies). Teardown after an explicit release or fail still runs, under a bounded deadline;
- honor `app.dependency_overrides` and cached sub-dependencies within one job;
- app-lifespan resources remain process-scoped;
- request-only concepts such as an HTTP `Request` must produce a clear error unless a deliberate queue-aware substitute exists;
- FastAPI-specific DI machinery stays isolated from the core transport/runtime.

### Graceful shutdown

On `SIGTERM`/`SIGINT`:

- stop fetching new work;
- allow the current job to finish;
- report its final outcome;
- run job dependency cleanup;
- exit application lifespan;
- exit cleanly.

If the platform forcibly kills the process before completion, rely on agent/SQS visibility (or Redis reservation expiry) for redelivery.

Shutdown races to handle explicitly:

- a signal arriving during an idle `GET /next` or blocking pop: stop waiting; if a message is handed over anyway, run it to completion rather than abandoning a message the agent now holds;
- a signal during outcome reporting: finish reporting within its bounded retries;
- repeated signals do not skip reporting;
- SIGKILL cannot promise cleanup.

Laravel Cloud gives Flex workers 90 seconds and Pro workers one hour to finish on shutdown; Flex also caps job runtime at 90 seconds. Document these limits for Cloud users.

### Worker lifecycle options

Provide useful framework-neutral CLI controls including equivalents of:

- `--max-jobs`;
- `--max-time`;
- `--stop-when-empty`;
- `--stop-when-empty-for` (seconds since the last job, or since start);
- `--timeout` (worker default job timeout, default 60);
- `--sleep` (seconds to wait after an empty poll in direct/Redis mode, default 3);
- `--rest` (seconds between jobs, default 0).

Direct-mode receive errors sleep 1 second and retry unless they indicate a lost connection.

Memory-limit worker recycling (Laravel `--memory`, exit 12) is deferred from v1; Laravel Cloud restarts a worker that exceeds its memory allocation and redelivers the job.

Process lifecycle differs by mode:

- **Worker clusters and App-cluster background processes (`sqs`, `redis`):** the worker runs as a long-lived service that Laravel Cloud supervises and restarts whenever it exits, for any reason. Exit codes (124 timeout, 1 fatal transport, 2 configuration, 0 agent loss or clean stop) are diagnostic; every exit leads to a restart, including the app lifespan. Consequences:
  - `--stop-when-empty` and `--stop-when-empty-for` default to off and the docs warn against them here: a supervised worker that exits on an empty queue is restarted immediately, in a loop.
  - `--max-jobs` and `--max-time` remain useful for recycling the process.
  - A configuration error (exit 2) restart-loops; log it clearly on every start.
- **Managed queues:** the platform owns worker lifetime and scaling (including scale to zero). The worker follows the agent protocol and does not assume it is supervised as a long-lived service.

On Laravel Cloud, queue assignment remains authoritative. Worker clusters scale on CPU, memory or a fixed count, not on queue depth; document this for `sqs` and `redis` modes.

### Queue selection

Canonical CLI concept:

```text
laravel-cloud-queues work myapp.main:app
```

On Cloud, the injected worker/queue assignment is authoritative. Do not pretend a conflicting CLI queue selection overrides hosted worker assignment.

Outside agent mode, allow explicit queue selection, including multiple named queues as a comma-separated priority list: poll in order, first queue with work wins.

---

## 14. Timeout semantics

Timeouts are process-level behavior matching Laravel's worker (D2), not asyncio cancellation.

Mechanism:

1. Before each job, arm `signal.setitimer(ITIMER_REAL, timeout)` on the main thread. The timeout comes from the message (D4), else the worker default (60 seconds). `0` disables the timeout.
2. When the alarm fires, the handler:
   1. applies the terminal checks: attempts exhausted (`tries > 0` and `attempt >= tries`) or `fail_on_timeout`; if either holds, the job is failed (§12 terminal failure, including `failed_job`) and the message completed;
   2. emits the lifecycle event: `failed` if the job was failed, otherwise `released`;
   3. exits immediately with `os._exit(124)`, skipping Python cleanup.
3. A retryable timeout does **not** release the message and does not apply backoff. The message returns through agent/SQS visibility or Redis reservation expiry, and its receive count increments on redelivery.
4. After a successful job, disarm the timer.

Laravel Cloud restarts the exited worker. The FastAPI lifespan runs once per worker process, so it starts again in the new process.

Known limitation, shared with Laravel: Python runs signal handlers between bytecode instructions, so a job blocked in native code or a blocking call can overrun its timeout until control returns to the interpreter. Document this; do not claim a stronger guarantee. No supervisor process in v1.

Conformance tests must prove the observable semantics in real subprocesses (exit code 124, lifecycle event, message redelivered with an incremented count, terminal failure on the last attempt, fail-on-timeout), for async handlers, sync Python loops and a native blocking call.

---

## 15. Observability and Laravel Cloud dashboard parity

Dashboard parity is a v1 release blocker.

### Cloud lifecycle events

Emit Laravel Cloud-compatible queue lifecycle events (`Illuminate\Foundation\Cloud\Queue`):

```json
{"_cloud_event": "queue", "timestamp": "2026-09-27 12:00:00.123456", "type": "processed", "queue": "emails", "duration_ms": 42}
```

- `type`: `queued`, `started`, `processed`, `released` or `failed`.
- `timestamp`: UTC, format `Y-m-d H:i:s.u` (six-digit microseconds, no `T`, no zone suffix).
- `queue`: the normalized logical queue name (§6).
- `duration_ms`: non-negative integer milliseconds, truncated (Laravel), present only on `processed`, `released` and `failed`.
- `queued` is emitted by the producer after a successful send; `started` when a delivery begins; exactly one completion event per delivery.

### Failed job event (D1)

Emit Laravel's `failed_job` event (`Illuminate\Foundation\Cloud\FailedJobProvider::log`):

| Field | Value |
|---|---|
| `_cloud_event` | `"failed_job"` |
| `id` | New UUIDv7 bound to the failure timestamp (distinct from the payload `uuid` and the SQS message ID) |
| `queue` | Normalized queue name |
| `started_at` | Delivery start, same timestamp format |
| `attempts` | Integer attempt count |
| `payload` | The original message body **as a string** |
| `exception_preview` | `"<ExceptionClass>: <message> in <file>:<line>"` (or without `: <message>` when empty), at most 1,001 characters |
| `job_name` | The payload's `displayName` |
| `exception` | Full exception with traceback |

Order: emit `failed_job` first, then the `failed` lifecycle event with the same timestamp (Laravel order; Symfony reverses it).

Size policy (D1). The Laravel Cloud log collector is documented in `symfony-on-cloud` as dropping lines over 16 KiB (unverified here; see `docs/references.md`):

1. If the whole encoded line fits the limit, send it as is.
2. Otherwise trim `exception` (keep the head and a truncation marker) until the line fits.
3. If it still does not fit, trim `payload` as well and add `"replayable": false`. Dashboard retry will not reproduce that job.
4. Measure the final encoded line in bytes, including JSON escaping and multibyte characters.

Never apply Symfony's payload projection (which keeps only `uuid`, `displayName` and a truncated `body`): it discards the Python envelope's arguments and context even for small messages.

Failure records are **best-effort**: the message is completed before the record is written, so a log-socket outage at that moment loses the record. Document this; there is no Python failed-job store.

Not emitted in v1: the `retried_at` variant of `failed_job` that Laravel emits when a failed job is forgotten.

### Socket protocol

Laravel Cloud observability uses newline-delimited JSON over a persistent Unix stream socket (`Illuminate\Foundation\Cloud\Events`).

- Address: `LARAVEL_CLOUD_LOG_SOCKET` when set, else `unix:///tmp/cloud-init.sock`. Verified present in Python containers on Laravel Cloud even though the variable is not set.
- Connect timeout 2 seconds; write timeout 2 seconds; persistent connection.
- Before each write, detect EOF and reconnect.
- Write the whole line, looping over partial writes; give up after repeated zero-byte writes and disconnect.
- One JSON object per line, trailing newline. Encoding matches Laravel's flags: slashes and Unicode unescaped, zero fractions preserved, invalid UTF-8 replaced.
- Serialize writes from concurrent producers so lines never interleave.
- Telemetry failures are logged locally without recursion and never raised to the caller.

### Failure policy

Observability is best-effort.

An observability socket outage must never convert a successful queue job into a failed job.

Record/log telemetry degradation locally and let the conformance report surface it.

Agent `/result` reporting is **not** observability; it is part of queue correctness and remains fatal when unavailable after bounded retries (§11).

In `sqs` and `redis` modes, the package emits **no** lifecycle or `failed_job` events: Laravel Cloud currently ingests queue lifecycle events for managed queues only (confirmed by the Laravel Cloud team, D12). Observability in these modes is the worker's own structured log lines. Revisit if the platform starts accepting them.

---

## 16. Tracing

Support optional OpenTelemetry/W3C trace-context propagation in v1.

Requirements:

- no hard OpenTelemetry dependency in core;
- versioned envelope metadata/context section;
- inject standard trace context on dispatch when tracing is available;
- extract/activate it on worker execution, and reset it after each job so context never leaks between jobs;
- do not conflate application trace propagation with Laravel Cloud's required queue lifecycle events;
- failure to use optional tracing must not break queue execution.

---

## 17. FastAPI adapter

FastAPI is the reference framework implementation.

### Goals

The integration should feel like part of FastAPI/Python, not like Laravel APIs translated into Python.

It should provide:

- explicit integration object bound to a FastAPI app;
- decorator-first job registration;
- dependency injection;
- lifespan integration;
- typed sync/async dispatch;
- worker bootstrap from app import target;
- direct callability;
- test/eager mode;
- job context dependency.

Illustrative only:

```python
from fastapi import Depends, FastAPI
from laravel_cloud_queues.fastapi import LaravelCloudQueues
from laravel_cloud_queues import JobContext

app = FastAPI()
queues = LaravelCloudQueues(app)

@queues.job(name="emails.send", queue="emails", tries=3)
async def send_email(
    user_id: int,
    mailer: Mailer = Depends(get_mailer),
    job: JobContext = Depends(current_job),
) -> None:
    ...
```

Do not blindly commit to this exact signature if it creates type-checking or parameter-collision problems. The final API must meet the semantics while remaining idiomatic and statically typeable.

### Supported FastAPI generation

Target the current supported FastAPI stack at release time and Pydantic v2. Do not add Pydantic v1/legacy compatibility branches to a new package.

---

## 18. Vanilla/core mode

Core must remain useful without FastAPI.

Provide a straightforward registry/queue object model for scripts or other frameworks to build on.

Do not require users to construct a fake web application.

Core public APIs should cover:

- registry;
- job definition;
- sync/async dispatch;
- worker;
- policies;
- transports;
- codecs;
- testing primitives;
- typed configuration/errors.

This core becomes the foundation for future Django and Flask adapters.

---

## 19. Future Django and Flask adapters

Do not ship incomplete adapters in the first implementation.

Instead, document adapter interfaces and intended integration points.

Future principles:

- Django should feel native to Django settings/apps/management commands and dependency/service patterns used in Django projects.
- Flask should feel native to Flask extensions/application context/CLI.
- Framework-native APIs can differ; shared core behavior and wire contract must remain identical.

---

## 20. Testing modes

### Eager/in-memory application testing

Provide a fast test mode for application code.

It must still exercise, through the same code paths as the real worker:

- argument binding;
- payload validation;
- serialization/deserialization;
- handler registration;
- FastAPI DI and cleanup where applicable.

Eager mode works both inside and outside a running event loop without nesting loops.

It may execute immediately and surface handler exceptions to the test.

Allow recording dispatched jobs for assertions.

Clearly state that eager mode does not replace transport/conformance testing.

### Direct SQS / LocalStack testing

LocalStack should exercise the actual SQS transport implementation, including:

- send/receive;
- named queue URL construction;
- standard delay;
- retry via visibility;
- attempt count;
- delete;
- FIFO behavior;
- fair-queue attributes;
- queue-not-found behavior;
- payload size behavior where feasible;
- self-managed `sqs` mode configuration, including that `AWS_*` variables are ignored;
- visibility renewal during a long job.

The same SQS test suite runs against moto locally (no Docker needed) and LocalStack in CI, selected by `LARAVEL_CLOUD_QUEUES_TEST_SQS=moto|localstack`. LocalStack in CI is the authoritative release gate; moto is a local convenience and may emulate visibility timing, FIFO deduplication and fair queues less precisely.

LocalStack verifies the arguments sent to SQS and its emulated behavior. It cannot prove server-side fairness or real SQS timing; record such cases at the `emulated` evidence tier.

### Redis/Valkey testing

Run the `redis` transport against real Valkey and Redis in CI (containers) and locally (any Redis-compatible server, such as Laravel Herd's Valkey on `127.0.0.1:6379`). Cover:

- dispatch, reserve, success, retry with backoff (same job ID), terminal failure;
- delayed jobs and the 900-second cap;
- attempt counting across redeliveries;
- reservation expiry after a worker is killed (redelivery with an incremented attempt);
- reservation renewal during a long job;
- atomicity under concurrent workers (no job delivered to two workers at once while reserved);
- TLS (`rediss://`) connection;
- rejection of FIFO and fair-queue options.

### Agent emulator

Build a local Laravel Cloud queue-agent emulator that exposes the baseline protocol over a Unix socket:

- `GET /next`;
- `POST /result`;
- message holding/state;
- result capture;
- visibility/release behavior sufficient to verify client semantics;
- deterministic fault injection for disconnects, malformed responses, HTTP errors, delayed responses, etc.

It need not reproduce unrelated Laravel Cloud implementation internals. It exists to provide a deterministic compatibility contract.

`symfony-on-cloud`'s `tests/Fixtures/agent-server.php` is only a canned stub (a fixed response for every `GET /next`, `200` for everything else). Use it as a starting point for protocol shape, not as a model: the Python emulator holds message state, validates `/result` bodies, tracks receive counts and releases, and injects faults. Test the real worker as a subprocess against it.

### Observability collector

Build a local Unix-socket collector for newline-delimited Cloud events.

Capture and validate:

- queue lifecycle sequence;
- normalized queue;
- timestamps;
- durations;
- failed-job events;
- best-effort failure behavior.

---

## 21. Demo/conformance application

A top-level **`demo/`** directory is required.

This is not primarily a polished showcase. It is an executable FastAPI compatibility/conformance application designed to tell engineers exactly what works and what does not.

It must be excluded from the PyPI distribution.

### Required demo capabilities

Provide endpoints/fixtures/jobs that exercise at least:

- standard dispatch;
- sync handler;
- async handler;
- producer and worker as separate processes;
- FastAPI lifespan state available to jobs;
- `Depends()` inside jobs;
- `yield` dependency cleanup;
- direct job callability;
- named queue;
- per-dispatch queue override;
- delayed standard job;
- retry/backoff using same message ID;
- `ApproximateReceiveCount` attempts;
- explicit `JobContext.release()`;
- explicit terminal fail;
- default `tries=1`;
- retry exhaustion;
- timeout/release behavior;
- fail-on-timeout;
- FIFO group/dedup;
- rejection of FIFO per-message delay;
- fair-queue message group on standard queue;
- rejection of invalid FIFO/fair combinations;
- unknown job terminal behavior;
- schema-incompatible payload terminal behavior;
- oversized payload typed error;
- queue-not-found typed error;
- observability events;
- failed-job event;
- observability socket outage not failing successful job;
- agent `/result` failure terminating worker;
- graceful shutdown;
- trace-context propagation when optional tracing is installed, and absence of errors when it is not;
- eager testing behavior;
- dashboard-retry replay of a captured failed payload (D3);
- `failed_job` size policy: fits, exception trimmed, not replayable (D1);
- retry policy carried in the message across a simulated deploy (D4);
- `.options(...)` builder with handler parameters named `queue`/`delay`/`timeout` (D5);
- backend selection and configuration errors (D6a);
- self-managed `sqs` mode ignoring `AWS_*` variables;
- `redis` mode dispatch, retry, delay, timeout and terminal failure;
- log-only terminal failures outside managed mode (D6b);
- two sequential jobs on one worker with isolated dependencies, trace context and `JobContext`;
- undecodable-message handling, including `@pointer` bodies.

### Human-readable output

Provide a CLI/test presentation such as:

```text
PASS     dispatch.standard
PASS     worker.agent_receive
PASS     retry.visibility_release
PARTIAL  observability.failed_job
FAIL     timeout.release
SKIPPED  cloud.live
```

### Machine-readable compatibility report

Generate a structured JSON report such as `compatibility-report.json`.

Statuses:

- `pass`;
- `fail`;
- `partial`;
- `skipped`;
- `unsupported`.

Each result should include:

- feature ID;
- expected behavior;
- observed behavior;
- status;
- evidence;
- error/exception if present;
- upstream source/baseline reference;
- evidence tier: `unit`, `socket`, `emulated`, or `live`;
- deviation label, when the behavior intentionally differs from Laravel;
- environment metadata;
- Python/package/framework versions.

The report carries a schema version and run-level metadata (revision, environment, timestamp) separate from per-feature evidence.

Prefer evidence over assertions. For example, retry compatibility should record the original and retried message IDs and attempt counts so the report proves the same SQS message was released rather than duplicated.

---

## 22. Conformance methodology

Every infrastructure behavior should be traceable to upstream source or an explicit project-level decision.

Build a machine-readable conformance catalog that maps feature IDs to:

- Laravel source file and pinned version;
- optionally Symfony cross-framework source file/commit;
- expected semantic behavior;
- test implementation;
- result.

The suite must not mutate its expectations merely to make the Python implementation pass. Correcting an expectation that is proven wrong against source is allowed, with the evidence recorded.

When Python intentionally differs because of language/framework constraints, report `partial` or a documented justified deviation instead of pretending full compatibility.

The catalog is a versioned file in the repository. Every capability required by this scope has exactly one catalog record; duplicate or missing IDs fail the suite. Every required feature must `pass` at its declared evidence tier. Only features on an explicit, reviewed exception list may be `skipped`, `partial` or `unsupported`: currently the live Laravel Cloud managed-queue checks, pending platform support for Python. The report command exits non-zero on any missing record or unapproved non-pass status.

---

## 23. CLI

Ship one framework-neutral entry point:

```text
laravel-cloud-queues
```

Expected command families include:

```text
laravel-cloud-queues work myapp.main:app
laravel-cloud-queues inspect myapp.main:app
laravel-cloud-queues conformance ...
```

Exact subcommands may evolve, but there should be one canonical worker/runtime CLI across frameworks.

Future Django/Flask integrations may add native wrappers while delegating to the same core worker.

CLI errors should be actionable and package-level, not raw boto3/HTTP stack traces by default; a `--debug` flag may show tracebacks.

`inspect` reports registry and configuration (mode, queues, registered jobs) without opening broker connections or printing secrets. It never performs destructive queue operations.

The worker target may be a FastAPI app (`module:app`) or a core registry object (`module:registry`), so plain-Python apps need no FastAPI.

`conformance` runs against repository tooling (`demo/`, emulators); when that tooling is absent from an installed package, it explains how to run it from a checkout rather than failing obscurely.

Worker exit codes:

| Code | Meaning |
|---|---|
| 0 | Clean stop (signal, `--max-jobs`, `--max-time`, `--stop-when-empty`) or agent unhealthy (matches Laravel) |
| 1 | Other fatal transport error (lost SQS lease, Redis connection loss after retries, ambiguous acknowledgement) |
| 2 | Configuration error at startup |
| 124 | Job timeout (§14) |

---

## 24. Public errors

Define a coherent typed exception hierarchy. Likely concepts include:

- configuration error;
- managed queue not found;
- payload too large;
- serialization/codec error;
- unsupported envelope version;
- unknown job;
- argument/schema mismatch;
- invalid queue option combination;
- agent unreachable/protocol error;
- transport error;
- job explicit failure;
- unsupported overflow payload.

Each error has one classification, used consistently by the worker, CLI, eager mode and transports:

| Class | Examples | Worker behavior |
|---|---|---|
| Dispatch error | configuration, queue not found, payload too large, invalid option | Raised to the caller; nothing sent |
| Deterministic job defect | malformed envelope, unsupported version, unknown job, codec or argument mismatch, `@pointer` | Terminal on first delivery |
| Handler failure | exception from the handler or its teardown | Retry policy applies |
| Fatal worker error | agent unhealthy, lost lease, ambiguous acknowledgement | Stop the worker |

`ManagedQueueNotFoundError` and `PayloadTooLargeError` expose stable fields (queue name; size and limit). Chained exceptions never carry secrets.

Do not expose AWS SDK or Redis client exceptions as the normal public contract when a stable package-level error is appropriate.

---

## 25. README requirements

`README.md` is a required product deliverable, not an afterthought.

It must include clear, copyable steps for:

1. installing core and FastAPI extra;
2. creating/configuring a FastAPI integration;
3. declaring a job;
4. dispatching synchronously and asynchronously;
5. using FastAPI dependencies in a job;
6. running the worker;
7. selecting/using named queues;
8. delays;
9. retry policy;
10. FIFO and fair-queue/message-group usage;
11. `JobContext` release/fail usage;
12. local development/direct SQS or LocalStack configuration;
13. eager testing;
14. running the demo/conformance suite;
15. understanding Laravel Cloud zero/low-config behavior, and the current platform status (managed queues not yet available for Python);
16. running on Laravel Cloud worker clusters today with self-managed SQS or Laravel Valkey (`LARAVEL_CLOUD_QUEUES_BACKEND`), including the `AWS_*` collision with object storage;
17. current support matrix;
18. known limitations and roadmap, especially compression/large-payload S3 offload, the native-code timeout limitation, best-effort failure records, and at-least-once delivery;
19. statement that `demo/` and `probe-app/` are development tooling and are not shipped in the PyPI package.

The README should lead with the simplest FastAPI path, then explain deeper concepts.

Do not write Laravel-flavored Python APIs merely for familiarity. Describe concepts in Python/FastAPI language while explaining the Laravel Cloud infrastructure they map to.

---

## 26. Packaging and release posture

### 0.x

Use `0.x` while:

- public FastAPI API is still changing;
- conformance gaps remain;
- live Cloud Python support is not yet verified;
- package naming/API adoption has not been internally settled.

### 1.0

Do not publish `1.0.0` until:

- core public API is intentionally stable;
- FastAPI API is intentionally stable;
- Cloud contracts are sufficiently verified;
- SemVer compatibility can be taken seriously.

After 1.0, breaking public API changes require a major version.

Clearly distinguish public modules from internal modules: document the public import surface; underscore-prefixed modules and subpackages are not stable.

---

## 26a. Security

- Message bodies are untrusted: follow §8's trust-boundary rules.
- Never log `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` credentials, SQS or Redis credentials, receipt handles or full job payloads by default.
- `failed_job` events carry job payloads and exceptions into Laravel Cloud's logs and dashboard. Document that job arguments should not contain secrets.
- Credential selection is explicit in every mode (§6); never fall back silently to ambient credentials.
- Refuse endpoint overrides when managed configuration is present.
- Keep TLS verification on for SQS and `rediss://` connections.
- The agent socket is an unauthenticated local socket: never proxy it to a network listener.
- Test sockets live in private temporary directories and are never created by deleting existing paths.
- This repository is public: never copy code or internal details from private Laravel repositories (see `docs/references.md`).

---

## 27. Platform support

Official v1 local/development support:

- Linux;
- macOS.

Windows:

- best effort;
- direct SQS/eager functionality should avoid unnecessary incompatibility;
- Windows-specific agent-emulator support is not a v1 release blocker.

Production Laravel Cloud runtime is Linux-oriented.

---

## 28. Out of scope for the first implementation

Do **not** allow these to expand v1:

- PHP job payload compatibility;
- PHP/Python cross-language job execution;
- Django production adapter;
- Flask production adapter;
- result backend / RPC-style `await job.result()`;
- internal worker concurrency >1;
- production Python failed-job database, dead-letter queues or failed-job retry commands;
- general-purpose job-framework positioning (self-managed SQS and Redis exist for Laravel Cloud worker clusters);
- `retry_until`, `max_exceptions` and memory-limit worker recycling;
- Laravel overflow (`@pointer`) payloads and credential caching;
- porting Laravel's `FailedJobProvider` fetch path or emitting `retried_at`;
- Trio;
- a supervisor process for hard timeouts;
- database queue backend;
- job chains;
- batches;
- unique jobs;
- debouncing;
- generalized queue middleware ecosystem;
- large-payload compression;
- S3/object-storage payload offload;
- arbitrary pickle/cloudpickle payloads;
- live Laravel Cloud Python deployment as a current CI/release gate.

Design extension points so later work remains possible without implementing these now.

---

## 29. Known future roadmap

Prominent TODOs/roadmap items should include:

1. compression;
2. transparent S3/object-storage large-payload offload;
3. Django adapter;
4. Flask adapter;
5. live Laravel Cloud managed-queue verification once Laravel Cloud enables managed queues for Python (platform work: allow FastAPI/Python in managed-queue validation; inject managed config and AWS container credentials into Python containers; allow a Python worker command; run the agent in Python worker containers);
6. live worker-cluster smoke test (D6c): manual and optional, available once the package can dispatch and consume;
7. `retry_until` and `max_exceptions`;
8. dead-letter handling and failed-job tooling outside managed mode;
9. evaluate advanced queue workflow features only after core compatibility is proven;
10. evaluate future Laravel Managed Queue changes detected by upstream drift CI.

---

## 30. Acceptance criteria

The initial implementation is acceptable when all of the following are true:

1. Package builds and installs cleanly on Python 3.10+.
2. `ty check` passes for shipped code, and Ruff `ANN` finds no missing annotations.
3. Core can dispatch and consume a versioned typed JSON job envelope.
4. FastAPI integration supports sync/async jobs, `Depends()`, per-job dependency cleanup, and app lifespan.
5. Workers are separate processes and one-job-at-a-time.
6. Cloud agent protocol works against the emulator with correct fatal/retry behavior.
7. Direct SQS works against LocalStack.
8. Retries demonstrably release the same message using visibility semantics.
9. Default attempts are 1 unless configured otherwise.
10. Fresh delays, retry delays, FIFO, and fair queues obey the expected constraints.
11. Terminal failures and deterministic decoding/unknown-job errors do not loop indefinitely.
12. Cloud lifecycle and failed-job observability pass the local compatibility tests.
13. Observability failure is nonfatal; agent result-report failure is fatal.
14. Timeout semantics behave at the process level.
15. Graceful shutdown finishes the current job and exits.
16. Queue name normalization/prefix/suffix behavior matches the pinned Laravel baseline.
17. Typed queue-not-found and payload-too-large errors work.
18. Eager testing is useful but distinct from infrastructure tests.
19. Optional W3C/OpenTelemetry propagation works when enabled.
20. `demo/` runs a broad compatibility suite and produces both human-readable and JSON reports.
21. The compatibility report records upstream baseline/source evidence.
22. `README.md` contains complete installation and usage instructions.
23. `demo/` is absent from the built PyPI artifacts.
24. CI passes all required quality gates.
25. No live Laravel Cloud deployment is required yet; that test is documented as pending.
26. `managed`, `sqs` and `redis` backends are selected only by `LARAVEL_CLOUD_QUEUES_BACKEND` or managed config, and ambiguous configuration fails clearly.
27. Self-managed SQS never uses ambient `AWS_*` credentials or endpoints unless explicitly opted in.
28. The Redis/Valkey backend passes its conformance suite against Valkey and Redis.
29. Retry policy travels in the message and survives a simulated deploy.
30. A captured failed payload replays as a fresh first attempt.
31. `failed_job` events follow the D1 size policy and Laravel's field set and order.
32. Timeouts exit 124 with the correct lifecycle event and redelivery in real subprocess tests.
33. Downstream typing samples pass `ty check`, including the `.options(...)` builder.
34. Wheel and sdist install cleanly outside the repository and exclude `demo/`, `probe-app/` and `docs/`.
35. The conformance report exits non-zero on any missing record or unapproved non-pass status.

---

## 31. Definition of success

Success is not merely “Python can push to and read from SQS.”

Success means a Python/FastAPI application can use Laravel Cloud queues — worker clusters today, Managed Queues as soon as the platform enables them for Python — in a way that is:

- operationally compatible with the Laravel 13.x Cloud queue contract;
- visible in Laravel Cloud's queue observability/failure model;
- safe under retries, worker crashes, timeouts, shutdown, and agent failures;
- idiomatic to Python/FastAPI;
- strictly typed;
- locally reproducible through a serious conformance harness;
- maintainable enough to plausibly become first-party Laravel tooling.
