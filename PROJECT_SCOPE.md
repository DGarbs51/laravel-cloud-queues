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

### Upstream drift policy

Pin the baseline above for deterministic CI and conformance results. Add a separate upstream-drift check that detects newer Laravel 13.x releases and relevant changes without silently changing the baseline. Updating the compatibility baseline must be an intentional change with corresponding test and documentation updates.

---

## 3. Core product objective

Provide a PyPI package that lets a Python application use Laravel Cloud Managed Queues as a first-class hosted queue backend.

The package must:

1. Dispatch jobs from Python application processes to Laravel Cloud Managed Queues.
2. Run dedicated Python worker processes/containers that can execute on separate hardware from the producer.
3. Consume through the Laravel Cloud in-container queue agent when that agent is enabled.
4. Support direct SQS outside Laravel Cloud for local development, LocalStack, conformance testing, and compatibility/debug scenarios.
5. Match Laravel Cloud queue lifecycle, retry, queue-name, FIFO, fair-queue, failure, timeout, and observability semantics closely enough that Python jobs behave operationally like first-class Laravel Cloud Managed Queue jobs.
6. Provide a framework-native FastAPI experience while keeping the transport/runtime core independent from FastAPI.
7. Provide a conformance harness that objectively reports what matches the Laravel baseline and what does not.

Direct SQS is a tested compatibility/development mode. The package should **not** be positioned as a general-purpose SQS job framework; Laravel Cloud Managed Queues remain the production product center.

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
│       │   └── sqs/
│       ├── worker/
│       ├── observability/
│       ├── testing/
│       ├── cli/
│       └── fastapi/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conformance/
└── demo/
    └── ...
```

The precise internal layout can change if the implementation team finds a cleaner separation, but the public boundaries should remain clear.

### Published distribution

The top-level `demo/` directory is a development/conformance application and **must not be included in the PyPI wheel or source distribution payload unless there is a compelling packaging reason explicitly approved later**. Configure the build backend/package inclusion rules and add a packaging test proving `demo/` is absent from the built distribution.

Tests, local emulators, and development-only assets should likewise not accidentally inflate the runtime wheel.

### Optional dependencies

One PyPI distribution is required.

Initial intended install shape:

```text
pip install laravel-cloud-queues
pip install "laravel-cloud-queues[fastapi]"
```

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

CI must exercise the supported version matrix. The implementation should avoid unnecessarily raising the minimum version.

### Typing

`mypy --strict` is a release gate for shipped package code.

Requirements:

- all public APIs fully typed;
- preserve useful callable signatures through decorators where practical;
- minimize `Any`;
- unavoidable `Any` must be localized and justified;
- include `py.typed`;
- exported generic types should be useful to downstream applications;
- public decorator and dispatch typing should give strong IDE/mypy behavior;
- tests may use narrowly scoped typing relaxations where justified, but production package code must remain strict.

### Linting/formatting

Use a modern, minimal Python toolchain. Ruff is a suitable default for linting/formatting unless the agents identify a concrete reason otherwise.

### CI release gates

A release must pass:

- Python 3.10+ matrix;
- `mypy --strict`;
- lint/format checks;
- unit tests;
- FastAPI integration tests;
- LocalStack/SQS tests;
- Cloud-agent emulator tests;
- observability socket tests;
- compatibility/conformance suite;
- package build/install smoke tests;
- `py.typed` verification;
- verification that `demo/` is excluded from published artifacts.

Real Laravel Cloud Python deployment verification is expected later but is explicitly **not** a current release gate.

---

## 6. Configuration contract

### Laravel Cloud production configuration

`LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is the canonical production configuration source.

The package should auto-detect Laravel Cloud configuration and require minimal/no manual AWS or queue wiring in a Cloud deployment.

Known baseline shape includes concepts such as:

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
    "credentials": "ecs"
  },
  "agent": {
    "enabled": true,
    "socket": "/tmp/cloud-agent.sock"
  }
}
```

The exact parser must be based on current pinned upstream behavior rather than this illustrative shape.

### Validation

Parse configuration once into strongly typed internal models and validate eagerly at startup.

Requirements:

- fail fast with package-level configuration errors;
- validate required fields and supported driver modes;
- preserve unknown fields where practical for forward compatibility;
- never silently reinterpret malformed Cloud configuration;
- local explicit settings/env overrides are allowed for local development and test transports;
- Laravel Cloud-injected assignment remains authoritative in Cloud.

### Queue URL/name rules

Match Laravel's prefix/suffix semantics, including FIFO suffix placement:

- standard: `{prefix}/{queue}{suffix}`
- FIFO: `{prefix}/{base}{suffix}.fifo`

Queue normalization for observability must invert those rules and return the logical managed queue name.

Dispatching to an unprovisioned/nonexistent managed queue must raise a dedicated typed package error such as `ManagedQueueNotFoundError`. Never silently fall back to the default queue and never auto-create hosted queues.

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
2. opt-in automatic package discovery — convenience feature.

Do not require recursive implicit scanning.

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
- trace/context metadata;
- future extension without breaking v1.

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

Before sending, measure the fully encoded message body against the actual transport limit.

If oversized, raise a stable typed `PayloadTooLargeError` containing useful size/limit information.

Never silently truncate, compress, or offload in v1.

Add a prominent roadmap/TODO for:

- transparent compression;
- S3/object-storage large-payload offload;
- configurable thresholds;
- transparent worker hydration/cleanup.

Laravel's own evolving overflow/large-payload implementation should be reviewed when that future work begins.

---

## 9. Dispatch API

Support both synchronous and asynchronous dispatch.

Illustrative API:

```python
receipt = send_email.dispatch(user_id=123)
receipt = await send_email.dispatch_async(user_id=123)
```

Both paths must share the same validation, serialization, routing, tracing, and transport semantics.

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

A job can declare a default queue and dispatch can override it.

```python
@queues.job(queue="emails")
async def send_email(...) -> None:
    ...

await send_email.dispatch_async(..., queue="priority")
```

The exact technique for distinguishing job arguments from dispatch options must be unambiguous and type-friendly. A builder/options object may be preferable to injecting transport keywords into a job's own keyword namespace; the agents should choose an API that cannot collide with legitimate handler parameters.

### Delays

Fresh-message delays on standard SQS queues must respect the SQS per-message maximum of 900 seconds.

Invalid longer delays must fail loudly.

FIFO queues do not support per-message delay. Reject that combination.

Retries are different from fresh delays: retry backoff uses message visibility and can extend up to SQS's 12-hour visibility limit.

### FIFO

Queues ending in `.fifo` use FIFO semantics.

Support:

- message group ID;
- deduplication ID;
- sensible defaults compatible with Laravel/SQS;
- strict validation.

Default FIFO group should match Laravel's behavior where appropriate (logical queue name). Default dedup ID should be unique when content-based dedup is not being explicitly relied upon.

### Fair queues

For standard queues, support SQS message groups as fair-queue tenant keys.

Do not conflate standard-queue fair message groups with FIFO ordering groups. Reject invalid cross-model combinations rather than silently ignoring attributes.

---

## 11. Transport model

### Dispatch path

Publishing uses SQS with the Cloud-provided queue URL configuration.

Use `boto3` as the canonical direct SQS SDK.

Synchronous calls can use boto3 directly. Async-facing APIs must isolate blocking boto3 operations so they never block the FastAPI/AnyIO event loop.

### Receive path on Laravel Cloud

When the injected agent config says the agent is enabled, receive through the in-container Laravel Cloud agent over its Unix socket.

Do not decide agent use merely by probing socket existence; treat Cloud configuration as authoritative.

Baseline socket default: `/tmp/cloud-agent.sock` when not otherwise configured.

Baseline behavior:

- `GET /next`
- long-poll timeout long enough to outlast the agent poll cycle (Laravel baseline uses 65 seconds);
- HTTP 204 means no work;
- HTTP 200 must contain a valid structured message;
- unexpected status, malformed response, or unreachable agent is a transport-fatal condition.

Message data includes concepts such as:

- `messageId`;
- `receiptHandle`;
- `body`;
- `attributes`;
- `queueUrl`.

### Report path

The agent owns the SQS terminal operation for agent-delivered messages.

Report outcome via:

- `POST /result`
- status `processed`, or
- status `released` with delay.

Use bounded retries for transient connectivity issues. If reporting ultimately fails, treat the worker as fatally unhealthy:

- do not fetch another job;
- do not assume acknowledgement happened;
- terminate the worker;
- allow agent/SQS visibility semantics to cause later redelivery.

At-least-once execution and duplicate possibility after ambiguous acknowledgement must be documented. Applications should design jobs with normal queue idempotency expectations.

### Direct SQS receive mode

Outside Cloud/agent mode, direct SQS receive is supported for local development and compatibility tests.

Use long polling and request `ApproximateReceiveCount`.

For direct mode:

- success deletes the original message;
- retry calls `ChangeMessageVisibility` on the original message;
- terminal failure deletes after failure reporting;
- never create a duplicate message merely to implement retry.

Direct SQS mode should be fully tested but not marketed as the primary production use case.

---

## 12. Retry and failure semantics

Match Laravel/SQS-native retry semantics.

### Attempts

`ApproximateReceiveCount` is the source of truth for the delivery attempt count.

Default `tries` is **1**, matching Laravel's worker default unless overridden by job/worker policy.

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

Visibility timeout is whole seconds and must not accidentally floor positive sub-second retry delays to zero. Mirror the protective behavior in the current cross-framework reference: positive sub-second delays should round upward; explicit zero can remain zero.

Clamp retry visibility to 43,200 seconds (12 hours).

### Terminal failed jobs

There is no independent Python production failed-job store.

On terminal failure:

- emit Cloud-compatible failed-job information;
- complete/delete the queue message according to agent/direct transport semantics;
- let Laravel Cloud own operational failed-job inspection/retry workflows.

Local harnesses may capture failures for assertions only.

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

Use **AnyIO** for core async orchestration.

- async handlers execute naturally;
- sync handlers execute directly in the worker process;
- do not use a thread pool for sync handler execution merely for throughput;
- process-level failure/timeout semantics remain enforceable.

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

If the platform forcibly kills the process before completion, rely on agent/SQS visibility for redelivery.

### Worker lifecycle options

Provide useful framework-neutral CLI controls including equivalents of:

- `--max-jobs`;
- `--max-time`;
- `--stop-when-empty`;
- `--stop-when-empty-for`;
- any small rest/sleep controls justified by local/direct mode.

On Laravel Cloud, queue assignment remains authoritative.

### Queue selection

Canonical CLI concept:

```text
laravel-cloud-queues work myapp.main:app
```

On Cloud, the injected worker/queue assignment is authoritative. Do not pretend a conflicting CLI queue selection overrides hosted worker assignment.

Outside Cloud/direct mode, allow explicit queue selection, including multiple named queues where appropriate.

---

## 14. Timeout semantics

Timeouts are process-level correctness behavior, not merely asyncio cancellation.

Requirements:

- job-level timeout;
- worker-level default;
- explicit fail-on-timeout behavior;
- recognizable nonzero process exit on timeout, preferably matching Laravel's Cloud timeout exit semantics where applicable (baseline Laravel uses exit status 124);
- by default, a timed-out retryable job should be released rather than terminally failed;
- fail-on-timeout converts it to terminal failure;
- synchronous native/blocking code must not defeat timeout correctness simply because Python cannot cancel a thread.

Design implementation carefully around Python process/signal behavior and platform constraints. Conformance tests must prove the observable semantics, not merely that an asyncio timeout exception occurred.

---

## 15. Observability and Laravel Cloud dashboard parity

Dashboard parity is a v1 release blocker.

### Cloud lifecycle events

Emit Laravel Cloud-compatible queue lifecycle events:

- queued;
- started;
- processed;
- released;
- failed.

Match queue normalization, timestamp precision/format, duration fields, and event schema to the pinned Laravel baseline.

### Failed job event

Match the current Laravel Cloud failed-job event contract closely enough for the dashboard to identify and display Python failures.

Relevant fields include concepts such as:

- event type;
- failure ID (UUIDv7 where compatible);
- queue;
- started timestamp;
- attempts;
- payload;
- job name/display name;
- exception preview;
- exception detail.

The agents must inspect the exact pinned Laravel and `symfony-on-cloud` source before freezing this schema. If the cross-framework adapter contains a temporary platform workaround (for example, log-line size handling) that differs from framework behavior, document the difference, encode it in conformance evidence, and choose the behavior that maximizes actual Laravel Cloud dashboard compatibility.

### Socket protocol

Laravel Cloud observability uses newline-delimited JSON over a persistent stream/Unix socket.

Baseline log socket:

- env override `LARAVEL_CLOUD_LOG_SOCKET`;
- default `unix:///tmp/cloud-init.sock`;
- short connection/write timeout;
- one JSON object per line.

### Failure policy

Observability is best-effort.

An observability socket outage must never convert a successful queue job into a failed job.

Record/log telemetry degradation locally and let the conformance report surface it.

Agent `/result` reporting is **not** observability; it is part of queue correctness and remains fatal when unavailable after bounded retries.

---

## 16. Tracing

Support optional OpenTelemetry/W3C trace-context propagation in v1.

Requirements:

- no hard OpenTelemetry dependency in core;
- versioned envelope metadata/context section;
- inject standard trace context on dispatch when tracing is available;
- extract/activate it on worker execution;
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

Do not blindly commit to this exact signature if it creates mypy or parameter-collision problems. The final API must meet the semantics while remaining idiomatic and statically typeable.

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

It must still exercise:

- argument binding;
- payload validation;
- serialization/deserialization;
- handler registration;
- FastAPI DI and cleanup where applicable.

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
- payload size behavior where feasible.

### Agent emulator

Build a local Laravel Cloud queue-agent emulator that exposes the baseline protocol over a Unix socket:

- `GET /next`;
- `POST /result`;
- message holding/state;
- result capture;
- visibility/release behavior sufficient to verify client semantics;
- deterministic fault injection for disconnects, malformed responses, HTTP errors, delayed responses, etc.

It need not reproduce unrelated Laravel Cloud implementation internals. It exists to provide a deterministic compatibility contract.

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
- trace-context propagation when optional tracing is installed;
- eager testing behavior.

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
- environment metadata;
- Python/package/framework versions.

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

The suite must not mutate its expectations merely to make the Python implementation pass.

When Python intentionally differs because of language/framework constraints, report `partial` or a documented justified deviation instead of pretending full compatibility.

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

CLI errors should be actionable and package-level, not raw boto3/HTTP stack traces by default.

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
- job explicit failure.

Do not expose AWS SDK-specific exceptions as the normal public contract when a stable package-level error is appropriate.

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
15. understanding Laravel Cloud zero/low-config behavior;
16. current support matrix;
17. known limitations and roadmap, especially compression/large-payload S3 offload;
18. statement that `demo/` is development/conformance tooling and is not shipped in the PyPI package.

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

Clearly distinguish public modules from internal modules.

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
- production Python failed-job database;
- generic AWS SQS product positioning;
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
5. live Laravel Cloud end-to-end smoke suite once the Python runtime is available;
6. evaluate advanced queue workflow features only after core compatibility is proven;
7. evaluate future Laravel Managed Queue changes detected by upstream drift CI.

---

## 30. Acceptance criteria

The initial implementation is acceptable when all of the following are true:

1. Package builds and installs cleanly on Python 3.10+.
2. `mypy --strict` passes for shipped code.
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

---

## 31. Definition of success

Success is not merely “Python can push to and read from SQS.”

Success means a Python/FastAPI application can use Laravel Cloud Managed Queues in a way that is:

- operationally compatible with the Laravel 13.x Cloud queue contract;
- visible in Laravel Cloud's queue observability/failure model;
- safe under retries, worker crashes, timeouts, shutdown, and agent failures;
- idiomatic to Python/FastAPI;
- strictly typed;
- locally reproducible through a serious conformance harness;
- maintainable enough to plausibly become first-party Laravel tooling.
