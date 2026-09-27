# Multi-Agent Build Prompt — `laravel-cloud-queues`

You are an orchestrated engineering team consisting of Codex, Claude Code, and Grok 4.7. Build a production-quality Python package named **`laravel-cloud-queues`** that integrates Python applications with **Laravel Cloud Managed Queues**.

You are not brainstorming. You are expected to research the pinned upstream contracts, implement the package, implement the FastAPI adapter, build the local compatibility infrastructure and demo application, run tests, review each other's work, and leave the repository in a release-quality `0.x` state.

The repository should contain a detailed `PROJECT_SCOPE.md`. Treat that document as the authoritative product scope. This prompt defines execution priorities and acceptance criteria; when more detail is needed, consult `PROJECT_SCOPE.md`.

## Mission

Create a Python 3.10+ package that lets a Python application dispatch jobs from its app/web layer and process them in dedicated queue workers running on separate hardware using Laravel Cloud Managed Queues.

Compatibility is with **Laravel Cloud's hosted Managed Queue infrastructure and semantics**, not with Laravel's PHP queue API. The producer and worker are the same Python application in different processes/deployments. Do not attempt PHP/Python job payload interoperability.

The package should feel native to Python and to the framework being used. FastAPI is the first complete framework integration. The core must remain framework-independent so Django and Flask adapters can be added later with native framework ergonomics.

## Upstream baseline

Pin initial conformance research to:

- `laravel/framework` tag **v13.33.0**
- `laravel/symfony-on-cloud` commit **50c945170b6cb5690370d15fd725c6f82495e9ba**

Before coding, independently inspect the upstream source. At minimum read:

### `laravel/framework` v13.33.0

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
- relevant 13.x queue documentation

### `laravel/symfony-on-cloud` pinned commit

- `src/Queue/Agent/AgentClient.php`
- `src/Queue/ManagedQueueConfig.php`
- `src/Queue/Messenger/CloudQueueTransport.php`
- `src/Queue/QueueEventSubscriber.php`
- `src/Observability/Events.php`
- relevant tests and README

Laravel Framework is the canonical semantic reference. `symfony-on-cloud` is the cross-framework precedent.

Record exact upstream files/commits behind each conformance expectation. Do not change expected behavior just to make the Python implementation pass.

Also implement an **upstream drift check** that can alert on newer Laravel 13.x changes without silently changing the pinned baseline.

## Non-negotiable engineering constraints

- Python **3.10+**
- `mypy --strict` on shipped package code
- include `py.typed`
- MIT license
- package distribution name: `laravel-cloud-queues`
- import namespace: `laravel_cloud_queues`
- use a `src/` layout
- one PyPI package
- FastAPI provided as an optional extra
- current supported FastAPI generation / Pydantic v2; no Pydantic v1 legacy branch
- AnyIO for core async orchestration
- boto3 for direct SQS
- Linux + macOS officially supported for local/dev; Windows best effort
- no pickle/cloudpickle default serialization
- no result backend
- no internal production failed-job database
- no worker concurrency >1 in v1
- no PHP payload compatibility
- no live Laravel Cloud deployment requirement in the current acceptance gate

## Repository deliverables

At minimum produce:

```text
pyproject.toml
README.md
LICENSE
PROJECT_SCOPE.md
src/laravel_cloud_queues/...
tests/...
demo/...
```

The top-level **`demo/` directory is mandatory** and contains the FastAPI compatibility/conformance application.

**`demo/` must be excluded from the wheel and source distribution published to PyPI.** Add a packaging test that builds the artifacts and proves `demo/` is absent.

Do not ship nonfunctional Django/Flask adapters. Design their extension points and document their future ergonomics, but implement only core/vanilla + FastAPI now.

## Required architecture

Create clean separations for:

1. configuration;
2. job registry/definition;
3. versioned JSON codec/envelope;
4. dispatch;
5. SQS transport;
6. Laravel Cloud agent transport;
7. worker runtime;
8. retry/timeout/failure policy;
9. observability;
10. FastAPI adapter;
11. testing/eager mode;
12. conformance catalog/reporting;
13. CLI.

Avoid a monolithic queue class.

### Configuration

`LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is authoritative on Laravel Cloud.

Parse it into typed configuration eagerly at startup. Preserve unknown fields where practical. Validate invalid configuration clearly.

Support Cloud queue prefix/suffix rules, including FIFO URL construction where the environment suffix is inserted before `.fifo`.

Use the injected agent enabled flag as authoritative for agent receive behavior rather than probing socket presence.

Queue assignment in a Cloud worker is authoritative. Local/direct SQS mode can accept explicit queue selection.

### Cloud agent protocol

Implement the current Laravel Cloud agent behavior from the pinned sources.

Expected concepts include:

- Unix socket, conventionally `/tmp/cloud-agent.sock`;
- `GET /next`;
- long poll;
- 204 = no job;
- structured 200 response containing message ID, receipt handle, body, attributes and queue URL;
- `POST /result`;
- `processed`;
- `released` plus optional delay.

Agent result reporting is part of queue correctness. Use bounded retries for transient failures. If a processed/released result cannot ultimately be reported:

- stop;
- do not fetch another message;
- terminate the worker as unhealthy;
- do not assume acknowledgement succeeded;
- rely on visibility/redelivery.

Build an agent emulator with deterministic fault injection to test these cases.

### SQS transport

Publishing goes to SQS.

Outside Cloud/agent receive mode, support direct SQS receive for local development and conformance.

Use `ApproximateReceiveCount` as attempt count.

Retries must modify visibility on the **same original SQS message**. Never implement ordinary retry by sending a new message.

Use typed package errors rather than leaking ordinary AWS exceptions where a stable abstraction makes sense.

Direct SQS is a compatibility/dev mode, not the package's primary product identity.

## Job API

The API must be Python/FastAPI-native.

FastAPI should use decorator-first job definition through an explicit integration object.

A decorated function must remain directly callable.

Support:

- automatic default wire name from import path;
- explicit stable name override;
- default queue on declaration;
- queue override at dispatch;
- sync and async job handlers;
- sync and async dispatch;
- both positional and keyword job arguments;
- keyword args documented as preferred for long-lived job compatibility.

Core/vanilla Python should have a standalone registry rather than requiring a FastAPI app.

Support explicit job-module registration as the documented production default and opt-in auto-discovery as a convenience.

Unknown job names are terminal/non-retryable.

### Dispatch receipt

Dispatch returns a typed lightweight receipt containing at least:

- provider message ID;
- logical queue.

It is not a job result future.

## Serialization

Create a versioned JSON wire envelope.

It must carry enough information for:

- envelope version;
- unique job UUID;
- job wire name;
- display name;
- positional args;
- keyword args;
- extensible metadata/context;
- observability;
- future migrations.

Core codecs must cover sensible native types, dataclasses, enums, UUID/date/time types and extension hooks.

Pydantic v2 model support is optional/integration-aware, not a mandatory core dependency.

No arbitrary pickle/cloudpickle.

Schema/codec/envelope failures that are deterministic are terminal and non-retryable.

Before dispatch, measure the final encoded body and raise typed `PayloadTooLargeError` if it exceeds the transport limit.

Do not add compression or S3 offload now. Leave a **prominent roadmap/TODO** for:

- transparent compression;
- S3/object-storage pointers for large payloads;
- thresholds and transparent hydration.

## Queue semantics

Implement and test:

### Named queues

- logical queue names;
- prefix/suffix URL mapping;
- normalization for telemetry;
- queue-not-found typed error;
- never fall back silently.

### Standard delays

Fresh dispatch delays must obey SQS's 900-second per-message limit.

Fail loudly for impossible longer delays.

### FIFO

`.fifo` queues must support:

- message group ID;
- deduplication ID;
- safe/default values compatible with Laravel/SQS.

Reject per-message fresh delay on FIFO.

Reject standard/fair-queue-only grouping controls when used incorrectly on FIFO.

### Fair queues

Support message group ID on a standard queue as an SQS fair-queue tenant key.

Do not treat it as FIFO ordering/deduplication.

Reject FIFO-only options on standard queues.

## Retry and failure policy

Match Laravel/SQS semantics.

- default attempts/tries: **1**
- `ApproximateReceiveCount` is attempt count
- ordinary unhandled exceptions use the effective retry policy
- retry releases the same message via visibility
- retry visibility max 43,200 seconds
- positive sub-second retry delays must not accidentally become immediate retry because of flooring
- terminal failures are deleted/acknowledged after Cloud failure reporting
- deterministic payload/job/schema errors are immediately terminal

Support both decorator shorthand and reusable typed policy objects.

Provide a typed, non-serialized `JobContext` with:

- attempt;
- queue;
- message ID;
- explicit release;
- explicit terminal fail.

Ensure explicit release/fail cannot accidentally be followed by a normal success acknowledgement.

Do not create a Python failed-job database. Laravel Cloud owns operational failed-job state.

## Worker model

Workers are dedicated processes/containers and can run on different hardware than producers.

v1: **one in-flight job per worker process**.

Use horizontal scaling, not an internal async task pool.

Async jobs run in the AnyIO-driven worker runtime.

Sync jobs execute directly in the worker process rather than a thread pool.

### FastAPI worker behavior

The worker loads an explicit app target, e.g.:

```text
laravel-cloud-queues work myapp.main:app
```

Run the FastAPI application's lifespan once for the lifetime of the worker.

FastAPI jobs support `Depends()` where practical.

Each job gets a fresh dependency scope and `yield` dependency teardown. Request-only dependencies should fail clearly rather than being faked silently.

### Shutdown

On SIGTERM/SIGINT:

- stop fetching new jobs;
- finish current job;
- report outcome;
- perform dependency teardown;
- run lifespan shutdown;
- exit cleanly.

### Worker controls

Implement useful controls including:

- max jobs;
- max time;
- stop when empty;
- stop when empty for a period;
- any direct-mode sleep/rest controls that are justified.

Cloud-assigned queue remains authoritative in Cloud.

## Timeout behavior

Timeout correctness must be process-level.

Implement:

- worker default timeout;
- job override;
- fail-on-timeout option;
- retryable timeout releases the job;
- fail-on-timeout becomes terminal;
- recognizable nonzero worker exit, aligned with Laravel Cloud's baseline timeout exit behavior where feasible (Laravel baseline uses 124).

Do not claim timeout compatibility merely because an asyncio task was cancelled. Tests must prove externally observable worker/message behavior, including synchronous handlers.

## Observability

Dashboard parity is a release blocker.

Implement Laravel Cloud-compatible lifecycle events:

- queued;
- started;
- processed;
- released;
- failed.

Implement terminal failed-job event data sufficient for Laravel Cloud dashboard behavior.

Inspect the exact pinned framework and Symfony source before finalizing fields.

Use newline-delimited JSON over the Cloud log socket. Baseline default is `unix:///tmp/cloud-init.sock`, with `LARAVEL_CLOUD_LOG_SOCKET` override.

Observability emission is best-effort. Telemetry failure must not fail an otherwise successful job.

Again: agent `/result` reporting is not optional telemetry.

## Tracing

Implement optional W3C/OpenTelemetry trace-context propagation.

No hard OTel dependency.

Producer injects trace context when available. Worker restores it around handler execution.

Do not mix this with Laravel Cloud lifecycle-event requirements.

## Eager/application testing mode

Provide an eager/in-memory test mode for users.

It should still execute:

- binding;
- serialization/deserialization;
- registration;
- FastAPI dependency resolution/cleanup.

It may execute immediately and surface errors.

Allow useful dispatch assertions.

Explicitly distinguish it from transport/conformance tests.

## Local infrastructure and conformance

Build:

1. LocalStack SQS integration;
2. Cloud-agent Unix socket emulator;
3. Laravel Cloud observability socket collector;
4. conformance catalog;
5. compatibility report generator.

The tests must verify actual semantics rather than implementation details.

Important proof cases include:

- retry uses same message ID;
- `ApproximateReceiveCount` increments;
- processed deletes/acks;
- released does not duplicate;
- timeout behavior;
- agent reporting failure kills worker;
- observability failure is nonfatal;
- FIFO/fair validations;
- queue URL normalization;
- unknown job terminal;
- schema mismatch terminal;
- payload size guard;
- graceful shutdown;
- FastAPI lifespan/DI cleanup.

## `demo/` compatibility application

Build the test/reference FastAPI app in top-level **`demo/`**.

Its primary job is to exercise all supported compatibility features and tell engineers what works and what does not.

Do not turn this into a superficial showcase.

The demo should expose deterministic endpoints/jobs/fixtures and support producer and worker as separate processes.

It must generate:

1. readable terminal results;
2. machine-readable `compatibility-report.json`.

Allowed statuses:

- `pass`
- `fail`
- `partial`
- `skipped`
- `unsupported`

Each feature record must include:

- feature ID;
- expected behavior;
- observed behavior;
- evidence;
- errors;
- environment/package/framework versions;
- upstream Laravel/Symfony source reference.

Make results evidence-driven.

Example:

```text
PASS     retry.visibility_release
         message_id_before == message_id_after
         attempt_before=1 attempt_after=2
```

The live Laravel Cloud test should currently report `skipped` with an explicit reason rather than failing the suite.

Real Laravel Cloud Python deployment verification will be added when the environment becomes available. Prepare a documented smoke-test path, but do not make it a present release gate.

## CLI

Ship one canonical framework-neutral executable:

```text
laravel-cloud-queues
```

Include a worker command and useful inspect/conformance commands.

Future framework-specific wrappers may call into this core CLI/runtime.

## README is mandatory

Write a high-quality `README.md` with copyable, end-to-end usage steps.

It must cover:

- installation;
- FastAPI setup;
- declaring a job;
- sync dispatch;
- async dispatch;
- running a worker;
- FastAPI dependencies in jobs;
- named queues and overrides;
- delays;
- retries/backoff;
- explicit release/fail;
- FIFO;
- fair queues;
- eager tests;
- LocalStack/local development;
- running `demo/` conformance;
- Laravel Cloud auto-configuration;
- support matrix;
- limitations;
- roadmap, including compression/S3 large payload handling.

Lead with the shortest working FastAPI example.

Explicitly explain that `demo/` is repository-only conformance tooling and is excluded from the PyPI package.

## Scope exclusions

Do not spend v1 implementation time on:

- PHP/Python job interoperability;
- Django implementation;
- Flask implementation;
- chains;
- batches;
- uniqueness;
- debouncing;
- generalized queue middleware;
- job return/result backend;
- multiple concurrent jobs per worker;
- production Python failed-job storage;
- compression;
- S3 large-payload offload;
- arbitrary object pickle;
- general-purpose SQS product features unrelated to Laravel Cloud;
- live Cloud verification as a release blocker.

## Suggested multi-agent ownership

Use this as a starting split, but rebalance when useful.

### Claude Code — contract/architecture lead

- independently inspect Laravel Framework and Symfony sources;
- extract a contract matrix;
- challenge ambiguous semantics;
- propose clean Python architecture/public API;
- review observability/agent fidelity;
- review docs for correctness.

### Codex — implementation lead

- scaffold package;
- implement typed core;
- implement transports;
- implement worker;
- implement FastAPI adapter;
- implement CLI;
- implement tests/package build;
- keep `mypy --strict` green.

### Grok 4.7 — adversarial/conformance lead

- independently audit upstream behavior;
- attack edge cases;
- build/expand conformance scenarios;
- inspect SQS/FIFO/fair/retry boundaries;
- validate demo evidence;
- perform final compatibility-gap review.

All agents must review one another's work. Do not accept an implementation only because its author says it is complete.

## Execution sequence

1. **Research**
   - inspect pinned upstream source;
   - write a concise contract matrix;
   - identify exact compatibility obligations;
   - identify any framework-vs-Symfony differences.

2. **Architecture**
   - define typed public interfaces;
   - define envelope v1;
   - define exception hierarchy;
   - define config models;
   - define transport interfaces;
   - define conformance feature IDs.

3. **Scaffold**
   - pyproject;
   - package;
   - test matrix;
   - lint/mypy config;
   - build config excluding `demo/`;
   - README skeleton;
   - PROJECT_SCOPE.md.

4. **Core implementation**
   - codecs/envelope;
   - registry/jobs;
   - dispatch receipt;
   - policies/context;
   - config;
   - SQS;
   - agent;
   - observability;
   - worker/timeout/shutdown;
   - testing mode;
   - tracing.

5. **FastAPI implementation**
   - integration object;
   - decorator API;
   - app import target;
   - lifespan;
   - per-job Depends scope;
   - yield cleanup;
   - typed developer ergonomics.

6. **Local platform**
   - LocalStack;
   - agent emulator;
   - observability collector.

7. **Demo/conformance**
   - implement every required feature probe;
   - terminal output;
   - JSON report;
   - evidence.

8. **Docs**
   - complete README usage guide;
   - architecture notes;
   - source/baseline references;
   - known deviations;
   - roadmap TODOs.

9. **Independent review**
   - each agent reviews areas it did not author;
   - resolve discrepancies against upstream source;
   - run full CI matrix;
   - build wheel/sdist and inspect contents.

10. **Final report**
    - summarize completed features;
    - list exact conformance results;
    - list any `partial`, `unsupported`, or intentionally deferred items;
    - include commands to install, test, run demo, and build package;
    - do not claim full compatibility when evidence says otherwise.

## Release acceptance gate

Do not declare the build complete until:

- Python 3.10+ matrix passes;
- `mypy --strict` passes;
- lint/format passes;
- package builds/installs;
- `py.typed` is present;
- `demo/` is proven absent from wheel and sdist;
- core unit tests pass;
- FastAPI integration tests pass;
- LocalStack tests pass;
- agent emulator tests pass;
- observability tests pass;
- conformance suite produces its JSON report;
- no unexplained conformance failure remains;
- README has complete usage steps;
- large-payload compression/S3 TODO is clearly recorded;
- live Laravel Cloud test is documented but not required yet.

If a Laravel/Symfony contract is unclear, do not guess silently. Inspect source, compare tests, record the ambiguity, make the most defensible implementation choice, and add a focused conformance test.

Build this as if it may be transferred into the Laravel organization later.
