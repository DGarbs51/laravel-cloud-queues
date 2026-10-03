# Architecture

How `laravel-cloud-queues` is put together, for application developers and for anyone
writing a framework adapter. The internal contract pack with lane ownership and test
conventions is [`contract/architecture.md`](contract/architecture.md); the delivery state
machine is [`contract/worker.md`](contract/worker.md).

## Two processes, one application

```text
 web / script process                       worker process
 ───────────────────                        ──────────────
 job.dispatch_async(...)                    laravel-cloud-queues work myapp.main:app
   │ encode + validate                        │ enter app lifespan once
   ▼                                          ▼
 Producer.send(body) ──► broker ──► Consumer.receive() ──► decode + validate ──► handler
                        (SQS / agent /          │                                   │
                         Redis)                 └──── complete / release ◄──────────┘
```

The producer and the worker are the same Python application in two execution modes. They
share code and configuration, never process state: the worker may run on other hardware
and on a different deployment revision. Everything a job needs travels in the message,
including its retry policy.

## Layers

```text
laravel_cloud_queues.fastapi     adapter: LaravelCloudQueues, per-job Depends() scope, lifespan
        │
registry / jobs / codecs         core: Registry, Job, RetryPolicy, JobContext, envelope,
        │            \           annotation-driven codecs, one dispatch and one execution path
transports            observability
  base.py sqs/ agent/ redis/     events, log socket, failure records, trace propagation
        │
worker / cli                     delivery state machine, timeouts, watchdog, signals, exit codes
```

**Registry and jobs** (`laravel_cloud_queues.Registry`, `Job`). A registry maps stable wire
names to handlers. `@registry.job(...)` wraps a function in a typed `Job` that is still
directly callable and gains `dispatch`, `dispatch_async` and `.options(...)`. The registry
also owns the codec registry, the lazily loaded configuration, the backend, telemetry and
the *invoker* (how handlers are called; the FastAPI adapter supplies one with dependency
injection). Job lookup is registry-only: an unknown name is a terminal defect, never an
import.

**Envelope and codecs** (`jobs/envelope.py`, `codecs/`). Messages are one JSON object with
Laravel's top-level `uuid` and `displayName` keys, plus a versioned `laravel_cloud_queues`
section carrying job name, arguments, retry policy, dispatch queue and trace context.
Argument decoding is driven by the handler's type annotations; the payload never names a
Python type. Decoding is bounded (16 MiB, depth 64) and rejects NaN/Infinity, duplicate
keys and Laravel overflow `@pointer` bodies.

**Dispatch pipeline** (`jobs/dispatch.py`). `dispatch` and `dispatch_async` share one
pipeline: resolve the queue (options > job default > backend default), validate options
against the queue kind and backend (FIFO/fair/delay rules), encode arguments, build the
envelope, check the size limit, send, then emit `queued` in managed mode. The async form
runs the blocking part in a worker thread.

**Execution path** (`jobs/execution.py`). `prepare_execution` decodes and validates a body
against the registry (job defects surface here); `run_prepared` activates trace context,
sets the current `JobContext`, calls the invoker (handler plus per-job teardown) and returns
the outcome. The worker and eager test mode use exactly this path.

**Transports** (`transports/`). Synchronous, body-opaque `Producer` and `Consumer`
protocols over `str` bodies and `Delivery` records (message ID, receipt, logical queue,
attempt count). They never see envelopes or jobs. `Producer` and `Consumer` are separate
because managed mode sends through SQS but receives through the Laravel Cloud agent.

| Backend | Producer | Consumer | Retry | Attempt count |
|---|---|---|---|---|
| `managed`, agent enabled | SQS `SendMessage` | agent `GET /next`, `POST /result` over a Unix socket | `released` with delay to the agent | `ApproximateReceiveCount` |
| `managed`, agent disabled; `sqs` | SQS `SendMessage` | SQS `ReceiveMessage` (long poll, one message), delete / `ChangeMessageVisibility` | visibility change on the same message | `ApproximateReceiveCount` |
| `redis` | `RPUSH` / `ZADD` delayed | Lua reserve (migrate due delayed + expired reserved, pop, count attempt, reserve) | move reserved -> delayed, same job ID | reservation counter |

**Observability** (`observability/`). Laravel Cloud lifecycle events and `failed_job`
records as NDJSON over the log socket in managed mode (with the size policy from D1);
job and failure log lines through `laravel-cloud-logging` in every mode; optional OpenTelemetry trace
propagation. All best-effort: a telemetry failure is logged and never raised.

**Worker and CLI** (`worker/`, `cli/`). One process, one in-flight delivery, AnyIO on
asyncio on the main thread. Per delivery: receive, decode, pre-run attempt check, arm
`setitimer`, start the lease watchdog thread, run, disarm, report exactly one outcome.
Timeouts exit the process with 124 from the signal handler. `SIGTERM` finishes the current
job before exiting. Exit codes 0/1/2/124 are documented in the README. The CLI is a `click`
group (`cli.cli`); `cli.main(argv)` wraps it for the console script and returns the exit
code.

## Configuration flow

`load_config()` reads `LARAVEL_CLOUD_QUEUES_BACKEND` (or detects managed configuration)
and returns a frozen `QueueConfig` with exactly one backend section. Nothing ambient
(`REDIS_URL`, `AWS_*`) selects a backend; `sqs` mode never touches the boto3 default chain
or `AWS_ENDPOINT_URL` unless explicitly opted in. Code arguments to `load_config()` win
over the environment. `Registry(config=...)` accepts the result; without it the registry
loads configuration lazily on first dispatch or worker start, and never in eager test mode.

## Extension points

**Framework adapters** (Django, Flask, ...) build on four seams and do not touch
transports or the worker:

1. **`Invoker`** (`laravel_cloud_queues.registry.Invoker`): `is_injected(parameter)` decides
   which handler parameters are runtime-injected rather than serialized;
   `invoke(job, args, kwargs, context)` calls the handler and runs any per-job teardown
   before returning. The FastAPI adapter's invoker resolves `Depends()` in a fresh scope per
   delivery.
2. **`WorkerTarget`** (`laravel_cloud_queues.registry.WorkerTarget`): anything with a
   `registry` property and a `lifespan()` async context manager. The CLI resolves
   `module:attr` to a `Registry`, an app whose `state.laravel_cloud_queues` is a target, or
   any object satisfying the protocol, by duck typing; the worker never imports a framework.
3. **Registration sugar**: an adapter wraps `Registry.job(...)` with framework-native
   declaration (decorators, settings, management commands) while keeping the same wire
   contract, so a Django producer and a FastAPI worker could share a queue.
4. **CLI**: the `click` group `laravel_cloud_queues.cli.cli` mounts into a click-based host
   CLI (`app.cli.add_command(cli, "queues")` in Flask). A Django management command can
   delegate to `laravel_cloud_queues.cli.main(argv)`, which returns the exit code.

**Codecs**: register a `Codec` (tag, Python type, `encode`, `decode`) on the registry's
`CodecRegistry` to serialize an application type; the tag is only ever resolved through
the trusted annotation, never from payload content.

**Transports**: a new broker implements `Producer`/`Consumer` from `transports/base.py`.
Only the SQS, agent and Redis transports are part of the product; the transport interface
may still change during 0.x.

## Where the Laravel Cloud contract lives

| Laravel Cloud concept | Python side |
|---|---|
| `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` | `config/_loader.py`, `ManagedQueuesConfig` |
| Queue agent `GET /next` / `POST /result` | `transports/agent/` |
| SQS queue URL prefix/suffix rules | `transports/sqs/` (`SqsQueue::getQueue`/`suffixQueue` semantics, `normalize_queue`) |
| Lifecycle events, `failed_job`, log socket | `observability/` |
| Retry/backoff/timeout semantics of `queue:work` | `jobs/policy.py`, `worker/` |
| Payload `uuid`/`displayName` | `jobs/envelope.py` |

Each behavior is traceable to the pinned upstream source through the conformance catalog
([`contract/catalog.json`](contract/catalog.json)); intentional differences are listed in
[`deviations.md`](deviations.md).
