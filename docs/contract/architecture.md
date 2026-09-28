# Architecture and contract

Contract pack for the v0.1 build (AGENT_BUILD_PROMPT.md "Contract pack"). The typed
interfaces in `src/laravel_cloud_queues/` are the contract of record; this file explains
how they fit together. Changing a contract signature needs the lead's approval.

| # | Contract artifact | Where |
|---|---|---|
| 1 | Configuration models, backend selection | `config/_models.py`, `config/_loader.py` docstring |
| 2 | Envelope v1 | `jobs/envelope.py` (wire example in module docstring) |
| 3 | Delivery state machine, outcome ownership | [worker.md](worker.md) |
| 4 | Error hierarchy + classification | `errors.py` |
| 5 | Transport interface (agent, SQS, Redis) | `transports/base.py` |
| 6 | Lifecycle + `failed_job` fixtures | `tests/fixtures/events/` (lane L7, from `upstream-evidence.md`) |
| 7 | Agent protocol fixtures | `tests/fixtures/agent/` (lane L4) |
| 8 | Public API signatures | `registry/`, `jobs/job.py`, `jobs/context.py`, `fastapi/`, `testing/` |
| 9 | CLI commands, options, exit codes | [cli.md](cli.md) |
| 10 | Conformance catalog | `catalog.json` + `catalog.schema.json` (lane L0R) |

## Layers

```
fastapi/  (adapter: Invoker with per-job DI scope, lifespan, WorkerTarget)
   |
registry/ jobs/ codecs/  (core: declaration, envelope, validation, dispatch, execution)
   |                \
transports/          observability/  (events, socket, failure records, tracing)
 base.py  sqs/ agent/ redis/
   |
worker/ cli/  (delivery state machine, timeouts, watchdog, signals, exit codes)
```

- **Transports are synchronous and body-opaque.** They move `str` bodies and return
  `Delivery` records. They never see envelopes, jobs or events. Async code offloads them with
  `anyio.to_thread.run_sync`. `Producer` and `Consumer` are separate because managed mode
  sends via SQS but receives via the agent.
- **Core owns every job semantic.** One dispatch pipeline (`jobs/dispatch.py`) serves
  `dispatch` and `dispatch_async`. One execution path (`jobs/execution.py`) serves the worker
  and eager mode.
- **Decoding is annotation-driven.** Payload data never names a Python type, module or
  callable. The registered handler's type hints decide what each argument decodes into.
- **Only FastAPI code imports FastAPI** (`fastapi/`). The worker resolves targets by duck
  typing (`WorkerTarget`), never importing FastAPI itself.
- **Observability is mode-aware.** Cloud events go to the log socket in managed mode only
  (D12). In `sqs`/`redis` modes the worker writes structured JSON lines to stdout.

## Key flows

**Dispatch.** `job.options(...).dispatch_async(**kw)` -> `prepare_dispatch` (queue resolution,
option validation, argument encoding, envelope, size check; pure CPU) -> `to_thread(send_prepared)`
(`producer.send`, then `queued` event in managed mode) -> `DispatchReceipt`.

**Worker.** `anyio.run(..., backend="asyncio")` on the main thread. Enter the target's
lifespan once. Loop: `to_thread(consumer.receive)` -> `started` -> `prepare_execution`
(job defects are terminal) -> pre-run attempt check -> arm `setitimer` + start watchdog
thread -> `run_prepared` (handler + per-job teardown; sync handlers run on the main thread)
-> disarm, stop watchdog -> report outcome. Managed mode completes a terminal message before
`failed_job` -> `failed`; self-managed `sqs`/`redis` writes the failure line to stdout before
completion and sends no socket events (D6b/D12). Completion events are immediate, and the
timer excludes reporting/rest (`completion-event-immediate`, `timeout-window-handler-only`,
both project deviations; D13.2). See worker.md for outcomes and acknowledgement failures.

**Eager testing.** `registry.testing(eager=True)` swaps the dispatch send step for a recorder
that immediately runs `prepare_execution` + `run_prepared` on the encoded body (same code
path as the worker; handler exceptions surface to the test). Inside a running loop,
`dispatch_async` awaits in that loop; sync `dispatch` of an async handler inside a running
loop runs it on a helper thread with its own loop (no nested loop).

## Lead decisions

Recorded as **D13** in `docs/decisions.md` (terminal-failure order, event timing, `GET /next`
retries, strict managed config, labeled deviations, dependencies, envelope layout, Redis
lease expiry, explicit release).

## Lane ownership (paths)

| Lane | Paths |
|---|---|
| L3a config + SQS | `config/_loader.py`, `transports/sqs/`, `transports/__init__.py` (`create_backend`) |
| L3b envelope + codecs | `codecs/`, `jobs/envelope.py`, `jobs/signature.py` |
| L3c jobs, registry, dispatch, eager | `jobs/{policy,context,job,dispatch,execution}.py`, `registry/`, `testing/`, `tests/typing/` |
| L4 agent | `transports/agent/`, `tests/fixtures/agent/` |
| L5 redis | `transports/redis/` |
| L7 observability | `observability/`, `tests/fixtures/events/` |
| L6 worker + CLI | `worker/`, `cli/` |
| L8 FastAPI | `fastapi/` |
| L9 local platform | `harness/` |
| L10 demo + conformance | `demo/` |

Each lane also owns its tests under `tests/unit/<area>/` and `tests/integration/<area>/`.
`errors.py`, `config/_models.py`, `transports/base.py`, `__init__.py` and `pyproject.toml`
are lead-owned: request changes on the todo.

## Test conventions

- `uv run pytest` from the repository root. `harness` is importable (pytest `pythonpath`).
- Markers: `agent`, `sqs`, `redis`, `socket`, `subprocess`, `runtime_proof`, `packaging`,
  `conformance`. Services are skipped when absent, and fail when
  `LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1` (CI).
- SQS: `LARAVEL_CLOUD_QUEUES_TEST_SQS=moto|localstack` (moto `ThreadedMotoServer` locally).
- Redis: `LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL` (default `redis://127.0.0.1:6379/15`), unique
  key prefix per test, never FLUSHDB.
- Tests never read ambient `AWS_*`; set explicit dummy credentials.
