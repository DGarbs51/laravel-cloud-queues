# Executable conformance demo

`demo/` is repository-only. It is excluded from the wheel and source distribution,
as are the harness, tests and `probe-app/`. Run from a repository checkout:

```sh
uv sync
uv run python -m demo.conformance --sqs moto --report compatibility-report.json
```

Moto runs an isolated HTTP SQS server without Docker. Redis probes use Laravel Herd's
Valkey at `redis://127.0.0.1:6379/15`, unique key prefixes, and prefix-scoped cleanup;
they never flush a database. To use another service:

```sh
export LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL=redis://127.0.0.1:6379/15
export LARAVEL_CLOUD_QUEUES_TEST_REDIS_TLS_URL='rediss://localhost:6380/0?ssl_ca_certs=/path/ca.crt'
uv run python -m demo.conformance --sqs moto
```

The TLS probe requires a TLS-enabled Redis/Valkey service with certificate verification.
An unavailable service is a pytest skip locally, **but still fails the conformance gate**:
only the catalog's two approved live Cloud checks may skip. Set
`LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1` to fail immediately on fixture setup instead.
A full local run also executes packaging and typing checks, so it needs the development
dependencies and access to the package cache/index used by the existing packaging suite.

A quick, service-free check and a focused product check:

```sh
uv run python -m demo.conformance --only envelope.v1_shape envelope.codecs agent.receive_count_default -q
uv run python -m demo.conformance --only retry.exhaustion timeout.release_exit_124 -q
uv run pytest tests/conformance tests/contract -q
```

`--only` produces an explicitly selected report, not a whole-catalog release verdict.
Other pytest options pass through (use `--` before positional pytest arguments).
Deselected, unexecuted and unresolved required nodes cannot pass. Collection failures,
setup/teardown errors, duplicate feature IDs, uncovered scope requirements, missing probes,
unapproved skips/partial/unsupported results and pytest errors make the command exit 1.
The JSON report carries the pinned upstream baselines, git revision, UTC time, environment,
versions, sources, declared evidence tiers, deviations, per-node durations and observations.
One PASS/FAIL/etc. line per feature and summary counts are printed for humans.

The catalog at `docs/contract/catalog.json` is the mapping of record. A probe reference is
an exact pytest node ID; a reference to a parametrized function includes **all** its cases.
Demo probes declare `@pytest.mark.conformance("feature.id", tier="emulated")` (or the actual
lower tier) and can call `evidence.record(key, json_value)`, `evidence.observed(summary)` and
`evidence.status("partial" | "unsupported", reason)`. Explicit non-pass reports still need a
reviewed catalog exception. A declared probe tier below its feature's tier fails the gate.
Existing lane tests are referenced directly; their tier is the catalog's reviewed tier.
SQS emulation proves message attributes and client semantics, not live server fairness.

## Producer and worker

`demo.app:app` is a real FastAPI app bound to `LaravelCloudQueues`. It exposes
`POST /dispatch/{wire_job_name}` with JSON `{"kwargs": {...}, "options": {...}}`, plus
`GET /health`. The jobs include sync/async handlers, lifespan state, yield dependencies,
named queues, retries, explicit release/fail, timeouts, collisions with option names,
and large payloads. Poison-envelope fixtures are built in the managed probes.

For a manual Redis demonstration, start the worker and producer in different terminals:

```sh
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/15
export LARAVEL_CLOUD_QUEUES_REDIS_PREFIX=lcq-manual-demo:
uv run laravel-cloud-queues work demo.app:app
# In another terminal with the same environment:
uv run python -m demo.produce demo.sync --kwargs '{"label":"hello"}'
```

`demo.produce` uses FastAPI's TestClient to call the dispatch HTTP route in its own process,
so no ASGI server dependency is needed. It shares only the broker with the separate worker.
You may also serve the app using `uvicorn demo.app:app` if uvicorn is installed. This is a
local compatibility fixture with synthetic payloads, not a public application endpoint.
`LCQ_DEMO_EVENTS=/path/events.jsonl` records handler/dependency/lifespan observations without
arguments; `LCQ_DEMO_NAMED_QUEUE` selects an isolated named queue for concurrent probes.

Managed mode cannot set `_SQS_ENDPOINT`. Managed probes therefore **enqueue real encoded
wire envelopes directly into the agent emulator**; they do not pretend that the managed
producer sent through SQS. A separate socket-tier probe checks `queued` emission after a
successful producer return and its absence after a failed send. Real producer dispatch,
SQS attributes, retries, FIFO/fair options and ignored ambient `AWS_*` values are exercised
against moto/LocalStack in self-managed SQS mode.

The managed emulator and Unix log collector come from `harness/`. The worker runs via
`laravel-cloud-queues work demo.app:app` (the equivalent Python module entry point inside
probes). Short-lease tests use public `WorkerOptions`; `demo.fault_worker` deliberately
loses a response after an actual SQS delete to prove the worker stops on ambiguous ack.
Timeout probes assert exit 124, lifecycle output and redelivery counts; retry probes record
the original/retried message IDs and attempts. The native-call probe records delayed Python
signal handling, the documented timeout limitation.

Process logs and sanitized collector captures go under `conformance-artifacts/`. Full
payloads are compared in memory, then replaced with hashes and byte counts in collector
artifacts. Receipt handles and credential fields are redacted from report evidence. Eager
mode tests validate application execution/DI; they do not replace these transport tests.

## CI and platform status

The `conformance` GitHub Actions job provisions pinned LocalStack and Valkey services plus
verified TLS, then runs:

```sh
uv run python -m demo.conformance --sqs localstack --report compatibility-report.json
```

It always uploads the JSON report and `conformance-artifacts/`, and the aggregate `ci` gate
requires it. The main test matrix additionally runs against real Redis. LocalStack is the
authoritative SQS gate; moto is local convenience. A local report validates CI wiring and
runs quality tools; only GitHub can attest to the full remote matrix result.

Live Laravel Cloud managed queues and dashboard retry remain skipped with their explicit
catalog approvals until Python managed queues are supported (§1). No live Cloud deployment
or paid service is needed. During lane integration, unmerged contract implementations fail
normally; they are not converted into temporary approvals or hidden skips.
