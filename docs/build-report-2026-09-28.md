# Build report — laravel-cloud-queues 0.1.0 (2026-09-28)

Final report of the orchestrated build described in `AGENT_BUILD_PROMPT.md`. The lead was Claude Opus 5.5 running the `solo-orchestrator` skill inside Solo. The run started 2026-09-27 21:20 EDT and finished 2026-09-28 00:20 EDT. The steps you should take yourself are in [`human-verification.md`](human-verification.md).

## Summary

- **Branch:** `main` (explicit stay-on-main run, D10). There was no PR. Final commit: `bf3c12d` (pushed), 93 commits since the run started.
- **Outcome:** release-quality `0.x` package, FastAPI adapter, local compatibility infrastructure, `demo/` conformance application and CI, all merged and green. The live Laravel Cloud smoke test on a worker cluster passes with the real package.
- **Paid usage:** none. Only included subscriptions were used (Claude Max, Codex Pro, Cursor Ultra). No Codex reset credits were used.

## Verification at `bf3c12d`

| Check | Result |
|---|---|
| GitHub Actions (push to `main`) | All jobs green: `lint` (ruff + actionlint), `typecheck` (mypy `--strict`), `test` on CPython 3.10–3.14 against LocalStack 4.14.0, Valkey 8.1.8, Redis 8.10.2 and a TLS-only Valkey with `LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1`, `packaging` (3.10, 3.14), `conformance` |
| Full test suite, local (Python 3.14, moto, Herd Valkey, local TLS Valkey) | 1,205 passed |
| Conformance gate, local and CI (`python -m demo.conformance`) | 80 `pass`, 2 approved `skipped`, gate PASS, exit 0 |
| Packaging | Wheel and sdist each contain only `laravel_cloud_queues/**` plus metadata, `README.md`, `LICENSE` and `pyproject.toml`. `py.typed` is present in both. `demo/`, `probe-app/`, `docs/`, `tests/`, `harness/`, `.github/` and `.cloud/` are absent. The core wheel installs on 3.10 outside the checkout and imports without FastAPI, Redis or OpenTelemetry. The sdist with `[fastapi]` installs on 3.14. The console entry point works. `conformance` outside a checkout exits 2 with guidance. |
| Live Laravel Cloud smoke test (probe-app, worker clusters, Laravel Valkey over TLS) | All six cases correct on the final deployment (see below) |

## Conformance results

The catalog of record is `docs/contract/catalog.json`: 82 records covering every §21 probe and every §30 criterion, each traced to pinned upstream source or to a project decision.

- `pass`: 80
- `skipped` (approved exception list only):
  - `cloud.live_managed` — Laravel Cloud does not yet enable managed queues for Python (§1).
  - `cloud.dashboard_retry_live` — same reason. The replay behavior itself passes locally against the agent emulator (`retry.dashboard_replay`).
- `fail`, `partial`, `unsupported`, `missing`: none.

The report command exits non-zero on any missing record or unapproved non-pass status. Runner tests cover that gate, and the gate also runs as a CI job.

## Completed features, mapped to §30

| §30 | Status |
|---|---|
| 1 Builds and installs on 3.10+ | Met: CI matrix 3.10–3.14; isolated wheel/sdist installs |
| 2 `mypy --strict` for shipped code | Met: 45 source files, CI `typecheck` |
| 3 Core dispatches and consumes a versioned typed envelope | Met |
| 4 FastAPI: sync/async, `Depends()`, per-job cleanup, lifespan | Met, including teardown before acknowledgement and FastAPI-parity exception propagation into `yield` dependencies |
| 5 Separate worker processes, one job at a time | Met |
| 6 Agent protocol vs emulator with correct fatal/retry behavior | Met |
| 7 Direct SQS vs LocalStack | Met in CI (moto locally) |
| 8 Retries release the same message | Met: evidence records identical message IDs, attempts 1 then 2 |
| 9 Default attempts 1 | Met |
| 10 Delays, retry delays, FIFO, fair queues constrained | Met |
| 11 Terminal/decoding failures don't loop | Met |
| 12 Lifecycle and `failed_job` observability | Met (socket-tier tests; managed mode only per D12) |
| 13 Observability failure non-fatal; agent result failure fatal | Met |
| 14 Process-level timeouts | Met |
| 15 Graceful shutdown | Met |
| 16 Queue name normalization matches Laravel | Met, including `Str::finish` repeated-suffix collapse |
| 17 Typed queue-not-found and payload-too-large errors | Met |
| 18 Eager testing useful and distinct | Met |
| 19 W3C/OpenTelemetry propagation | Met (`[otel]` extra; no leakage between jobs) |
| 20 `demo/` with human and JSON reports | Met |
| 21 Report records upstream baseline/source evidence | Met |
| 22 README complete | Met (all §25 items; runnable examples tested) |
| 23 `demo/` absent from artifacts | Met |
| 24 CI passes all gates | Met |
| 25 Live Cloud deployment not required, documented as pending | Met. Managed-queue live checks are pending the platform; worker-cluster live smoke test was also done and passes. |
| 26 Backend selected only by `LARAVEL_CLOUD_QUEUES_BACKEND` or managed config | Met |
| 27 Self-managed SQS never uses ambient `AWS_*` unless opted in | Met (tested with a hostile `AWS_*` environment) |
| 28 Redis/Valkey conformance vs Valkey and Redis | Met (Valkey locally and in CI, Redis in CI, TLS in CI and locally) |
| 29 Retry policy travels in the message | Met |
| 30 Captured failed payload replays as a fresh first attempt | Met (emulated; live check skipped, approved) |
| 31 `failed_job` D1 size policy, Laravel fields and order | Met |
| 32 Timeouts exit 124 with correct event and redelivery (subprocess) | Met |
| 33 Downstream typing samples pass | Met (positive and expected-error samples, including `.options()`) |
| 34 Wheel/sdist install outside the repo, exclusions | Met |
| 35 Report exits non-zero on missing record or unapproved non-pass | Met |

## Live Laravel Cloud smoke test (D6c)

`probe-app/` installs the package from this repository and runs `python -m laravel_cloud_queues.cli work main:app` on the App cluster (1 process) and the worker cluster (4 processes). It runs in `redis` mode against the environment's Laravel Valkey over TLS. One `POST /queue/e2e` from the web process produced:

| Case | Observed |
|---|---|
| `ok` (sync), `async_ok` | Processed on attempt 1 |
| `delayed` (5 s) | Processed on attempt 1 after the delay |
| `flaky_retry` (`tries` 2) | Released on attempt 1, processed on attempt 2 |
| `terminal_fail` (`tries` 2) | Released, then on attempt 2 a D6b failure record and `failed` |
| `timeout_then_terminal` (`tries` 2, timeout 3 s) | Exit 124, restarted by Laravel Cloud within about a second, redelivered after the 60 s lease, failed on the last attempt |

New platform evidence: **Laravel Cloud restarts a background process after it exits 124**. The earlier prototype run could not isolate this. The details are in `docs/audits/2026-09-27/platform-findings.md`.

## Deviations from Laravel

There are 19 labeled deviations. Each has its Laravel behavior, our behavior, the reason and the source in [`deviations.md`](deviations.md) and in the catalog.

- **Following symfony-on-cloud:** `agent-receive-ignores-queues`, `agent-socket-env-fallback`, `fifo-delay-rejected`, `fifo-fair-cross-model-rejected`, `fresh-delay-cap`, `missing-receive-count-is-one`, `receive-long-poll-params`, `retry-delay-rounds-up`, `telemetry-queue-from-queue-url`.
- **Project decisions:** `completion-event-immediate`, `credentials-explicit-only`, `deterministic-defects-terminal`, `failed-job-size-policy`, `local-size-rejection`, `managed-config-strict-shape`, `outcome-event-after-ack-rejection`, `registry-only-resolution`, `timeout-window-handler-only`, `visibility-renewal-watchdog`.

## Decisions recorded during the build

- **D13** (reviewed against pinned sources by a Codex agent, review R2):
  - mode-specific terminal-failure order: managed completes then emits `failed_job` then `failed`; `sqs`/`redis` write the failure line and then delete;
  - immediate completion events;
  - `GET /next` retries on HTTP 4xx/5xx, as Laravel does;
  - strict managed configuration;
  - labeled deviations;
  - core dependencies `httpx` and `typing-extensions`;
  - envelope layout;
  - Redis lease expiry;
  - explicit release semantics;
  - direct polling rules.
- **D14** (from cross-lab review R1): 7-day maximum job timeout, the 16 MiB decode ceiling bounds Redis payloads, UTF-8-only envelopes, and bounded decoding work.

Both supersede some wording in `PROJECT_SCOPE.md` (§8, §11, §12). Following the authority rules, they are recorded in `docs/decisions.md` and the scope text was not rewritten.

## Known limitations

- **Native-code timeouts:** a job blocked in native code that holds the GIL overruns its timeout. The overrun was measured at about 0.9 s in `tests/runtime_proof/FINDINGS.md`. Such a call also starves lease renewal. The worker then treats the lease as lost and does not report an outcome.
- **Failure records:** they are best-effort. In managed mode the message is completed before the record is written. There is no Python failed-job store.
- **Delivery:** at-least-once. Duplicates are possible after an ambiguous acknowledgement, so jobs should be idempotent.
- **Managed mode:** built and tested against the pinned contract, the agent emulator and LocalStack only. Live verification waits on the platform.
- **Payload size:** Redis payloads are bounded by the 16 MiB decode ceiling. SQS and managed queues are bounded at 1 MiB. Moving from Redis to SQS reintroduces 1 MiB.
- **Idle shutdown:** an idle worker waits for the current poll on SIGTERM, up to 20 s with a single SQS queue and up to 65 s in agent mode (Laravel parity).
- **Teardown deadline:** the deadline after an explicit release or fail bounds async `yield` teardown only. Sync teardown runs in FastAPI's threadpool and is bounded by the job timeout.
- **Documented gaps:** tuples nested inside untyped (`Any`) arguments do not round-trip, and Decimal exponents are not bounded.
- **Upstream drift:** the drift check is weekly and advisory. The pin stays at `laravel/framework` v13.33.0 and `laravel/symfony-on-cloud` `50c94517`.

## Commands

```sh
# Install
pip install "laravel-cloud-queues[fastapi,redis]"

# Development checks (needs a Redis-compatible server at 127.0.0.1:6379; moto provides SQS)
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy
LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1 uv run pytest \
  --deselect tests/integration/redis/test_redis_integration.py::test_tls_roundtrip  # TLS needs LARAVEL_CLOUD_QUEUES_TEST_REDIS_TLS_URL

# Demo and conformance (writes compatibility-report.json and conformance-artifacts/)
uv run python -m demo.conformance --report compatibility-report.json
uv run laravel-cloud-queues conformance        # same, from a checkout

# Worker
uv run laravel-cloud-queues work myapp.main:app
uv run laravel-cloud-queues inspect myapp.main:app

# Build
uv build
```

## How the build was run

- **Contract first.** The lead wrote the typed contract pack (`src/` interfaces, `docs/contract/*.md`) while three workers ran research, the runtime proof and the local platform in parallel.
- **Lanes.** Implementation lanes each ran in their own worktree and branch. The lead reviewed each handoff and merged the exact handoff SHA into `main` with `--no-ff` after running the full suite.

| Lane | Author |
|---|---|
| L0R upstream evidence and catalog | Claude Fable 5.1 |
| L2 runtime proof | Cursor Grok 4.7 |
| L9 local platform (agent emulator, collector, fixtures) | Codex GPT-6 Astra |
| L3a config and SQS | Codex GPT-6 Astra (rerouted; a Cursor worker produced no output) |
| L3b envelope and codecs | Codex GPT-6 Astra |
| L3c jobs, registry, dispatch | Claude Opus 5.5 |
| L4 agent transport | Codex GPT-6 Astra |
| L5 Redis transport | Codex GPT-6 Astra |
| L6 worker and CLI | Claude Opus 5.5 |
| L7 observability | Cursor Grok 4.7 |
| L8 FastAPI adapter | Cursor Grok 4.7 |
| L10 demo and conformance | Codex GPT-6 Astra |
| L11 probe-app smoke test | Lead |
| L12 README and docs | Claude Fable 5.1 |
| L1 CI and packaging | Claude Sonnet 5 |

- **Cross-lab reviews.** Each review came from a different lab than the author. All findings were fixed with regression tests.

| Review | Scope | Findings |
|---|---|---|
| R1 | trust boundary, Redis, harness | 4 major, 8 minor |
| R2 | contract docs vs upstream | 5 major, 5 minor |
| R3 | worker and core | 1 blocker, 3 major, 2 minor |
| R4 | agent, SQS, observability, FastAPI | 1 major, 5 minor |
