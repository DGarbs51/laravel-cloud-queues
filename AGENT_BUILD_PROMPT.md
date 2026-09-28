# Multi-Agent Build Prompt — `laravel-cloud-queues`

You are an orchestrated engineering team (Claude Code, Codex, Grok 4.7). Build a production-quality Python package, **`laravel-cloud-queues`**, that lets Python applications use Laravel Cloud queues: Laravel Cloud Managed Queues, plus self-managed SQS and Redis/Valkey for Laravel Cloud worker clusters.

You are not brainstorming. Research the pinned contracts, implement the package and its FastAPI adapter, build the local compatibility infrastructure and `demo/`, run the tests, review each other's work, and leave the repository in a release-quality `0.x` state.

## Authority

1. **`PROJECT_SCOPE.md` is authoritative.** This prompt defines execution, ownership and gates. It does not restate the scope; when it cites a section (§), read that section. Nothing here is a substitute for the scope.
2. **`docs/decisions.md` (D1–D6)** records resolved decisions. The scope incorporates them; if the two disagree, the decision record wins and the scope must be corrected.
3. **`docs/references.md`** lists what to verify against: pinned public sources, Laravel Cloud docs, and private Laravel repositories (cite by name only).
4. **`docs/audits/2026-09-27/`** explains why the scope says what it says, including `platform-findings.md` (live Laravel Cloud evidence).

Do not rewrite `PROJECT_SCOPE.md` to match what you built. If research proves a scope statement wrong, propose an amendment with source evidence, record it in `docs/decisions.md`, and get it reviewed by another agent. The user makes any call the evidence cannot settle.

## Mission (summary; see §1–§3)

- Python 3.10+, FastAPI as the first framework adapter, framework-independent core, Django/Flask later.
- Compatibility is with **Laravel Cloud's queue infrastructure and semantics**, not Laravel's PHP API. No PHP/Python payload interoperability.
- Producer and worker are the same Python application in different processes, on possibly different hardware and deploy revisions.
- **Platform status (§1):** Laravel Cloud runs Python, but managed queues are not yet enabled for Python. Build `managed` mode against the pinned contract, LocalStack and the agent emulator. Worker-cluster modes (`sqs`, `redis`) work on Laravel Cloud today and are first-class.

## Pinned baseline (§2)

- `laravel/framework` **v13.33.0** (`91188a17ceaa3dbace6e8a5f7abd0d042e466359`) — canonical.
- `laravel/symfony-on-cloud` **50c945170b6cb5690370d15fd725c6f82495e9ba** — cross-framework precedent.

Follow §2's evidence hierarchy: executed code > tests > comments > docs. Record the exact upstream file behind every conformance expectation. Implement the upstream drift check (§2) as an advisory CI job that never moves the pin.

## Resolved conflicts

Where Laravel and Symfony disagree, or the project deliberately deviates, the scope already decides. Encode each row as a labeled deviation in the conformance catalog.

| Topic | Laravel | Symfony | Project (scope §) |
|---|---|---|---|
| Agent receive selection | Agent only when requested queue matches worker queue; otherwise direct SQS | Agent whenever enabled; ignores requested queues | Symfony rule; conflicting CLI queue is a startup error (§11) |
| `GET /next` retries | 3 attempts (retry after 0 ms, 500 ms) | 1 attempt | Laravel (§11) |
| 200 without `messageId` | Empty poll | Empty poll | Empty poll (§11) |
| `/result` 4xx | Non-fatal `RequestException` | Non-fatal `RuntimeException` | Fatal, typed `AgentProtocolError` — deviation (§11) |
| Agent-loss exit status | 0 | Consumer exits | 1 — deviation (§11, §23) |
| Timeout | `SIGALRM`, failure checks, `released`/`failed` event, exit 124, no release | Not implemented | Laravel parity (§14, D2) |
| Retry delay rounding | Truncates | Rounds sub-second up; clamps 43,200 | Symfony (§12) |
| Fresh delay > 900 s | Forwards to SQS | Rejects | Reject (§10) |
| FIFO + delay | Omits `DelaySeconds` | Rejects | Reject (§10) |
| FIFO/fair cross-model options | Not rejected | Rejects | Reject (§10) |
| Default FIFO dedup | New ordered UUID; empty → omit | UUIDv7; empty kept | New unique ID; explicit empty → omit (§10) |
| Default tries | 1 | Messenger: 3 retries | 1 (§12) |
| Retry policy location | In payload | Worker config | In message (§8, D4) |
| `failed_job` fields | Full set incl. `exception_preview`, `job_name` | Omits both; trims | Laravel fields + D1 size policy (§15) |
| `failed_job` / `failed` order | `failed_job` first | `failed` first | Laravel (§15) |
| `duration_ms` | Truncated | Rounded, ≥ 0 | Truncated, ≥ 0 (§15) |
| Malformed managed config | Throws | Treated as unconfigured | Throws (§6) |
| `credentials: "ecs"` | Explicit ECS provider; unknown throws | ECS or SDK default chain | Explicit; unknown is an error; never default chain (§6) |
| Queue URL | Full-URL pass-through; suffix once | Literal concatenation | Laravel (§6) |
| Agent socket fallback | Config, else default | Config, `LARAVEL_CLOUD_AGENT_SOCKET`, default | Symfony (§6) |

## Contract pack — freeze before parallel work

Step 2 below produces these files. After review, they are the shared interfaces every agent builds against; changes need the owner plus one reviewer.

1. Configuration models and backend selection (§6, D6a).
2. Envelope v1 schema, including `uuid`, `displayName` and the retry policy (§8).
3. Delivery state machine and outcome ownership (§12).
4. Error hierarchy and classification table (§24).
5. Transport interface covering the agent, direct SQS and Redis (§11).
6. Lifecycle and `failed_job` event fixtures (§15).
7. Agent protocol fixtures: requests, responses, faults (§11).
8. Public API signatures: decorator, direct call, `dispatch`, `dispatch_async`, `.options(...)`, `JobContext`, registry (§7, §9, §10, §17, §18).
9. CLI commands, options and exit codes (§13, §23).
10. Conformance catalog with feature IDs, one per §21 and §30 item (§22).

## Ownership

One writer per artifact. Reviewers propose changes; they do not rewrite an owner's files.

| Area | Owner | Required reviewer |
|---|---|---|
| Contract matrix, conformance catalog of record, public API design | Claude | Grok |
| Core: config, envelope/codecs, registry, dispatch, policies | Codex | Claude |
| Transports: agent, direct SQS, Redis/Valkey | Codex | Grok |
| Worker runtime, timeouts, shutdown | Codex | Grok |
| FastAPI adapter | Codex | Claude |
| Observability and tracing | Codex | Claude |
| Agent emulator, log collector, LocalStack and Valkey harnesses | Grok | Codex |
| Conformance scenarios, `demo/`, report generator | Grok | Claude |
| CLI | Codex | Grok |
| Packaging, CI, drift check | Codex | Grok |
| `README.md` | Claude | Codex |

**Integration owner:** Codex merges to the integration branch and runs the full gate.

**Disagreements:**
1. Settle them with source evidence (Laravel canonical).
2. If evidence cannot settle it, record the question in `docs/decisions.md` and ask the user.
3. Never resolve a disagreement by editing the other agent's tests.

**Mechanics:**
- Each agent works on its own branch or git worktree.
- Commit small, reviewable changes.
- The contract pack merges first.
- Do not push to `main` unless instructed: every push to `main` redeploys `probe-app/` on Laravel Cloud.

## Execution sequence

1. **Research.** Read the pinned sources and `docs/audits/2026-09-27/`. Confirm the resolved-conflicts table against source. Report any row that is wrong, with evidence, before building on it.
2. **Contract pack.** Produce and review the ten artifacts above.
3. **Runtime proof.** Before building the worker:
   - Prove the D2 timeout mechanism in real subprocesses: exit 124, correct event, redelivery, terminal failure on the last attempt, for async, sync-loop and native-blocking handlers.
   - Prove visibility/reservation renewal runs while a sync handler blocks the loop.
   - Record the native-code limitation as observed.
4. **Scaffold.** `pyproject.toml` (hatchling; extras `fastapi`, `redis`, `otel`), `src/` layout, `py.typed`, CI matrix (§5), lint and mypy config, packaging exclusions for `demo/`, `probe-app/` and `docs/`.
5. **Core and transports.** Config and mode selection, envelope and codecs with trust-boundary validation, registry, dispatch pipeline, policies, agent, direct SQS, Redis/Valkey, observability, tracing, eager mode.
6. **Worker.** Delivery state machine, timeouts, renewal watchdog, shutdown races, CLI.
7. **FastAPI adapter.** Integration object, decorator, lifespan, per-job `Depends` scope with teardown before acknowledgement, `JobContext`.
8. **Local platform.** Stateful agent emulator with fault injection, log collector, LocalStack, Valkey/Redis (Laravel Herd's Valkey on `127.0.0.1:6379` locally; containers in CI).
9. **Demo and conformance.** Every §21 probe, the catalog, and the human-readable and JSON reports with evidence tiers and deviation labels.
10. **Docs.** README per §25, architecture notes, deviations, roadmap.
11. **Independent review.** Each agent reviews areas it did not author, tracing high-risk expectations back to source. Then one clean full-gate run on the integration branch, and building the wheel and sdist and inspecting their contents.
12. **Final report** (below).

## Release gate

Do not declare the build complete until **all** of the following hold:

- every acceptance criterion in **§30** (1–35);
- every CI release gate in **§5**;
- every required feature in the conformance catalog `pass`es at its declared evidence tier;
- only features on the reviewed exception list (currently the live Laravel Cloud managed-queue checks) are `skipped`/`partial`/`unsupported`;
- the report command exits non-zero on any missing record or unapproved non-pass.

An explained failure is still a failure.

## Security

Follow §8's trust boundary and §26a. This repository is public: never copy code, configuration or internal details from private Laravel repositories; cite them by name only.

## Final report

- Completed features, mapped to §30.
- Exact conformance results, with every non-`pass` item and its approval.
- Deviations from Laravel, with sources.
- Known limitations (native-code timeout overrun, best-effort failure records, at-least-once delivery).
- Commands to install, test, run the demo and conformance, and build the package.
- Anything that needs the user's decision.

Do not claim full compatibility where the evidence says otherwise.
