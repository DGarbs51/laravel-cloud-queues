# Human verification checklist

These steps need a person: reviewing decisions, checking live infrastructure, handling credentials, and choices the build could not make on its own. They go with [`build-report-2026-09-28.md`](build-report-2026-09-28.md). Work top to bottom; the first two sections are the most time-sensitive.

## 1. Security and credentials (do first)

- [ ] **Check that no private Laravel material was copied into this public repository.** Private repositories should be cited by name only. From the repository root:
  ```sh
  git grep -nE "laravel/(cloud|cloud-app-operator|cloud-init|cloud-logging-collector|activejob-laravel-cloud|cloud-docs|cloud-internal-docs)" -- . ':!docs/references.md'
  ```
  Review each hit: it should be a name-only citation.

## 2. Laravel Cloud state and live smoke test

The build's probe application (`probe-app/`, with its environment, Valkey cache, database cluster and buckets) was deleted on 2026-09-28. Live checks now run in the canary applications [`fastapi-cloud-queues`](https://github.com/DGarbs51/fastapi-cloud-queues) and [`python-cloud-queues`](https://github.com/DGarbs51/python-cloud-queues): open a deployment's dashboard and use **Run check**.

## 3. Reproduce the local results

On a clean checkout with Laravel Herd's Valkey running at `127.0.0.1:6379`:

- [ ] Install and run the static checks:
  ```sh
  uv sync
  uv run ruff check . && uv run ruff format --check . && uv run ty check
  ```
- [ ] Run the full suite. Expect about 1,205 passed. The TLS test needs a TLS server, so it is deselected here; CI covers it.
  ```sh
  LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1 uv run pytest -q \
    --deselect tests/integration/redis/test_redis_integration.py::test_tls_roundtrip
  ```
- [ ] Run the conformance suite. Expect 80 PASS and 2 SKIPPED (`cloud.live_managed`, `cloud.dashboard_retry_live`) with exit 0 when a TLS Valkey is configured. Without one, `redis.tls` fails as a missing service; set `LARAVEL_CLOUD_QUEUES_TEST_REDIS_TLS_URL` to fix that.
  ```sh
  uv run python -m demo.conformance --report compatibility-report.json
  ```
  Open `compatibility-report.json` and check that its evidence convinces you. For example, `sqs.retry_visibility_release` should show the same message ID with attempts 1 and 2.
- [ ] Build the artifacts and inspect them by hand:
  ```sh
  uv build
  unzip -l dist/*.whl
  tar tzf dist/*.tar.gz
  ```
  Only `laravel_cloud_queues/` and metadata should be present.

## 4. GitHub

- [ ] Check that the latest run on `main` is green: `gh run list --limit 3`. The `conformance` job uploads `compatibility-report.json` as an artifact.
- [ ] Trigger the advisory drift check once by hand and read its summary: `gh workflow run drift.yml`, then `gh run list --workflow drift.yml`.
- [ ] Decide whether to add branch protection on `main` requiring the `ci` job. The build pushed straight to `main` by design (D10).
- [ ] LocalStack is pinned to `4.14.0`, the last image that starts without an auth token. Decide whether to keep that pin or move to a token-based LocalStack or another SQS emulator later. The reasoning is in a comment in `.github/workflows/ci.yml`.

## 5. Decisions that are yours to confirm

- [ ] **D13 and D14** in `docs/decisions.md`. They supersede some wording in `PROJECT_SCOPE.md`:
  - terminal-failure order (managed: complete, then `failed_job`, then `failed`; `sqs`/`redis`: failure line, then delete);
  - Redis reservation expiry, now plus a 60 s lease renewed by the watchdog;
  - the Redis payload bound (16 MiB decode ceiling);
  - the 7-day maximum job timeout;
  - `GET /next` retrying on 4xx/5xx.

  Per the authority rules the scope was not rewritten. Decide whether to amend `PROJECT_SCOPE.md` to match.
- [ ] **The 19 deviations from Laravel** in `docs/deviations.md`. Each is labeled in the catalog and the report. Confirm you accept them, especially:
  - `deterministic-defects-terminal`: bad payloads fail on the first delivery;
  - `credentials-explicit-only` and `managed-config-strict-shape`: stricter than Laravel;
  - `completion-event-immediate`: `duration_ms` excludes idle time.
- [ ] **FastAPI teardown semantics.** A `yield` dependency now receives the job's exception, like a FastAPI request. Cleanup written after `yield` without `try/finally` does not run when the job fails. This is FastAPI parity and is documented in the README.
- [ ] **Agent shutdown.** An idle managed-mode worker waits for the in-flight `GET /next` (up to 65 s) on SIGTERM, so a message handed over is never dropped (Laravel parity). This fits within Flex's 90 s shutdown window. Confirm that trade-off.

## 6. Before publishing to PyPI (not done)

The package has **not** been published. Before publishing:

- [ ] Confirm the distribution name (`laravel-cloud-queues`), the import name (`laravel_cloud_queues`), the public class names (`LaravelCloudQueues`, `Registry`, `JobContext`) and the version (`0.1.0`).
- [ ] Confirm the license holder line in `LICENSE`. It currently reads "laravel-cloud-queues contributors"; there is no personal branding, per §1.
- [ ] Decide on project URLs and authors metadata in `pyproject.toml`. None are set.
- [ ] Check that the README renders on PyPI. It links to repository files (`docs/…`) with relative links, which do not resolve on PyPI. Switch them to absolute GitHub URLs if you publish.

## 7. When Laravel Cloud enables managed queues for Python

Managed mode cannot be verified live yet. When the platform supports it:

- [ ] Confirm the platform changes listed in `docs/audits/2026-09-27/platform-findings.md`:
  - FastAPI is allowed in managed-queue validation;
  - `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` and ECS credentials are injected into Python containers;
  - a Python worker command is allowed;
  - the agent sidecar serves `/tmp/cloud-agent.sock`.
- [ ] Run a managed-mode probe:
  - unset `LARAVEL_CLOUD_QUEUES_BACKEND`;
  - check `laravel-cloud-queues inspect main:app` (mode `managed`);
  - dispatch jobs and confirm they appear in Laravel Cloud's Queues dashboard, including lifecycle events and `failed_job` records;
  - retry a failed job from the dashboard and confirm it runs as a fresh attempt 1.
- [ ] Then turn `cloud.live_managed` and `cloud.dashboard_retry_live` into live probes and remove them from the catalog's exception list.
