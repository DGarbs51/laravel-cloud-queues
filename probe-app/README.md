# probe-app — Laravel Cloud probe

Repository-only tooling. Not part of the published `laravel-cloud-queues` package.

A minimal FastAPI app (Python 3.14, uv) whose `GET /verify` route reports what a Laravel Cloud container exposes, plus a live smoke test of the real `laravel-cloud-queues` package on Laravel Cloud worker clusters (D6c). It installs the package from this repository (`[tool.uv.sources]` path `..`, built as a wheel because Cloud deploys only `probe-app/`).

## Deploy to Laravel Cloud

1. Create an application from this repository and select `probe-app` as the root directory (monorepo picker).
2. Leave the build and deploy commands empty. Cloud detects FastAPI, installs dependencies with uv from `uv.lock`, and reads Python 3.14 from `.python-version`.
3. Keep the start command Cloud pre-fills. It runs behind Cloud's nginx proxy on `$PORT` (3000):

   ```sh
   uvicorn 'main:app' --host '' --port $PORT
   ```

4. Add the environment variable `PROBE_TOKEN` with a long random value. `/verify` returns 503 until it is set.

Managed queues cannot be added: the Cloud API currently rejects them for FastAPI applications. See `docs/audits/2026-09-27/platform-findings.md`.

## Use

```sh
curl "https://<your-app>.laravel.cloud/verify?token=$PROBE_TOKEN"
```

The response contains:

- `runtime`: Python version, platform, working directory.
- `env`: **every** environment variable with its full value, secrets included. Test-only; delete this app after testing.
- `managed_queues_config`: the structure of `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`, with long digit runs (such as AWS account IDs) masked.
- `sockets`: whether the agent socket and log socket exist and accept a connection. The probe connects only and never writes, so no events reach the dashboard. Also lists every socket in `/tmp`.
- `aws_container_credentials`: whether the ECS container credential variables are present.

## Run locally

```sh
cd probe-app
uv sync
PROBE_TOKEN=dev uv run uvicorn main:app --port 8000
curl "localhost:8000/verify?token=dev"
```

## Queue smoke test (D6c)

Environment: a Laravel Valkey cache attached (injects `REDIS_URL`) and `LARAVEL_CLOUD_QUEUES_BACKEND=redis`.
Background processes on the App and worker clusters run:

```sh
python -m laravel_cloud_queues.cli work main:app
```

Dispatch one job per case and read what the workers recorded (all routes require `PROBE_TOKEN`):

```sh
curl -X POST "https://<your-app>/queue/e2e?token=$PROBE_TOKEN"        # returns {case: job uuid}
curl "https://<your-app>/queue/jobs/<uuid>?token=$PROBE_TOKEN"          # events per attempt
curl -X POST "https://<your-app>/queue/burst?n=50&token=$PROBE_TOKEN"
```

Cases: `ok` (sync), `async_ok`, `delayed` (5 s), `flaky_retry` (tries 2, fails once), `terminal_fail` (tries 2, D6b failure record in the worker log), `timeout_then_terminal` (timeout 3 s, exits 124 twice, failed on the last attempt). Worker logs (`cpx cloud environment:logs`) show one JSON line per outcome.
