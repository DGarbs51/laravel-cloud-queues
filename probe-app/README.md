# probe-app — Laravel Cloud probe

Repository-only tooling. Not part of the published `laravel-cloud-queues` package.

A minimal FastAPI app (Python 3.14, uv) whose `GET /verify` route reports what a Laravel Cloud container exposes. It lets us check the platform assumptions in `docs/audits/2026-09-27/` against a real deployment.

## Deploy to Laravel Cloud

1. Create an application from this repository and select `probe-app` as the root directory (monorepo picker).
2. Set the start command:

   ```sh
   uvicorn main:app --host 0.0.0.0 --port $PORT
   ```

3. Add the environment variable `PROBE_TOKEN` with a long random value. `/verify` returns 503 until it is set.
4. Optional: add a managed queue or a worker to the environment, redeploy, and compare the output.

## Use

```sh
curl "https://<your-app>.laravel.cloud/verify?token=$PROBE_TOKEN"
```

The response contains:

- `runtime`: Python version, platform, working directory.
- `env`: names of relevant variables (`LARAVEL_CLOUD*`, `CLOUD_*`, `AWS_*`, `QUEUE_*`, `MESSENGER_*`). Only `set` and `length` are returned for each value; `PORT`, region and `QUEUE_CONNECTION` are shown in full.
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
