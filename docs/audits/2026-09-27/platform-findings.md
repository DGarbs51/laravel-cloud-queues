# Laravel Cloud platform findings — 2026-09-27

Evidence gathered from a live deployment of `probe-app/` and from the Laravel Cloud CLI and API. These results test assumptions the source audits could not prove from the pinned upstream code.

## Deployment

- Organization: Laravel GTM
- Application: `laravel-cloud-queues` (`app-a2d97eb6-24b3-4e46-87ef-d3edd9d884f8`), region `us-east-2`
- Environment: `production` (`env-a2d97eb8-3c7f-4080-994b-f65c751ed899`), root directory `probe-app`
- Commit deployed: `dbd410a`
- Build and deploy commands: empty. Cloud installed dependencies with uv from `uv.lock` and pre-filled the FastAPI start command.

## `/verify` output from the web container

- Runtime: Python 3.14.7, Linux aarch64, glibc 2.36; working directory `/var/www/workspace/probe-app`.
- `PORT=3000`.
- Environment variables present: `LARAVEL_CLOUD`, `LARAVEL_CLOUD_APP_NAME`, `LARAVEL_CLOUD_BUILD_NUMBER`, `LARAVEL_CLOUD_COMMIT`, `LARAVEL_CLOUD_COMMIT_SHA`, `LARAVEL_CLOUD_DEPLOY`, `LARAVEL_CLOUD_DEPLOY_UUID`, `LARAVEL_CLOUD_ENV_BRANCH`, `LARAVEL_CLOUD_ENV_NAME`, `LARAVEL_CLOUD_ENV_UUID`, `LARAVEL_CLOUD_REGION`, `AWS_EC2_METADATA_DISABLED`.
- `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`: **not present**.
- `LARAVEL_CLOUD_LOG_SOCKET`: **not set**, but `/tmp/cloud-init.sock` **exists and accepts connections**. The default path assumed by the scope is correct for Python containers.
- Agent socket `/tmp/cloud-agent.sock`: **does not exist**.
- ECS container credential variables (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`): **none set**. With `AWS_EC2_METADATA_DISABLED` also set, the container has no AWS identity.

## Managed queue creation attempt

The Cloud CLI (`managed-queue:create`) has no runtime or framework check. It posts an instance of type `managed_queue` whose worker is configured for Laravel: `{connection: "cloud", queue, backoff: 0, tries: 1, timeout: 60}`. It also requires a `composer.json` containing `aws/aws-sdk-php` in the working directory.

Results against the `production` environment:

| Size | API response |
|---|---|
| `mq.flex.256mb` | `422` — `size: The size value is invalid.` |
| `mq.dedicated.flex.256mb` (Private Cloud size) | `422` — `type: Managed queues are not available for FastAPI applications.` |

No queue was created. The environment still has only its `App` instance.

## Conclusions

1. **The Cloud API explicitly blocks managed queues for FastAPI applications.** The refusal names the detected framework, which suggests an allowlist of frameworks (Laravel, Symfony) rather than a check on the Python runtime as a whole.
2. **A Python app cannot dispatch to or consume managed queues on Cloud today.** It has no queue configuration, no agent socket and no AWS credentials.
3. **The observability socket is already available** to Python containers at the default path, so lifecycle events may not need platform changes.
4. The scope's premise that live Cloud verification is merely "not a release gate yet" understates the situation: the platform support does not exist yet.

## Platform changes needed to unlock Python

1. Allow FastAPI (and Python generally) in the managed-queue validation on instance creation.
2. Inject `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` and AWS container credentials into Python web and worker containers.
3. Let Python queues declare their own worker command (for example `laravel-cloud-queues work <target>`) instead of Laravel `queue:work` options, and define how `tries`, `backoff` and `timeout` reach it.
4. Run the cloud-agent sidecar in Python worker containers, serving `/tmp/cloud-agent.sock`.

## Facts confirmed by Laravel Cloud documentation

From <https://laravel.com/cloud/docs/queues> and <https://laravel.com/cloud/docs/runtimes>:

- Managed queues are listed for Laravel and Symfony only.
- Job payloads may be up to 1 MiB.
- Cloud extends a running job's visibility timeout in three-minute increments.
- Failed jobs can be inspected, retried and deleted from the dashboard or API.
- Flex workers: 90-second job runtime limit and 90 seconds to finish on shutdown. Pro workers: no fixed runtime limit and one hour to finish.
- Queue names may be at most 39 characters, including `.fifo`.
- A worker that exceeds its memory restarts, and the job is redelivered.
- Python 3.10–3.14 are supported; the version comes from `.python-version` or `requires-python`.

## Attached resources in a Python container (second probe run)

After attaching a MySQL database, a Laravel Valkey cache and object storage to the environment, `/verify` (now returning every variable) showed the following. Values are omitted here; only names and shapes are recorded.

| Resource | Variables injected | Shape |
|---|---|---|
| Database (MySQL) | `DATABASE_URL` | `mysql://<user>:<password>@<host>.db.laravel.cloud:3306/<database>` |
| Valkey | `REDIS_URL` | `rediss://application:<password>@<host>.caches.laravel.cloud:6379/0` (TLS) |
| Object storage (Cloudflare R2) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_ENDPOINT_URL`, `AWS_REGION=auto`, `AWS_DEFAULT_REGION=auto`, `AWS_BUCKET`, `AWS_USE_PATH_STYLE_ENDPOINT` | R2 credentials and endpoint |

Python applications receive single URL variables, not Laravel-style split variables (`DB_HOST`, `REDIS_HOST` and so on).

Other platform variables observed: `NGINX_HTTP_TIMEOUT=20` (web requests are cut off after 20 seconds), `NGINX_UPSTREAM_PORT=3000`, `WEB_CONCURRENCY=1`, `PYTHONUNBUFFERED=1`, `UV_COMPILE_BYTECODE=1`, IPv6 cluster networking (`KUBERNETES_*`, `SVC_*`).

### Implications

1. **A Redis/Valkey queue backend is viable on worker clusters today.** `REDIS_URL` is sufficient for a TLS client, and Valkey supports the Lua scripting a reliable queue needs.
2. **Object storage occupies the standard `AWS_*` names.** boto3 honors `AWS_ENDPOINT_URL` for every service, so an SQS client built from the default chain in the same container would send SQS requests to R2 with R2 credentials and region `auto`. Self-managed SQS must use package-specific configuration passed explicitly to the client, not the standard `AWS_*` variables or Laravel's `sqs` connection variable names.
3. **Managed mode must select credentials explicitly.** When managed queues reach Python, `credentials: "ecs"` must use the ECS container credential provider, because the default chain would pick up the R2 keys first.
4. **Synchronous dispatch and eager execution inside web requests must finish well under 20 seconds.**
