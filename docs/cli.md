# Command Line

## Introduction

The package installs the `laravel-cloud-queues` console script and its shorter alias,
`lcq`. Both run the same commands:

```shell
laravel-cloud-queues --help
laravel-cloud-queues work --help
```

Every command reports errors as a single line on stderr. Pass `--debug` to any command
to see the full traceback. Credentials in URLs and query strings are redacted either way.

## `work`

Runs a queue worker for `TARGET`:

```text
laravel-cloud-queues work [TARGET] [--queue Q[,Q...]] [--max-jobs N] [--max-time S]
                                 [--memory MiB|N%]
                                 [--stop-when-empty] [--stop-when-empty-for S]
                                 [--timeout S] [--sleep S] [--rest S] [--debug]
```

`TARGET` is a `module:attribute` path to a FastAPI app with `LaravelCloudQueues` bound, a
`Registry`, or any [worker target](extending.md#worker-targets). When you omit it, the
worker uses the target in your `pyproject.toml`:

```toml
[tool.laravel-cloud-queues]
target = "myapp.main:app"
```

Without that setting, the worker looks for an `app`, `api` or `registry` attribute in the
`main`, `app`, `api`, `app.main` and `app.api` modules of the current directory, in that
order, and uses the first worker target it finds. `inspect` resolves its target the same
way.

| Option | Default | Description |
|---|---|---|
| `--queue Q[,Q...]` | backend default | The queues to process, in priority order. In managed mode, it defaults to the configuration's `queue`. With the queue agent enabled, it names one queue |
| `--max-jobs N` | off | Stop after N deliveries, with exit code `0` |
| `--max-time S` | off | Stop after S seconds, checked between jobs, with exit code `0` |
| `--memory MiB\|N%` | env or managed 90% | Stop at the RSS limit after reporting a delivery, with exit code `12`; `0` disables it |
| `--stop-when-empty` | off | Stop on the first empty poll. **Not for supervised worker clusters** |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job. **Not for supervised worker clusters** |
| `--timeout S` | `60` | The default job timeout, for messages that do not declare one. `0` disables it |
| `--sleep S` | `3` | The wait after an empty poll (several SQS queues), or the blocking-pop wait (Redis) |
| `--rest S` | `0` | The pause between jobs |
| `--debug` | off | Show tracebacks |

Every duration accepts a finite, non-negative number of seconds. See
[Running the Worker](workers.md) for polling, shutdown and exit code details.

`--memory` takes a MiB value such as `256`, or a percentage such as `90%` of a finite
cgroup v2 memory limit. When absent, `LARAVEL_CLOUD_QUEUES_MEMORY` supplies the value.
Managed mode defaults to 90% of `/sys/fs/cgroup/memory.max` when it contains a finite
limit; there is no default otherwise or in `sqs`/`redis` mode. Laravel Cloud's worker
command is read-only, so tune memory there through the environment variable.

## `inspect`

Shows the resolved mode, the queues, the registered jobs and non-secret settings of
`TARGET`, without connecting to the broker:

```shell
laravel-cloud-queues inspect myapp.main:app
```

```text
Mode: redis
Queues:
  default: default
Jobs (2):
  emails.send (queue=emails, tries=3, backoff=5,30)
  reports.build
Settings:
  redis_url: rediss://caches.laravel.cloud:6379/0
  redis_prefix: laravel-cloud-queues:
```

| Option | Description |
|---|---|
| `--json` | Print machine-readable JSON instead |
| `--debug` | Show tracebacks |

Credentials, passwords and tokens are never printed.

## `retry`

Retries failed jobs from one or more signed Laravel Cloud HTTPS URLs:

```shell
laravel-cloud-queues retry 'https://cloud.laravel.com/...' --target myapp.main:app
```

```text
laravel-cloud-queues retry URL [URL...] [--target TARGET] [--cipher CIPHER] [--debug]
```

`--target` uses the same application resolution as `work`; omit it when the target is
configured in `pyproject.toml`. The command reads the encryption key from
`LARAVEL_CLOUD_QUEUES_KEY`, falling back to `APP_KEY`. A `base64:` key is decoded before use.
The key must match the key used by the Cloud endpoint.

`--cipher` defaults to `AES-256-CBC`. It also accepts `AES-128-CBC`, `AES-256-GCM` and
`AES-128-GCM`, case-insensitively. Both legacy single-job responses and v1 paginated
responses are supported. HTTP requests time out after ten seconds and retry transient
failures up to ten attempts, with exponential backoff starting at half a second. A `403`
is never retried. Redirects and non-HTTPS pagination links are refused.

Each valid job is dispatched immediately to its recorded queue with its original retry
policy and fresh dispatch identity and attempt count. A failed job is reported and skipped,
so other jobs and URLs can still succeed. Exit `1` means at least one fetch, decode or
dispatch failed; invalid URL or key configuration exits `2`. After a successful send,
managed mode emits the normal `queued` event followed by a best-effort `retried_at` update.

See [Failed Jobs and Delivery](failed-jobs.md#retrying-cloud-failed-jobs) for platform
requirements and delivery guarantees.

## `conformance`

Runs the repository's conformance suite, which checks the package's behavior against the
pinned Laravel baseline using local emulators. Every argument is passed through to the
suite:

```shell
laravel-cloud-queues conformance --sqs moto --report compatibility-report.json
```

The suite is development tooling and is not installed with the package. The command only
works from a checkout of the
[repository](https://github.com/DGarbs51/laravel-cloud-queues), after running `uv sync`.
Anywhere else, it explains how to set that up and exits with code `2`.

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success, or a clean worker stop |
| `1` | A fatal error |
| `2` | A configuration or usage error |
| `12` | The worker reached its memory limit after reporting a delivery (`work` only) |
| `124` | A job timed out (`work` only) |
