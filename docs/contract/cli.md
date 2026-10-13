# CLI contract

One entry point: `laravel-cloud-queues` (short alias `lcq`, same `laravel_cloud_queues.cli:main`),
built on `click`.
The command group `laravel_cloud_queues.cli:cli` can be mounted in any click CLI, for example
Flask's: `app.cli.add_command(cli, "queues")` gives `flask queues work ...`. A mounted group
keeps the same error handling and exit codes.
Errors are actionable one-line messages; `--debug` adds tracebacks. Secrets are never printed.

## `work [TARGET]`

`TARGET` = `module:attr`: a `Registry`, a FastAPI app with `LaravelCloudQueues` bound, or any
`WorkerTarget`. The current working directory is put on `sys.path` (like uvicorn). Without
`TARGET`: `[tool.laravel-cloud-queues] target` in `./pyproject.toml`, else the first worker
target among attributes `app`, `api`, `registry` of modules `main`, `app`, `api`,
`app.main`, `app.api`; none found is a `ConfigurationError` (exit 2). `inspect` resolves the
same way.

| Option | Default | Meaning |
|---|---|---|
| `--queue Q[,Q...]` | backend default | Priority list. Managed: defaults to the top-level `queue`. Agent mode: one queue only |
| `--max-jobs N` | none | Stop after N deliveries |
| `--max-time S` | none | Stop after S seconds (checked between jobs) |
| `--memory MiB\|N%` | env or managed 90% | RSS limit; exit 12 after delivery reporting; 0 disables |
| `--stop-when-empty` | off | Stop on the first empty poll (not for supervised worker clusters) |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job |
| `--timeout S` | 60 | Default job timeout when the message omits one (0 disables) |
| `--sleep S` | 3 | Wait after an empty poll (direct/Redis) |
| `--rest S` | 0 | Pause between jobs |
| `--debug` | off | Tracebacks on errors |

Memory precedence: explicit option, then `LARAVEL_CLOUD_QUEUES_MEMORY`, then 90% of a
finite `/sys/fs/cgroup/memory.max` in managed mode. No default in other modes or without
a finite limit (`max`, missing or unreadable). Explicit percentages require that limit;
invalid values are startup configuration errors (exit 2). Percentages must be between
0 and 100; MiB values must be finite and non-negative. Cloud's read-only worker command
uses the environment override or managed default.

Exit codes: see worker.md (0, 1, 2, 12, 124).

## `inspect [TARGET] [--json]`

Prints mode, resolved queues (default, worker assignment, managed inventory), registered jobs
(name, default queue, declared policy) and non-secret configuration. Opens no broker
connection and performs no queue operation. Exit 0, or 2 on configuration error.

## `retry URL [URL...] [--target TARGET] [--cipher CIPHER]`

Resolve `--target` with the same resolver as `work`, including the pyproject target and
conventional modules. Require absolute HTTPS URLs without embedded credentials, for both
initial URLs and pagination links; reject redirects and pagination cycles.

Load `LARAVEL_CLOUD_QUEUES_KEY`, falling back to `APP_KEY`; decode `base64:` keys. Missing,
invalid or incorrectly sized keys are `ConfigurationError` (exit 2). The default cipher is
`AES-256-CBC`; `--cipher` also accepts AES-128-CBC and AES-128/256-GCM. Authenticate Laravel
`encryptString` payloads with HMAC-SHA256 (CBC, constant-time MAC comparison) or a full
16-byte GCM tag before using their contents.

GET with `Cloud-Payload-Version: 1` and `Cloud-Encryption-Cipher: <cipher>`. Each attempt
has a ten-second timeout. Connection/transport errors, HTTP 408/429 and 5xx responses get
at most ten attempts, waiting 500 ms × 2^(n−1) between attempts. Never retry 403. Bound each
response to 64 MiB. No response-version header means one legacy failure record; `1` means
`{data: [...], links: {self, next}}`, following `next` until null. Other versions fail.

For each `{id, connection: "cloud", queue, payload}` record, use registry-only job lookup
and annotation-driven argument decoding, then the existing dispatch pipeline with the
saved policy and recorded queue. The new dispatch gets fresh UUID, tracing and FIFO
deduplication defaults; broker attempts restart at one. No handler runs during retry.
Only after sending and the normal `queued` event, emit managed-mode
`failed_job{id, queue, retried_at}` with UTC `Y-m-d H:i:s.u`. Telemetry remains best-effort.

Report and skip invalid records or failed dispatches, continue remaining URLs after a
page failure, and exit 1 if any failed. Exit 0 on success, including an empty batch.
`--debug` includes redacted tracebacks for per-record and page errors.

## `conformance [ARGS...]`

Runs the repository's conformance suite (`tests/conformance/`, harness emulators) and writes the human
and JSON reports; arguments pass through. When run from an installed package without a
checkout, prints how to run it from a repository checkout and exits 2.
