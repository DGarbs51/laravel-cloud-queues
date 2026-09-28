# CLI contract

One entry point: `laravel-cloud-queues` (short alias `lcq`, same `laravel_cloud_queues.cli:main`),
built on `click`.
The command group `laravel_cloud_queues.cli:cli` can be mounted in any click CLI, for example
Flask's: `app.cli.add_command(cli, "queues")` gives `flask queues work ...`. A mounted group
keeps the same error handling and exit codes.
Errors are actionable one-line messages; `--debug` adds tracebacks. Secrets are never printed.

## `work TARGET`

`TARGET` = `module:attr`: a `Registry`, a FastAPI app with `LaravelCloudQueues` bound, or any
`WorkerTarget`. The current working directory is put on `sys.path` (like uvicorn).

| Option | Default | Meaning |
|---|---|---|
| `--queue Q[,Q...]` | backend default | Priority list (direct/Redis). Agent mode: must match the assignment or be omitted |
| `--max-jobs N` | none | Stop after N deliveries |
| `--max-time S` | none | Stop after S seconds (checked between jobs) |
| `--stop-when-empty` | off | Stop on the first empty poll (not for supervised worker clusters) |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job |
| `--timeout S` | 60 | Default job timeout when the message omits one (0 disables) |
| `--sleep S` | 3 | Wait after an empty poll (direct/Redis) |
| `--rest S` | 0 | Pause between jobs |
| `--debug` | off | Tracebacks on errors |

Exit codes: see worker.md (0, 1, 2, 124).

## `inspect TARGET [--json]`

Prints mode, resolved queues (default, worker assignment, managed inventory), registered jobs
(name, default queue, declared policy) and non-secret configuration. Opens no broker
connection and performs no queue operation. Exit 0, or 2 on configuration error.

## `conformance [ARGS...]`

Runs the repository's conformance suite (`demo/`, harness emulators) and writes the human
and JSON reports; arguments pass through. When run from an installed package without a
checkout, prints how to run it from a repository checkout and exits 2.
