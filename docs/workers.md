# Running the Worker

## Introduction

The worker is a separate, long-running process that pulls jobs off your queues and runs
them. Start it with the `work` command and the same `module:attribute` target you would
give uvicorn:

```shell
laravel-cloud-queues work myapp.main:app
```

`lcq` is a shorter alias for the same command:

```shell
lcq work myapp.main:app
```

The worker imports your target, finds its registry, enters your application's lifespan
once, and then processes **one job at a time** until it is told to stop. To process more
jobs in parallel, run more worker processes, not in-process concurrency.

## Worker Targets

The target may be any of the following:

- A FastAPI app with `LaravelCloudQueues` bound, such as `myapp.main:app`
- A plain `Registry`, such as `myapp.jobs:registry`
- Any object with a `registry` property and a `lifespan()` async context manager (see
  [Extending](extending.md#worker-targets))

The current directory is placed on `sys.path`, as it is with uvicorn, so run the worker
from your project's root directory. Nested attributes such as `myapp.main:container.app`
are supported.

## Queue Priorities

By default, the worker processes the backend's default queue. To process other queues,
pass `--queue` with a comma-separated list. The list is a priority order: the worker
checks the first queue, then the second, and so on:

```shell
laravel-cloud-queues work myapp.main:app --queue high,default,low
```

Queue priorities apply to the `sqs` and `redis` backends. In managed mode with the queue
agent enabled, Laravel Cloud assigns the queue, and a `--queue` that differs from the
assignment is a configuration error.

## Worker Options

| Option | Default | Description |
|---|---|---|
| `--queue Q[,Q...]` | backend default | The queues to process, in priority order |
| `--max-jobs N` | off | Stop after processing N jobs |
| `--max-time S` | off | Stop after S seconds, checked between jobs |
| `--stop-when-empty` | off | Stop as soon as the queue is empty |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job |
| `--timeout S` | `60` | The default job timeout, for jobs that do not declare one. `0` disables it |
| `--sleep S` | `3` | How long to wait after an empty poll (see [Polling](#polling)) |
| `--rest S` | `0` | How long to pause between jobs |
| `--debug` | off | Show full tracebacks for errors |

Every exit, including those triggered by `--max-jobs` and `--max-time`, exits with code
`0`, so they are a safe way to recycle a worker periodically.

:::{warning}
Do not use `--stop-when-empty` or `--stop-when-empty-for` on Laravel Cloud worker
clusters. Laravel Cloud restarts a worker whenever it exits, so a worker that stops on an
empty queue restarts in a tight loop. These options are meant for scripts and CI.
:::

## Polling

The worker never busy-polls:

- **One SQS queue.** The worker long-polls for up to 20 seconds, one message at a time,
  with no extra sleep after an empty poll.
- **Several SQS queues.** Each queue is short-polled in priority order, and the worker
  sleeps for `--sleep` seconds if all of them were empty.
- **Redis.** The worker uses a bounded blocking pop of `--sleep` seconds.
- **The Laravel Cloud agent.** The agent long-polls for the worker.

When receiving fails because of a transient broker error, the worker logs a warning,
sleeps for one second, and tries again.

## Graceful Shutdown

When the worker receives `SIGTERM` or `SIGINT`, it stops fetching new jobs, lets the
current job finish, reports its outcome, runs its dependency teardown, exits your
application's lifespan, and exits with code `0`. Sending the signal again never skips
reporting.

If the signal arrives while the worker is waiting for a job, the worker waits for that
poll to return: up to 20 seconds for an SQS long poll, or up to 65 seconds for the Laravel
Cloud agent. If the poll hands over a message, the worker runs it rather than abandon a
message the broker now considers in flight.

Laravel Cloud sends `SIGTERM` when it deploys. If the platform kills the process before
the job finishes, SQS visibility or the Redis reservation expires and the job is
delivered again.

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | A clean stop: a signal, `--max-jobs`, `--max-time` or `--stop-when-empty*`. Also used when the Laravel Cloud agent becomes unavailable, matching Laravel |
| `1` | A fatal transport error: a lost visibility lease, a lost broker connection after retries, or an ambiguous acknowledgement |
| `2` | A configuration error at startup |
| `124` | A job timed out |

On Laravel Cloud, every exit is followed by a restart, so these codes are mostly
diagnostic. A configuration error restarts in a loop, and is logged clearly on every
start.

## Logging

The worker logs through Python's `logging` module, under the `laravel_cloud_queues`
logger. When the root logger has no handlers yet, the `work` command calls
[`laravel-cloud-logging`](https://pypi.org/project/laravel-cloud-logging/)'s `configure()`,
so every record is one Laravel-style JSON line with its level, context and exception chain,
like a Laravel app's logs. On Laravel Cloud the lines go to the log socket; elsewhere they go
to stdout. Set `LOG_LEVEL` to change the level (default `INFO`). If you configure logging
yourself before the worker starts, your configuration is used instead.

Errors are reported as one actionable line. Pass `--debug` to see the full traceback.
Credentials in URLs and query strings are redacted.

## Long-Running Jobs

On the `sqs` and `redis` backends, the worker renews the job's lease itself while it
runs. It receives each message with a 60-second lease, and a watchdog thread extends the
lease every 20 seconds, even while a sync handler blocks the event loop. If a renewal
fails, the worker no longer owns the message: it never reports success for that job, and
exits with code `1`.

In managed mode, the Laravel Cloud agent extends the job's visibility instead.

## Running the Worker From Python

You may also start a worker from your own code. `Worker.run()` must be called on the main
thread, because the timeout relies on signals, and it returns the exit code:

```python
import sys

from laravel_cloud_queues.worker import Worker, WorkerOptions, resolve_target

worker = Worker(resolve_target("myapp.main:app"), WorkerOptions(queues=("emails",), max_jobs=100))
sys.exit(worker.run())
```

`WorkerOptions` accepts the same settings as the command line: `queues`, `max_jobs`,
`max_time`, `stop_when_empty`, `stop_when_empty_for`, `timeout`, `sleep` and `rest`.
