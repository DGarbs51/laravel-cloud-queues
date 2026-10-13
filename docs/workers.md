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

The worker talks to the broker from its event loop: natively on `redis` and the Laravel
Cloud agent, and through a worker thread on SQS (see
[Native Async Paths](dispatching.md#native-async-paths)).

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

In managed mode, the worker processes the configuration's `queue` unless you pass
`--queue`. With the queue agent enabled, a worker processes one queue, so run one worker
per queue; a `--queue` that names several is a configuration error.

## Worker Options

| Option | Default | Description |
|---|---|---|
| `--queue Q[,Q...]` | backend default | The queues to process, in priority order |
| `--max-jobs N` | off | Stop after processing N jobs |
| `--max-time S` | off | Stop after S seconds, checked between jobs |
| `--memory MiB\|N%` | env or managed 90% | Recycle at the RSS limit after reporting a delivery; `0` disables it |
| `--stop-when-empty` | off | Stop as soon as the queue is empty |
| `--stop-when-empty-for S` | off | Stop after S seconds without a job |
| `--timeout S` | `60` | The default job timeout, for jobs that do not declare one. `0` disables it |
| `--sleep S` | `3` | How long to wait after an empty poll (see [Polling](#polling)) |
| `--rest S` | `0` | How long to pause between jobs |
| `--debug` | off | Show full tracebacks for errors |

Stops triggered by `--max-jobs` and `--max-time` exit with code `0`, so they are a safe
way to recycle a worker periodically. Memory recycling exits with code `12`.

:::{warning}
Do not use `--stop-when-empty` or `--stop-when-empty-for` on Laravel Cloud worker
clusters. Laravel Cloud restarts a worker whenever it exits, so a worker that stops on an
empty queue restarts in a tight loop. These options are meant for scripts and CI.
:::

## Memory Limits

The worker checks resident memory (RSS) after each delivery's outcome and completion
records have been reported. At or above its limit, it logs the measured RSS and limit,
stops fetching, exits the application's lifespan, and exits with code `12`. It never
interrupts a job in flight. Laravel Cloud treats exit `12` as a restart in place.

Laravel Cloud's worker command is read-only. Set `LARAVEL_CLOUD_QUEUES_MEMORY=256` for
a 256 MiB limit, or `LARAVEL_CLOUD_QUEUES_MEMORY=90%` for 90% of the container's cgroup
v2 limit. When neither the option nor the environment variable is set, managed mode
uses 90% of `/sys/fs/cgroup/memory.max`. A value of `max`, a missing file, or an unreadable
limit means there is no automatic limit. Self-managed `sqs` and `redis` have no default.
The resolved limit is logged at startup.

On other hosts, pass `--memory 256` or `--memory 90%`. The flag takes precedence over
the environment variable. MiB values must be finite and non-negative; percentages
must be between 0 and 100 and require a finite cgroup limit. Invalid settings exit `2`
before the consumer opens. Use `0` to disable memory recycling.

Linux reads current RSS from `/proc/self/statm`; elsewhere, or if that file cannot be
read, the worker uses `resource.getrusage()`'s peak RSS. This is a check between jobs,
so a job can still exceed the container limit before it finishes.

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
| `12` | The memory limit was reached after reporting the current delivery; restart in place on Laravel Cloud |
| `124` | A job timed out |

On Laravel Cloud, every exit is followed by a restart, so these codes are mostly
diagnostic. A configuration error restarts in a loop, and is logged clearly on every
start.

## Logging

All of the package's logging, in every mode, goes through Python's `logging` module under
the `laravel_cloud_queues` loggers, including the job and failure lines. When the worker
starts (`work` command or `Worker.run()`), it calls
[`laravel-cloud-logging`](https://pypi.org/project/laravel-cloud-logging/)'s `configure()`
if the root logger has no handlers yet, so every record is one Laravel-style line with its
level, context and exception chain, like a Laravel app's logs. On Laravel Cloud the lines
are JSON and go to the log socket. Elsewhere they go to stdout, as readable lines when
stdout is a terminal and JSON otherwise (`LOG_FORMAT=json` or `LOG_FORMAT=line` chooses).
Set `LOG_LEVEL` to change the level (default `INFO`). If your app already calls
`configure()`, or sets up any other logging, when it is imported, the worker keeps that
configuration and does not configure logging again.

Timeout diagnostics are written from the `SIGALRM` handler, where a logging lock may be
held, so they are written straight to stdout instead, in the same format.

Errors are reported as one actionable line. Pass `--debug` to see the full traceback.
Credentials in URLs and query strings are redacted.

## Long-Running Jobs

On the `sqs` and `redis` backends, the worker renews the job's lease itself while it
runs. It receives each message with a 60-second lease and extends it every 20 seconds. For
a sync handler, a watchdog thread renews the lease, even while the handler blocks the event
loop. For an `async def` handler, a task on the worker's loop renews it while the handler
awaits, so do not block the loop inside an async handler (see
[Sync and Async Handlers](jobs.md#sync-and-async-handlers)). If a renewal fails, the worker
no longer owns the message: it never reports success for that job, and exits with code `1`.

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
`max_time`, `stop_when_empty`, `stop_when_empty_for`, `timeout`, `sleep`, `rest` and
`memory` (a string such as `"256"` or `"90%"`).
