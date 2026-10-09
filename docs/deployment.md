# Deploying to Laravel Cloud

## Introduction

Laravel Cloud runs Python applications, including FastAPI, on Python 3.10 through 3.14.
This package requires Python 3.11 or newer.
Your web process dispatches jobs, and a separate worker process runs them. On Laravel
Cloud, that worker runs as a **worker cluster** or as a **background process** on your App
cluster.

Managed queues are not yet available for Python applications, so today you point your
worker at one of the two self-managed backends: a Laravel Valkey cache (`redis`) or your
own SQS queues (`sqs`).

## Using Laravel Valkey

The simplest way to run queues on Laravel Cloud today is with a Laravel Valkey cache.
Attach a Valkey cache to your environment, install the `redis` extra, and add one
environment variable:

```text
LARAVEL_CLOUD_QUEUES_BACKEND=redis
```

Laravel Cloud injects `REDIS_URL` (a `rediss://` TLS URL) for the attached cache, and in
`redis` mode the package uses it when `LARAVEL_CLOUD_QUEUES_REDIS_URL` is unset. If your
application also uses the cache for caching, you may want to keep queue keys in a
separate database by setting `LARAVEL_CLOUD_QUEUES_REDIS_URL` explicitly.

Then create a worker cluster, or a background process, with this command:

```text
laravel-cloud-queues work myapp.main:app
```

## Using Your Own SQS Queues

Create your queues in your AWS account, then add these environment variables:

```text
LARAVEL_CLOUD_QUEUES_BACKEND=sqs
LARAVEL_CLOUD_QUEUES_SQS_PREFIX=https://sqs.us-east-2.amazonaws.com/123456789012
LARAVEL_CLOUD_QUEUES_SQS_REGION=us-east-2
LARAVEL_CLOUD_QUEUES_SQS_KEY=your-access-key-id
LARAVEL_CLOUD_QUEUES_SQS_SECRET=your-secret-access-key
```

:::{warning}
Do not rely on `AWS_*` variables for SQS on Laravel Cloud. When object storage is
attached, those variables point at Cloudflare R2, not AWS. The package deliberately
ignores them in `sqs` mode. See [Why SQS Mode Has Its Own Variables](configuration.md#why-sqs-mode-has-its-own-variables).
:::

There is no Laravel Cloud queue agent outside managed mode, so the worker keeps long jobs
alive itself. It receives each message with a 60-second lease and extends it every 20
seconds while the job runs: from a watchdog thread for a sync handler, even while it blocks
the event loop, and from a task on the loop for an `async def` handler. If a renewal fails
because the message was deleted or reassigned, the worker has lost its lease: it never
reports success for a job it no longer owns, and it exits with code `1`.

## Worker Clusters Are Supervised

Laravel Cloud runs your worker as a **long-lived service and restarts it whenever it
exits**, for any reason. Keep these consequences in mind:

- **Do not** use `--stop-when-empty` or `--stop-when-empty-for` on Laravel Cloud. A worker
  that exits on an empty queue is restarted immediately, in a loop. Both options are off
  by default.
- `--max-jobs` and `--max-time` are useful for recycling the worker process periodically.
  Your FastAPI lifespan runs again in the new process.
- A configuration error (exit code `2`) causes a restart loop. The worker logs the problem
  clearly on every start, so check your environment's **Logs** tab.
- After a job timeout, the worker exits with code `124` and is restarted. See
  [Timeouts](retries-and-timeouts.md#timeouts).
- Worker clusters scale on CPU, memory or a fixed instance count, **not** on queue depth.
  Size the cluster for your expected throughput.

## Runtime and Shutdown Limits

Laravel Cloud gives **Flex** workers 90 seconds to finish on shutdown and caps job
runtime at 90 seconds. **Pro** workers have no fixed runtime limit and get one hour to
finish. Keep your job [timeouts](retries-and-timeouts.md#timeouts) inside those limits.

Web requests are cut off after 20 seconds, so keep dispatching inside a request short. A
single dispatch is one SQS `SendMessage` call or one Redis command.

## Thread Pools Are Small

On Laravel Cloud, `os.cpu_count()` reports your instance's CPU quota, not the host's. Python
sizes its default thread pools from it: a `ThreadPoolExecutor` without `max_workers`, and
asyncio's default executor behind `asyncio.to_thread()` and `loop.run_in_executor(None,
...)`, get `cpu_count + 4` threads, which is 5 threads on 1 vCPU. Code that leans on those
pools queues up quickly.

The `redis` backend's `dispatch_async` and worker, and the managed agent worker, are native
async and use no threads at all. The paths that still need a thread (SQS sends and
receives) use AnyIO's worker threads, which are not sized from the CPU count. See
[Native Async Paths](dispatching.md#native-async-paths).

## Managed Queues

When Laravel Cloud enables managed queues for Python, the platform will inject
`LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` and the package will select managed mode
automatically. No AWS or queue wiring is needed.

In managed mode, jobs are dispatched straight to Laravel Cloud's SQS queues. The worker
receives them through the queue agent's Unix socket when the agent is enabled, or
directly from SQS otherwise. Lifecycle and `failed_job` events go to Laravel Cloud's log
socket, so your jobs appear in the **Queues** dashboard, and failed jobs can be inspected
and retried there. The agent extends a running job's visibility in three-minute increments
and manages the worker's lifetime and scaling.

The worker processes `--queue`, or the configuration's `queue` when you omit it. Jobs
dispatched without a queue go to `connection.queue`. With the agent enabled, each worker
processes one queue.

## Smoke Testing Your Deployment

Two canary applications run the released package on Laravel Cloud, one branch per Python
version, in `redis` mode against Laravel Valkey:

- [`fastapi-cloud-queues`](https://github.com/DGarbs51/fastapi-cloud-queues) (FastAPI)
- [`python-cloud-queues`](https://github.com/DGarbs51/python-cloud-queues) (plain Python)

Each has a dashboard with a **Run check** button that dispatches one job per scenario
(quick, async, slow, retry, terminal failure and timeout) and reports a pass or fail
verdict for each one. You may use either as a template for a live check of your own
deployment.
