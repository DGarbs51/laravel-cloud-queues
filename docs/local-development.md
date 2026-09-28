# Local Development

## Introduction

To develop locally, run your web application as usual and start a worker in a second
terminal. All you need is a broker, and you may use whichever one you have on hand.

## Using Redis or Valkey

Any Redis or Valkey server works. [Laravel Herd](https://herd.laravel.com) bundles Valkey
on `127.0.0.1:6379`:

```shell
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0

laravel-cloud-queues work myapp.main:app
```

Remember to install the `redis` extra.

## Using a Local SQS

To develop against SQS semantics, including FIFO and fair queues, run
[LocalStack](https://www.localstack.cloud) or moto's server, and point the package at it
with `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT`:

```shell
export LARAVEL_CLOUD_QUEUES_BACKEND=sqs
export LARAVEL_CLOUD_QUEUES_SQS_PREFIX=http://localhost:4566/000000000000
export LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT=http://localhost:4566
export LARAVEL_CLOUD_QUEUES_SQS_REGION=us-east-1
export LARAVEL_CLOUD_QUEUES_SQS_KEY=test
export LARAVEL_CLOUD_QUEUES_SQS_SECRET=test

aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name default

laravel-cloud-queues work myapp.main:app
```

`LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` is the only endpoint override the package honors.
`AWS_ENDPOINT_URL` is ignored, and the override is refused when a managed configuration
is present.

## Exercising Managed Mode

To exercise managed mode locally, set `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` to a
document like the one shown in [Managed Queues](configuration.md#managed-queues), with
`"agent": {"enabled": false}`. The worker then receives directly from SQS, so you can
point the configuration's `connection.prefix` at LocalStack.

The agent path, and the observability socket, are exercised by the emulators in the
repository's `tests/harness/` directory.

## Working Through Jobs Quickly

A few worker options are handy during development:

```shell
# Process everything on the queue, then exit.
laravel-cloud-queues work myapp.main:app --stop-when-empty

# Process ten jobs, then exit.
laravel-cloud-queues work myapp.main:app --max-jobs 10

# Show full tracebacks.
laravel-cloud-queues work myapp.main:app --debug
```

Since the worker imports your code once, restart it after changing your jobs.

## Checking Your Setup

`inspect` prints the resolved mode, the queues and every registered job, without
connecting to the broker:

```shell
laravel-cloud-queues inspect myapp.main:app
```
