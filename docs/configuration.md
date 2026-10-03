# Configuration

## Introduction

Laravel Cloud Queues supports three queue backends. Choosing one is always explicit:
nothing is inferred from ambient environment variables such as `REDIS_URL` or
`AWS_ACCESS_KEY_ID`.

| Mode | Selected when | Broker | Jobs are received through |
|---|---|---|---|
| `managed` | `LARAVEL_CLOUD_QUEUES_BACKEND=managed`, or the variable is unset and `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present | Laravel Cloud's SQS queues | The Laravel Cloud queue agent when it is enabled, otherwise SQS directly |
| `sqs` | `LARAVEL_CLOUD_QUEUES_BACKEND=sqs` | Your own SQS queues | SQS directly |
| `redis` | `LARAVEL_CLOUD_QUEUES_BACKEND=redis` | Redis or Valkey | Redis |

If no backend is selected and no managed configuration is present, loading the
configuration raises a `ConfigurationError`, and the worker exits with code `2`.

:::{tip}
Not sure which backend to pick? On Laravel Cloud today, attach a Laravel Valkey cache and
use `redis`. For local development, any Redis or Valkey server works, including the Valkey
server bundled with Laravel Herd.
:::

## The Redis Backend

To use Redis or Valkey, set the backend and a connection URL:

```shell
LARAVEL_CLOUD_QUEUES_BACKEND=redis
LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0
```

The `redis` backend requires the `redis` extra. A `rediss://` URL enables TLS with
certificate verification turned on.

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_QUEUES_REDIS_URL` | `REDIS_URL` | The connection URL |
| `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE` | `default` | The default queue name |
| `LARAVEL_CLOUD_QUEUES_REDIS_PREFIX` | `laravel-cloud-queues:` | The prefix for every Redis key the package uses |

**Only in `redis` mode** does the package fall back to `REDIS_URL` when
`LARAVEL_CLOUD_QUEUES_REDIS_URL` is unset. On its own, `REDIS_URL` never selects a
backend, because many applications attach Valkey only for caching. Set
`LARAVEL_CLOUD_QUEUES_REDIS_URL` explicitly when you want your queues on a different
server, or a different database number, from your cache.

The Redis backend follows the semantics of Laravel's `redis` queue driver: a pending list
plus delayed and reserved sorted sets, atomic Lua scripts to reserve, release and delete
jobs, and an attempt counter per reservation. Several workers, even across clusters, can
share a queue without double delivery.

Redis queue names must not end with `:delayed`, `:reserved`, or `:notify`. These
suffixes are reserved for internal keys; using them raises `ConfigurationError`
before any Redis command is sent. Other colons in queue names are allowed.

:::{note}
FIFO and fair-queue options are SQS features. In `redis` mode they raise
`InvalidQueueOptionError`.
:::

## The SQS Backend

To use your own Amazon SQS queues, set the backend, the queue URL prefix, the region and
credentials:

```shell
LARAVEL_CLOUD_QUEUES_BACKEND=sqs
LARAVEL_CLOUD_QUEUES_SQS_PREFIX=https://sqs.us-east-2.amazonaws.com/123456789012
LARAVEL_CLOUD_QUEUES_SQS_REGION=us-east-2
LARAVEL_CLOUD_QUEUES_SQS_KEY=your-access-key-id
LARAVEL_CLOUD_QUEUES_SQS_SECRET=your-secret-access-key
```

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_QUEUES_SQS_PREFIX` | *required* | The queue URL prefix, such as `https://sqs.us-east-2.amazonaws.com/<account>` |
| `LARAVEL_CLOUD_QUEUES_SQS_REGION` | *required* | The AWS region. It is never defaulted |
| `LARAVEL_CLOUD_QUEUES_SQS_KEY` | *required* | The access key ID, unless `_SQS_CREDENTIALS=default` |
| `LARAVEL_CLOUD_QUEUES_SQS_SECRET` | *required* | The secret access key, unless `_SQS_CREDENTIALS=default` |
| `LARAVEL_CLOUD_QUEUES_SQS_SUFFIX` | *(empty)* | A suffix appended to logical queue names |
| `LARAVEL_CLOUD_QUEUES_SQS_QUEUE` | `default` | The default queue name |
| `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS` | *(unset)* | Set to `default` to opt into boto3's default credential chain |
| `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` | *(unset)* | An endpoint override for LocalStack or moto |

### Queue Names

Queue names resolve like they do in Laravel's `sqs` driver:

- A standard queue becomes `{prefix}/{queue}{suffix}`.
- A FIFO queue becomes `{prefix}/{base}{suffix}.fifo`.
- The suffix is only appended when the name does not already end with it.
- A queue name that is already a full URL is used as is.

Laravel Cloud queue names are at most 39 characters, including `.fifo`. Queues are never
created for you. Dispatching to a queue that does not exist raises
`ManagedQueueNotFoundError`.

### Why SQS Mode Has Its Own Variables

When you attach Laravel Cloud object storage (Cloudflare R2) to an environment, the
platform injects the **standard AWS variable names** into every container:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION=auto` and
more. Because boto3 honors `AWS_ENDPOINT_URL` for every service, an SQS client built from
the default credential chain in that container would send your SQS requests to R2, signed
with R2 keys.

For that reason, the package **never reads `AWS_*` variables in `sqs` mode**. Every
setting is passed to the boto3 client explicitly from `LARAVEL_CLOUD_QUEUES_SQS_*`, and
`AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_SQS` are ignored.

If you deliberately want boto3's default chain, for example local AWS profiles or IAM
roles on your own infrastructure, opt in explicitly:

```shell
LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default
```

## Managed Queues

In managed mode, Laravel Cloud injects `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` into your
containers. The package detects it automatically, so there is no AWS or queue wiring for
you to do: the queue URL prefix and suffix, the region, the credential provider, the
worker's queue assignment and whether the in-container queue agent is enabled all come
from that document.

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` | *(injected)* | Laravel Cloud's managed queue configuration document |
| `LARAVEL_CLOUD_AGENT_SOCKET` | `/tmp/cloud-agent.sock` | The agent socket, used when the document does not name one |
| `LARAVEL_CLOUD_LOG_SOCKET` | `unix:///tmp/cloud-init.sock` | The observability socket |

The document has this shape:

```json
{
  "driver": "cloud",
  "queue": "default",
  "queues": ["default", "emails"],
  "connection": {
    "prefix": "https://sqs.us-east-2.amazonaws.com/123456789012",
    "suffix": "",
    "queue": "default",
    "region": "us-east-2",
    "credentials": "ecs"
  },
  "agent": {"enabled": true, "socket": "/tmp/cloud-agent.sock"}
}
```

The package is strict about this document. Malformed JSON, a `driver` other than
`cloud`, a missing `connection` or `region`, or a `credentials` value other than `ecs` or
`instance` is a configuration error. The boto3 default credential chain is never used in
managed mode.

The `after_commit`, `overflow` and `credential_cache` settings are parsed and preserved,
but they are not implemented. The worker logs a warning when `overflow` or
`credential_cache` is enabled.

:::{note}
Managed queues are not yet available for Python applications on Laravel Cloud. Managed
mode is built and tested against the pinned Laravel contract and local emulators, and
will be verified live once the platform enables it.
:::

## Configuring in Code

Every setting may also be passed in code with `load_config`. Settings passed in code take
precedence over the environment:

<!-- runnable -->
```python
from laravel_cloud_queues import Registry, load_config

config = load_config(backend="redis", redis_url="redis://127.0.0.1:6379/0")
registry = Registry(config=config)

assert config.mode == "redis"
assert config.default_queue == "default"
```

With FastAPI, pass the configuration to the integration:

```python
queues = LaravelCloudQueues(app, config=config)
```

When you do not pass a configuration, the registry loads it lazily from the environment
the first time you dispatch a job or start a worker. It is never loaded in
[eager test mode](testing.md).

`load_config` accepts the following keyword arguments, each of which overrides the
matching environment variable: `backend`, `managed_config`, `sqs_prefix`, `sqs_suffix`,
`sqs_queue`, `sqs_region`, `sqs_credentials`, `sqs_endpoint`, `redis_url`, `redis_queue`,
`redis_prefix`, `log_socket` and `agent_socket`. You may also pass `env` to read settings
from a mapping other than `os.environ`.

To provide static SQS credentials in code, use `StaticCredentials`:

```python
from laravel_cloud_queues import load_config
from laravel_cloud_queues.config import StaticCredentials

config = load_config(
    backend="sqs",
    sqs_prefix="https://sqs.us-east-2.amazonaws.com/123456789012",
    sqs_region="us-east-2",
    sqs_credentials=StaticCredentials(key="...", secret="..."),
)
```

## Inspecting the Configuration

To see which mode, queues and jobs your application resolves to, run the `inspect`
command. It never connects to the broker and never prints secrets:

```shell
laravel-cloud-queues inspect myapp.main:app
```

Add `--json` for machine-readable output. See the [CLI reference](cli.md) for details.

## Secrets

Configuration objects have secret-safe representations: credentials, Redis URLs, receipt
handles and job payloads are never logged by default.
