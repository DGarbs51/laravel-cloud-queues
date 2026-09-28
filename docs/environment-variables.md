# Environment Variables

Every environment variable the package reads, in one place. Settings passed to
[`load_config`](configuration.md#configuring-in-code) in code take precedence over these
variables.

## Backend Selection

| Variable | Description |
|---|---|
| `LARAVEL_CLOUD_QUEUES_BACKEND` | `managed`, `sqs` or `redis`. When unset, managed mode is selected if `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` is present; otherwise, loading the configuration fails |

## Redis Backend

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_QUEUES_REDIS_URL` | `REDIS_URL` | The connection URL. `rediss://` enables TLS with certificate verification |
| `LARAVEL_CLOUD_QUEUES_REDIS_QUEUE` | `default` | The default queue |
| `LARAVEL_CLOUD_QUEUES_REDIS_PREFIX` | `laravel-cloud-queues:` | The key prefix |
| `REDIS_URL` | | Read **only** in `redis` mode, as the fallback URL. It never selects a backend |

## SQS Backend

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_QUEUES_SQS_PREFIX` | *required* | The queue URL prefix |
| `LARAVEL_CLOUD_QUEUES_SQS_REGION` | *required* | The AWS region |
| `LARAVEL_CLOUD_QUEUES_SQS_KEY` | *required* | The access key ID, unless `_SQS_CREDENTIALS=default` |
| `LARAVEL_CLOUD_QUEUES_SQS_SECRET` | *required* | The secret access key, unless `_SQS_CREDENTIALS=default` |
| `LARAVEL_CLOUD_QUEUES_SQS_SUFFIX` | *(empty)* | A suffix appended to queue names |
| `LARAVEL_CLOUD_QUEUES_SQS_QUEUE` | `default` | The default queue |
| `LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS` | *(unset)* | `default` opts into boto3's default credential chain. Any other value is an error |
| `LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT` | *(unset)* | An endpoint override for LocalStack or moto. Refused when a managed configuration is present |

`AWS_*` variables, including `AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_SQS`, are never
read in `sqs` mode.

## Managed Mode

| Variable | Default | Description |
|---|---|---|
| `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG` | *(injected by Laravel Cloud)* | The managed queue configuration document |
| `LARAVEL_CLOUD_AGENT_SOCKET` | `/tmp/cloud-agent.sock` | The queue agent socket, when the document does not name one |
| `LARAVEL_CLOUD_LOG_SOCKET` | `unix:///tmp/cloud-init.sock` | The observability socket for lifecycle and `failed_job` events |
