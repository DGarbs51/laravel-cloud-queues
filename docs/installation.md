# Installation

## Requirements

Laravel Cloud Queues has a few requirements. Make sure your environment meets them
before you install the package:

- CPython 3.10, 3.11, 3.12, 3.13 or 3.14
- Linux (the production runtime on Laravel Cloud) or macOS (development). Windows is
  supported on a best-effort basis only.
- FastAPI 0.121 or later if you use the FastAPI integration

Async code runs on AnyIO with the asyncio backend. Trio is not supported.

## Installing the Package

You may install the package from PyPI with `pip`, `uv` or any other Python package
manager. Most FastAPI applications want the `fastapi` extra:

```shell
pip install "laravel-cloud-queues[fastapi]"
```

If you plan to use the Redis backend, which is the easiest way to run queues on Laravel
Cloud today, include the `redis` extra as well:

```shell
pip install "laravel-cloud-queues[fastapi,redis]"
```

With `uv`:

```shell
uv add "laravel-cloud-queues[fastapi,redis]"
```

### Optional Extras

The core package depends only on `boto3`, `anyio`, `click`, `httpx` and
`typing-extensions`. Everything else is opt-in:

| Extra | Installs | Use it when |
|---|---|---|
| `fastapi` | `fastapi>=0.121` | You use the [FastAPI integration](fastapi.md) |
| `redis` | `redis>=5.0` | You use the `redis` backend (Redis or Laravel Valkey) |
| `otel` | `opentelemetry-api>=1.27` | You want [trace propagation](observability.md#tracing) from dispatch to worker |

Pydantic v2 models are supported as job arguments whenever Pydantic is installed. FastAPI
installs it for you.

## Verifying the Installation

The package installs a `laravel-cloud-queues` console script, along with the shorter `lcq`
alias. Check that it is available:

```shell
laravel-cloud-queues --help
```

You may also check the installed version from Python:

```python
import laravel_cloud_queues

print(laravel_cloud_queues.__version__)
```

## Versioning

Laravel Cloud Queues is a `0.x` package. The FastAPI API is still settling, and live
Laravel Cloud managed-queue support is not yet verified. The
[public API](api/index.md) is kept stable within the 0.x line as far as practical.
Version `1.0.0` will be released once the core and FastAPI APIs are intentionally stable
and the Laravel Cloud contract has been verified live. After that, breaking changes will
require a major version.

## Next Steps

Once the package is installed, [configure a queue backend](configuration.md).
