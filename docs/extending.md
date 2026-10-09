# Extending

## Introduction

The core of Laravel Cloud Queues is framework-independent. The FastAPI integration is
built on a small set of extension points, and you can use the same ones to integrate
another framework, run the worker from your own tooling, or teach the package about your
own types.

:::{note}
The `registry`, `worker`, `transports`, `codecs` and `jobs` modules are extension points
for adapters. Their signatures may still change during the 0.x releases. Modules and
packages whose names start with an underscore are internal.
:::

## Worker Targets

The worker runs a **worker target**: any object with a `registry` property and a
`lifespan()` method that returns an async context manager. The worker enters the lifespan
once per process and exits it on a clean shutdown:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from laravel_cloud_queues import Registry


class Service:
    def __init__(self) -> None:
        self.registry = Registry()

    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[None]:
        await connect_database()
        yield
        await disconnect_database()


service = Service()
```

```shell
laravel-cloud-queues work myapp.service:service
```

The worker resolves `module:attribute` to a `Registry`, to an object satisfying the
`WorkerTarget` protocol, or to an application whose `state.laravel_cloud_queues` is a
target. It never imports a framework itself.

## Invokers

An **invoker** decides how handlers are called. It implements two methods:

- `is_injected(parameter)` returns `True` for parameters the worker supplies at run time,
  rather than from the message.
- `invoke(job, args, kwargs, context)` calls the handler, runs any per-job teardown, and
  returns once teardown has completed. A teardown exception propagates as a handler
  failure.

The default invoker injects parameters annotated `JobContext`, calls sync handlers on the
current thread and awaits async ones. The FastAPI integration supplies an invoker that
resolves `Depends()` in a fresh scope for each delivery. Pass your own with
`Registry(invoker=...)`.

## Registration Helpers

An adapter typically wraps `Registry.job(...)` in framework-native declarations, such as
decorators, settings or management commands, while keeping the same wire format. Because
the message format is shared, a producer built with one adapter and a worker built with
another can share a queue.

## Mounting the CLI

The command line is a [click](https://click.palletsprojects.com/) group,
`laravel_cloud_queues.cli.cli`, which you may mount inside any click-based CLI, including
Flask's:

```python
from laravel_cloud_queues.cli import cli

app.cli.add_command(cli, "queues")  # flask queues work myapp:registry
```

A mounted group keeps the same options, error handling and exit codes.

For CLIs that are not built on click, such as a Django management command, call
`laravel_cloud_queues.cli.main(argv)`, which runs a command and returns its exit code:

```python
from laravel_cloud_queues.cli import main

exit_code = main(["work", "myproject.queues:registry", "--queue", "emails"])
```

## Custom Codecs

To serialize your own types as job arguments, register a codec on the registry's
`CodecRegistry`. See [Custom Codecs](serialization.md#custom-codecs).

## Transports

A new broker implements the `Producer` and `Consumer` protocols from
`laravel_cloud_queues.transports.base`. Transports work with opaque string bodies and
delivery records, and never see jobs or envelopes. A broker with an asyncio client may also
implement `AsyncProducer` and `AsyncConsumer` and pass factories for them to `Backend`;
without them, `dispatch_async` and the worker run the sync transport in a worker thread. Only the SQS, agent and Redis transports
are part of the product today, and the transport interface may still change during the
0.x releases.
