# Arguments and Payloads

## Introduction

When you dispatch a job, its arguments are encoded into a versioned JSON message. The
worker decodes them using your handler's **type annotations**. The message never names a
Python type, module or callable, and the worker never imports anything because a message
asked it to. Nothing is ever pickled.

<!-- runnable -->
```python
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from laravel_cloud_queues import Registry

registry = Registry()


class Priority(Enum):
    LOW = "low"
    HIGH = "high"


@dataclass(frozen=True)
class Invoice:
    id: UUID
    issued_at: datetime
    lines: tuple[str, ...]


@registry.job(name="invoices.render")
def render_invoice(invoice: Invoice, priority: Priority = Priority.LOW) -> None:
    assert isinstance(invoice.id, UUID) and isinstance(invoice.issued_at, datetime)
    print(f"rendering {invoice.id} at {priority.value} priority")


with registry.testing():
    render_invoice.dispatch(
        Invoice(uuid4(), datetime.now(timezone.utc), ("Widget", "Gadget")), priority=Priority.HIGH
    )
```

## Supported Types

Out of the box, job arguments may use these types, nested as deeply as you need:

- JSON primitives: `str`, `int`, `float`, `bool` and `None`
- `list[...]` and `dict[str, ...]` (dictionary keys must be strings)
- `tuple[...]`, with element annotations such as `tuple[str, ...]` or `tuple[int, str]`
- Dataclasses
- Enums and `Literal[...]`
- Unions and optionals, such as `int | None`. Members are tried in declaration order.
- `UUID`, `datetime`, `date`, `time`, `Decimal` and `bytes`
- Pydantic v2 models, when Pydantic is installed
- Any type with a [custom codec](#custom-codecs)

Annotations are checked when the job is registered, so an unsupported parameter type
raises a `ConfigurationError` right away instead of failing on the worker.

Arguments are validated against the handler's signature when they are decoded, and before
the handler runs. A value that does not match its annotation is a job defect: it fails on
the first delivery and is never retried.

:::{note}
Parameters annotated `Any`, or not annotated at all, accept JSON values and the built-in
scalar types, but they never construct dataclasses, models, custom classes or tuples.
Annotate your parameters precisely.
:::

## Custom Codecs

To pass your own types as job arguments, register a **codec** on the registry's codec
registry. A codec has a unique `tag`, the `python_type` it handles, and `encode` and
`decode` methods that convert to and from JSON:

<!-- runnable -->
```python
from decimal import Decimal

from laravel_cloud_queues import Registry
from laravel_cloud_queues.codecs import JSONValue


class Money:
    def __init__(self, amount: Decimal, currency: str) -> None:
        self.amount = amount
        self.currency = currency


class MoneyCodec:
    tag = "money"
    python_type = Money

    def encode(self, value: Money) -> JSONValue:
        return {"amount": str(value.amount), "currency": value.currency}

    def decode(self, data: JSONValue) -> Money:
        assert isinstance(data, dict)
        return Money(Decimal(str(data["amount"])), str(data["currency"]))


registry = Registry()
registry.codecs.register(MoneyCodec())


@registry.job(name="invoices.charge")
def charge(customer_id: int, total: Money) -> None:
    print(f"charging {customer_id} {total.amount} {total.currency}")


with registry.testing():
    charge.dispatch(customer_id=7, total=Money(Decimal("19.99"), "USD"))
```

Register codecs **before** you declare the jobs that use them, because annotations are
validated at registration. You may also build a `CodecRegistry` up front and pass it to
`Registry(codecs=...)`.

A codec's tag is only ever resolved through the handler's trusted annotation, never from
message content alone. Codecs cannot replace a built-in type, an enum, a dataclass or a
Pydantic model, and each tag and type may only be registered once.

## Payload Limits

The encoded message is measured in UTF-8 bytes before it is sent:

- **SQS and managed queues** allow **1,048,576 bytes** (1 MiB), Laravel Cloud's documented
  job payload limit. Larger payloads raise `PayloadTooLargeError`, with `.size` and
  `.limit` attributes. A queue configured with a lower `MaximumMessageSize` raises the
  same error.
- **Redis** has no transport limit of its own. It is bounded only by the package's 16 MiB
  decoding ceiling, above which dispatch also raises `PayloadTooLargeError`.

:::{warning}
Moving an application from Redis to SQS or managed queues reintroduces the 1 MiB limit.
Nothing is truncated, compressed or offloaded to storage. If you need to process large
data, store it elsewhere and pass an identifier.
:::

## The Trust Boundary

Message bodies are untrusted input:

- Decoding is bounded in size (16 MiB) and nesting depth (64 levels).
- `NaN` and `Infinity`, duplicate keys and invalid UTF-8 are rejected.
- Malformed messages, unsupported message versions, unknown job names, codec failures and
  argument mismatches are deterministic defects. They fail on the first delivery and are
  never retried.

## Writing Good Payloads

- **Never put secrets in job arguments.** Failure records carry the full payload into
  your logs and the Laravel Cloud dashboard. Pass identifiers and look secrets up inside
  the handler.
- **Pass identifiers, not objects.** The producer and the worker are the same application,
  but they may run on different machines and on different deployment revisions. Never
  rely on process-local objects, memory or filesystem state in a payload.
- **Evolve signatures carefully.** Messages already on the queue were encoded against the
  old signature. Add new parameters with defaults, and keep old job names working until
  their messages have drained.

## The Message Format

Each message is one JSON object. Laravel's top-level `uuid` and `displayName` keys sit
beside a versioned `laravel_cloud_queues` section:

```json
{
  "uuid": "7f0c5b1e-...",
  "displayName": "emails.send",
  "laravel_cloud_queues": {
    "version": 1,
    "job": "emails.send",
    "args": [],
    "kwargs": {"user_id": 42, "template": "welcome"},
    "policy": {"tries": 3, "backoff": [1, 5]},
    "queue": "emails",
    "dispatched_at": "2026-09-27T12:00:00.123456+00:00",
    "context": {"traceparent": "..."}
  }
}
```

Tagged values, such as a `UUID`, are encoded as `{"$type": "uuid", "value": "..."}`.
The `policy` omits fields the job did not declare, and unknown keys are preserved and
ignored so that newer producers can talk to older workers.
