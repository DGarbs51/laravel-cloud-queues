"""Dispatch pipeline: queue resolution, option validation, envelope, size, send, telemetry."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest

from laravel_cloud_queues import JobContext
from laravel_cloud_queues.config import QueueConfig, SqsConnectionConfig
from laravel_cloud_queues.errors import (
    ArgumentError,
    InvalidQueueOptionError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
)
from laravel_cloud_queues.jobs import dispatch as dispatch_module
from laravel_cloud_queues.jobs.dispatch import prepare_dispatch, send_prepared
from laravel_cloud_queues.jobs.envelope import ENVELOPE_KEY, MAX_BODY_BYTES, decode_envelope
from laravel_cloud_queues.jobs.job import DispatchOptions, DispatchReceipt
from laravel_cloud_queues.observability import NullSink, Telemetry
from laravel_cloud_queues.registry import Registry
from laravel_cloud_queues.transports import Backend
from tests.unit.jobs.fakes import (
    FakeAsyncProducer,
    FakeProducer,
    RecordingTelemetry,
    make_registry,
)


def section(body: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(body)[ENVELOPE_KEY]
    return data


def test_dispatch_sends_envelope_and_returns_receipt() -> None:
    registry, producer, _ = make_registry()

    @registry.job(name="emails.send", queue="emails", tries=3, backoff=[1, 5])
    def send(user_id: int, template: str = "welcome") -> None: ...

    receipt = send.dispatch(123, template="reset")

    assert len(producer.sent) == 1
    message = producer.sent[0]
    assert receipt == DispatchReceipt(message_id="msg-1", queue="emails", uuid=receipt.uuid)
    assert message.queue == "emails"
    assert (message.delay_seconds, message.fifo_group, message.deduplication_id) == (0, None, None)
    assert message.message_group is None

    raw = json.loads(message.body)
    assert raw["uuid"] == receipt.uuid
    assert str(uuid.UUID(receipt.uuid)) == receipt.uuid
    assert raw["displayName"] == "emails.send"
    envelope = decode_envelope(message.body)
    assert envelope.job == "emails.send"
    assert envelope.args == (123,)
    assert dict(envelope.kwargs) == {"template": "reset"}
    assert envelope.queue == "emails"
    assert envelope.policy.tries == 3
    assert tuple(envelope.policy.backoff or ()) == (1, 5)
    assert envelope.policy.timeout is None
    assert envelope.policy.fail_on_timeout is None
    assert envelope.dispatched_at is not None
    dispatched_at = datetime.fromisoformat(envelope.dispatched_at)
    assert dispatched_at.utcoffset() == timedelta(0)
    assert abs(datetime.now(UTC) - dispatched_at) < timedelta(minutes=1)


def test_policy_section_only_has_declared_fields() -> None:
    registry, producer, _ = make_registry()
    registry.job(name="x", timeout=5)(lambda: None).dispatch()
    assert section(producer.sent[0].body)["policy"] == {"timeout": 5}


def test_each_dispatch_gets_a_new_uuid() -> None:
    registry, _, _ = make_registry()
    job = registry.job(name="x")(lambda: None)
    assert job.dispatch().uuid != job.dispatch().uuid


def test_trace_context_is_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    carrier = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
    monkeypatch.setattr(dispatch_module, "inject_trace_context", lambda: dict(carrier))
    registry, producer, _ = make_registry()
    registry.job(name="x")(lambda: None).dispatch()
    assert dict(decode_envelope(producer.sent[0].body).context) == carrier


# --- queue resolution -----------------------------------------------------------------


def test_queue_resolution_order() -> None:
    config = QueueConfig(
        mode="sqs",
        sqs=SqsConnectionConfig(
            prefix="https://sqs.us-east-2.amazonaws.com/1",
            region="us-east-2",
            credentials="default",
            queue="configured",
        ),
    )
    registry, producer, _ = make_registry(config=config)
    with_queue = registry.job(name="a", queue="job-queue")(lambda: None)
    without_queue = registry.job(name="b")(lambda: None)

    assert without_queue.dispatch().queue == "configured"
    assert with_queue.dispatch().queue == "job-queue"
    assert with_queue.options(queue="override").dispatch().queue == "override"
    assert [m.queue for m in producer.sent] == ["configured", "job-queue", "override"]
    assert section(producer.sent[2].body)["queue"] == "override"


def test_empty_queue_rejected() -> None:
    registry, producer, _ = make_registry()
    with pytest.raises(InvalidQueueOptionError):
        registry.job(name="a")(lambda: None).options(queue="").dispatch()
    assert producer.sent == []


# --- option validation matrix ---------------------------------------------------------

SQS_ID_128 = "a" * 128
SQS_ID_129 = "a" * 129
PUNCTUATION = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"


@pytest.mark.parametrize(
    ("queue", "options", "expected"),
    [
        ("emails", {}, (0, None, None, None)),
        ("emails", {"delay": 30}, (30, None, None, None)),
        ("emails", {"delay": 0.2}, (1, None, None, None)),
        ("emails", {"delay": timedelta(minutes=15)}, (900, None, None, None)),
        ("emails", {"message_group": "tenant-1"}, (0, None, None, "tenant-1")),
        ("emails", {"message_group": PUNCTUATION}, (0, None, None, PUNCTUATION)),
        ("emails", {"message_group": SQS_ID_128}, (0, None, None, SQS_ID_128)),
        ("orders.fifo", {}, (0, "orders.fifo", "<new>", None)),
        ("orders.fifo", {"delay": 0}, (0, "orders.fifo", "<new>", None)),
        ("orders.fifo", {"group": "customer-9"}, (0, "customer-9", "<new>", None)),
        ("orders.fifo", {"deduplication_id": "order-1"}, (0, "orders.fifo", "order-1", None)),
        ("orders.fifo", {"deduplication_id": ""}, (0, "orders.fifo", None, None)),
        ("orders.fifo", {"group": SQS_ID_128}, (0, SQS_ID_128, "<new>", None)),
    ],
)
def test_sqs_options(queue: str, options: dict[str, Any], expected: tuple[Any, ...]) -> None:
    """FIFO defaults follow Laravel SqsQueue::getQueueableOptions: group = queue name incl.
    ``.fifo`` (framework/src/Illuminate/Queue/SqsQueue.php:610), dedup = new unique ID
    (:629), empty values omitted (:635)."""
    registry, producer, _ = make_registry()
    registry.job(name="a")(lambda: None).options(queue=queue, **options).dispatch()
    message = producer.sent[0]
    delay, group, dedup, message_group = expected
    assert message.delay_seconds == delay
    assert message.fifo_group == group
    assert message.message_group == message_group
    if dedup == "<new>":
        assert message.deduplication_id
        assert len(message.deduplication_id) <= 128
    else:
        assert message.deduplication_id == dedup


@pytest.mark.parametrize(
    ("queue", "options"),
    [
        # standard queues: FIFO options rejected (labeled deviation: Laravel forwards)
        ("emails", {"group": "g"}),
        ("emails", {"deduplication_id": "d"}),
        ("emails", {"deduplication_id": ""}),
        ("emails", {"message_group": ""}),
        ("emails", {"message_group": "has space"}),
        ("emails", {"message_group": "é"}),
        ("emails", {"message_group": SQS_ID_129}),
        # delays
        ("emails", {"delay": -1}),
        ("emails", {"delay": 901}),
        ("emails", {"delay": 900.01}),
        ("emails", {"delay": float("nan")}),
        ("emails", {"delay": float("inf")}),
        # FIFO: no per-message delay (labeled deviation: Laravel omits it), no fair groups
        ("orders.fifo", {"delay": 1}),
        ("orders.fifo", {"delay": 0.01}),
        ("orders.fifo", {"delay": timedelta(seconds=5)}),
        ("orders.fifo", {"message_group": "tenant"}),
        ("orders.fifo", {"group": ""}),
        ("orders.fifo", {"group": SQS_ID_129}),
        ("orders.fifo", {"group": "tab\there"}),
        ("orders.fifo", {"deduplication_id": SQS_ID_129}),
        ("orders.fifo", {"deduplication_id": "ünicode"}),
    ],
)
def test_sqs_options_rejected(queue: str, options: dict[str, Any]) -> None:
    registry, producer, telemetry = make_registry()
    job = registry.job(name="a")(lambda: None).options(queue=queue, **options)
    with pytest.raises(InvalidQueueOptionError):
        job.dispatch()
    assert producer.sent == []
    assert telemetry.events == []


@pytest.mark.parametrize(
    ("queue", "options", "delay"),
    [
        ("emails", {}, 0),
        ("emails", {"delay": 900}, 900),
        ("orders.fifo", {}, 0),  # just a name: redis has no FIFO semantics
        ("orders.fifo", {"delay": 5}, 5),
    ],
)
def test_redis_options(queue: str, options: dict[str, Any], delay: int) -> None:
    registry, producer, _ = make_registry(mode="redis")
    registry.job(name="a")(lambda: None).options(queue=queue, **options).dispatch()
    message = producer.sent[0]
    assert message.delay_seconds == delay
    assert (message.fifo_group, message.deduplication_id, message.message_group) == (
        None,
        None,
        None,
    )


@pytest.mark.parametrize(
    ("queue", "options"),
    [
        ("emails", {"group": "g"}),
        ("emails", {"deduplication_id": ""}),
        ("emails", {"message_group": "tenant"}),
        ("orders.fifo", {"group": "g"}),
        ("orders.fifo", {"deduplication_id": "d"}),
        ("emails", {"delay": 901}),
    ],
)
def test_redis_options_rejected(queue: str, options: dict[str, Any]) -> None:
    registry, producer, _ = make_registry(mode="redis")
    with pytest.raises(InvalidQueueOptionError):
        registry.job(name="a")(lambda: None).options(queue=queue, **options).dispatch()
    assert producer.sent == []


def test_fifo_dedup_id_is_chosen_once_per_logical_dispatch() -> None:
    """Chosen during prepare and carried in the OutgoingMessage, so every send of the same
    prepared dispatch (SDK-internal retries) reuses it; each new dispatch gets a new one."""
    registry, producer, _ = make_registry()
    job = registry.job(name="a")(lambda: None).options(queue="orders.fifo")
    prepared = prepare_dispatch(job, (), {}, job.dispatch_options)
    send_prepared(job, prepared)
    send_prepared(job, prepared)
    job.dispatch()
    first, retry, other = (m.deduplication_id for m in producer.sent)
    assert first == retry == prepared.message.deduplication_id
    assert other != first


def test_options_are_validated_before_arguments_are_encoded() -> None:
    registry, _, _ = make_registry()
    job = registry.job(name="a")(lambda x: None)
    with pytest.raises(InvalidQueueOptionError):
        job.options(queue="q.fifo", delay=5).dispatch(object())


# --- payload size ---------------------------------------------------------------------


def test_payload_size_is_measured_in_utf8_bytes() -> None:
    registry, producer, _ = make_registry(producer=FakeProducer(max_payload_bytes=None))
    job = registry.job(name="a", queue="q")(lambda text: None)
    job.dispatch("é" * 500)
    body = producer.sent[0].body
    chars, size = len(body), len(body.encode("utf-8"))
    assert size > chars

    tight, tight_producer, _ = make_registry(producer=FakeProducer(max_payload_bytes=chars))
    tight_job = tight.job(name="a", queue="q")(lambda text: None)
    with pytest.raises(PayloadTooLargeError) as info:
        tight_job.dispatch("é" * 500)
    assert (info.value.size, info.value.limit, info.value.queue) == (size, chars, "q")
    assert tight_producer.sent == []

    exact, exact_producer, _ = make_registry(producer=FakeProducer(max_payload_bytes=size))
    exact.job(name="a", queue="q")(lambda text: None).dispatch("é" * 500)
    assert len(exact_producer.sent) == 1


def test_redis_is_bounded_only_by_the_decode_ceiling() -> None:
    """No transport limit (redis): beyond SQS's 1 MiB is fine, but a body the worker could
    not decode (> MAX_BODY_BYTES) is rejected at dispatch."""
    registry, producer, _ = make_registry(mode="redis")
    job = registry.job(name="a", queue="q")(lambda text: None)
    job.dispatch("x" * 2_000_000)
    assert len(producer.sent[0].body) > 2_000_000

    with pytest.raises(PayloadTooLargeError) as info:
        job.dispatch("x" * MAX_BODY_BYTES)
    assert info.value.limit == MAX_BODY_BYTES
    assert info.value.size > MAX_BODY_BYTES
    assert info.value.queue == "q"
    assert len(producer.sent) == 1


# --- arguments ------------------------------------------------------------------------


def test_argument_errors_send_nothing() -> None:
    registry, producer, _ = make_registry()

    @registry.job(name="a")
    def handler(user_id: int, context: JobContext) -> None: ...

    with pytest.raises(ArgumentError):
        handler.dispatch(1, extra=2)  # type: ignore[call-arg]
    with pytest.raises(ArgumentError):
        handler.dispatch()  # type: ignore[call-arg]
    with pytest.raises(ArgumentError):
        handler.dispatch(
            1,
            context=JobContext(  # injected parameters never come from callers
                job_name="a", uuid="u", message_id="m", queue="q", attempt=1, max_tries=1
            ),
        )
    assert producer.sent == []


def test_handler_parameters_named_like_options() -> None:
    """``queue``/``delay``/``timeout`` handler parameters are job arguments, never options."""
    registry, producer, _ = make_registry()

    @registry.job(name="a", queue="jobs")
    def handler(queue: str, delay: int, timeout: float) -> None: ...

    receipt = handler.options(delay=3).dispatch(queue="other", delay=60, timeout=1.5)
    message = producer.sent[0]
    assert receipt.queue == message.queue == "jobs"
    assert message.delay_seconds == 3
    envelope = decode_envelope(message.body)
    assert dict(envelope.kwargs) == {"queue": "other", "delay": 60, "timeout": 1.5}


# --- send + telemetry -----------------------------------------------------------------


def test_queued_event_after_successful_send(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, datetime]] = []

    def fake_lifecycle_event(type_: str, queue: str, *, timestamp: datetime) -> dict[str, object]:
        calls.append((type_, queue, timestamp))
        return {"type": type_, "queue": queue}

    monkeypatch.setattr(dispatch_module, "lifecycle_event", fake_lifecycle_event)
    registry, _, telemetry = make_registry()
    registry.job(name="a", queue="emails")(lambda: None).dispatch()
    assert telemetry.events == [{"type": "queued", "queue": "emails"}]
    assert calls[0][:2] == ("queued", "emails")
    assert calls[0][2].tzinfo is not None


def test_send_failure_propagates_without_queued_event() -> None:
    producer = FakeProducer(error=ManagedQueueNotFoundError("missing"))
    registry, _, telemetry = make_registry(producer=producer)
    with pytest.raises(ManagedQueueNotFoundError):
        registry.job(name="a")(lambda: None).dispatch()
    assert telemetry.events == []


def test_telemetry_failure_never_fails_a_sent_dispatch() -> None:
    telemetry = RecordingTelemetry(error=RuntimeError("socket gone"))
    registry, producer, _ = make_registry(telemetry=telemetry)
    receipt = registry.job(name="a")(lambda: None).dispatch()
    assert receipt.message_id == "msg-1"
    assert len(producer.sent) == 1


def test_backend_is_built_lazily_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from laravel_cloud_queues import registry as registry_module

    producer = FakeProducer()
    built: list[QueueConfig] = []

    def fake_create_backend(config: QueueConfig) -> Backend:
        built.append(config)
        return Backend(mode="sqs", producer=producer, consumer_factory=lambda **_: None)  # type: ignore[arg-type,return-value]

    monkeypatch.setattr(registry_module, "create_backend", fake_create_backend)
    config = QueueConfig(mode="sqs")
    registry = registry_module.Registry(config=config, telemetry=RecordingTelemetry())
    job = registry.job(name="a")(lambda: None)
    assert built == []
    job.dispatch()
    job.dispatch()
    assert built == [config]
    assert len(producer.sent) == 2


# --- sync / async ---------------------------------------------------------------------


def test_dispatch_async_sends_from_a_worker_thread() -> None:
    registry, producer, _ = make_registry()
    job = registry.job(name="a", queue="q")(lambda x: None)

    async def main() -> DispatchReceipt:
        return await job.dispatch_async(1)

    receipt = anyio.run(main)
    assert receipt.queue == "q"
    assert producer.threads[0] != threading.get_ident()
    assert decode_envelope(producer.sent[0].body).args == (1,)


def test_dispatch_async_raises_dispatch_errors() -> None:
    registry, producer, _ = make_registry()
    job = registry.job(name="a")(lambda x: None)

    async def main() -> None:
        with pytest.raises(InvalidQueueOptionError):
            await job.options(delay=-1).dispatch_async(1)
        with pytest.raises(ArgumentError):
            await job.dispatch_async()

    anyio.run(main)
    assert producer.sent == []


def _forbid_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_thread(*args: object, **kwargs: object) -> None:
        raise AssertionError("dispatch_async must stay on the loop")

    monkeypatch.setattr(anyio.to_thread, "run_sync", no_thread)


def test_dispatch_async_with_a_native_producer_never_leaves_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry = RecordingTelemetry(emits=False)  # redis mode
    registry, producer, _ = make_registry(mode="redis", native_async=True, telemetry=telemetry)
    job = registry.job(name="a", queue="q")(lambda x: None)
    _forbid_threads(monkeypatch)

    async def main() -> list[DispatchReceipt]:
        return [await job.dispatch_async(1), await job.dispatch_async(2)]

    first, second = anyio.run(main)
    assert (first.message_id, second.message_id) == ("msg-1", "msg-2")
    assert first.uuid == decode_envelope(producer.sent[0].body).uuid
    assert producer.threads == [threading.get_ident()] * 2
    [native] = producer.opened
    assert len(native.loops) == 2


def test_dispatch_async_builds_the_backend_once_in_a_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from laravel_cloud_queues import registry as registry_module

    producer = FakeProducer(supports_fifo=False, max_payload_bytes=None)
    threads: list[int] = []

    def fake_create_backend(config: QueueConfig) -> Backend:
        threads.append(threading.get_ident())
        return Backend(
            mode="redis",
            producer=producer,
            consumer_factory=lambda **_: None,  # type: ignore[arg-type,return-value]
            async_producer_factory=lambda: FakeAsyncProducer(producer),
        )

    monkeypatch.setattr(registry_module, "create_backend", fake_create_backend)
    registry = Registry(
        config=QueueConfig(mode="redis"),
        telemetry=Telemetry(sink=NullSink(), emits_cloud_events=False),
    )
    job = registry.job(name="a")(lambda: None)
    hops: list[object] = []
    original = anyio.to_thread.run_sync

    async def counting(func: Any, *args: Any, **kwargs: Any) -> Any:
        hops.append(func)
        return await original(func, *args, **kwargs)

    monkeypatch.setattr(anyio.to_thread, "run_sync", counting)

    async def main() -> None:
        await job.dispatch_async()
        await job.dispatch_async()

    anyio.run(main)
    assert len(threads) == 1 and threads[0] != threading.get_ident()
    assert len(hops) == 1
    assert producer.threads == [threading.get_ident()] * 2


def test_dispatch_async_emits_queued_off_the_loop_after_the_native_send() -> None:
    registry, producer, telemetry = make_registry(native_async=True)
    job = registry.job(name="a", queue="emails")(lambda: None)
    anyio.run(job.dispatch_async)
    assert [event["type"] for event in telemetry.events] == ["queued"]
    assert telemetry.events[0]["queue"] == "emails"
    assert telemetry.threads[0] != threading.get_ident()
    assert producer.threads == [threading.get_ident()]


def test_async_telemetry_failure_never_fails_a_sent_dispatch() -> None:
    telemetry = RecordingTelemetry(error=RuntimeError("socket gone"))
    registry, producer, _ = make_registry(native_async=True, telemetry=telemetry)
    receipt = anyio.run(registry.job(name="a")(lambda: None).dispatch_async)
    assert receipt.message_id == "msg-1"
    assert len(producer.sent) == 1


class SuspendingProducer(FakeAsyncProducer):
    """A native producer whose send suspends, then calls ``on_sent`` once it is accepted."""

    def __init__(self, sync: FakeProducer) -> None:
        super().__init__(sync)
        self.on_sent: Any = None

    async def send(self, message: Any) -> Any:
        await anyio.lowlevel.checkpoint()
        sent = await super().send(message)
        if self.on_sent is not None:
            self.on_sent()
        return sent


def _suspending_registry(
    *, config: QueueConfig | None, telemetry: Telemetry | None
) -> tuple[Registry, SuspendingProducer]:
    sync = FakeProducer()
    native = SuspendingProducer(sync)
    registry = Registry(
        config=config,
        backend=Backend(
            mode="sqs",
            producer=sync,
            consumer_factory=lambda **_: None,  # type: ignore[arg-type,return-value]
            async_producer_factory=lambda: native,
        ),
        telemetry=telemetry,
    )
    return registry, native


def test_cancellation_after_the_send_still_returns_the_receipt() -> None:
    """The queued event is shielded: an accepted message is never reported as failed."""
    telemetry = RecordingTelemetry()
    registry, native = _suspending_registry(config=QueueConfig(mode="sqs"), telemetry=telemetry)
    job = registry.job(name="a", queue="q")(lambda: None)
    receipts: list[DispatchReceipt] = []

    async def main() -> None:
        with anyio.CancelScope() as scope:
            native.on_sent = scope.cancel
            receipts.append(await job.dispatch_async())

    anyio.run(main)
    assert [receipt.message_id for receipt in receipts] == ["msg-1"]
    assert [event["type"] for event in telemetry.events] == ["queued"]


def test_cancellation_before_the_send_propagates() -> None:
    registry, native = _suspending_registry(
        config=QueueConfig(mode="sqs"), telemetry=RecordingTelemetry()
    )
    job = registry.job(name="a", queue="q")(lambda: None)

    async def main() -> None:
        with anyio.CancelScope() as scope:
            scope.cancel()
            await job.dispatch_async()
            raise AssertionError("the cancelled dispatch returned")

    anyio.run(main)
    assert native.sync.sent == []


def test_telemetry_lookup_failure_never_fails_a_sent_async_dispatch(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A supplied backend sends, then the lazy telemetry fails to load its configuration."""
    from laravel_cloud_queues import registry as registry_module
    from laravel_cloud_queues.errors import ConfigurationError

    def missing() -> QueueConfig:
        raise ConfigurationError("missing configuration")

    monkeypatch.setattr(registry_module, "load_config", missing)
    registry, native = _suspending_registry(config=None, telemetry=None)
    job = registry.job(name="a", queue="q")(lambda: None)
    receipt = anyio.run(job.dispatch_async)
    assert receipt.message_id == "msg-1"
    assert len(native.sync.sent) == 1
    assert "Could not emit the queued event for job a." in caplog.text


def test_native_send_failure_propagates_without_queued_event() -> None:
    producer = FakeProducer(error=ManagedQueueNotFoundError("missing"))
    registry, _, telemetry = make_registry(producer=producer, native_async=True)
    with pytest.raises(ManagedQueueNotFoundError):
        anyio.run(registry.job(name="a")(lambda: None).dispatch_async)
    assert telemetry.events == []


def test_dispatch_async_without_an_asyncio_loop_runs_in_a_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under trio there is no asyncio loop: the whole dispatch takes one thread hop."""
    registry, producer, _ = make_registry(native_async=True)
    job = registry.job(name="a")(lambda: None)

    async def no_loop() -> None:
        return None

    monkeypatch.setattr(registry, "async_producer", no_loop)
    anyio.run(job.dispatch_async)
    assert producer.threads[0] != threading.get_ident()
    assert producer.opened == []


def test_sync_dispatch_inside_a_running_loop() -> None:
    """Blocks the loop but never starts a nested loop."""
    registry, producer, _ = make_registry()
    job = registry.job(name="a")(lambda: None)

    async def main() -> DispatchReceipt:
        return job.dispatch()

    assert anyio.run(main).message_id == "msg-1"
    assert producer.threads == [threading.get_ident()]


def test_prepare_dispatch_accepts_explicit_options() -> None:
    registry, _, _ = make_registry()
    job = registry.job(name="a", queue="q")(lambda: None)
    prepared = prepare_dispatch(job, (), {}, DispatchOptions(queue="b", delay=2))
    assert prepared.job_name == "a"
    assert (prepared.message.queue, prepared.message.delay_seconds) == ("b", 2)
    assert json.loads(prepared.message.body)["uuid"] == prepared.uuid
