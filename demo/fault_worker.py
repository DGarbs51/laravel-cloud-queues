"""Conformance-only lost-response injection after an actual SQS acknowledgement."""

from laravel_cloud_queues.errors import AmbiguousAcknowledgementError
from laravel_cloud_queues.transports.base import Delivery
from laravel_cloud_queues.transports.sqs import SqsConsumer
from laravel_cloud_queues.worker import Worker, WorkerOptions, resolve_target

complete = SqsConsumer.complete


def applied_then_lost(self: SqsConsumer, delivery: Delivery) -> None:
    complete(self, delivery)
    raise AmbiguousAcknowledgementError("Synthetic response lost after DeleteMessage")


def main() -> int:
    SqsConsumer.complete = applied_then_lost  # type: ignore[method-assign]
    return Worker(resolve_target("demo.app:app"), WorkerOptions(max_jobs=2)).run()


if __name__ == "__main__":
    raise SystemExit(main())
