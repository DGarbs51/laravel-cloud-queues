"""Real alarm probe with contended logging/stdout locks; no external services."""

from __future__ import annotations

import io
import logging
import os
import sys
import threading
import time
from collections.abc import Sequence

from laravel_cloud_queues import Registry
from laravel_cloud_queues.config import QueueConfig
from laravel_cloud_queues.jobs.dispatch import prepare_dispatch
from laravel_cloud_queues.observability import NullSink, SocketEventSink, Telemetry
from laravel_cloud_queues.transports import Backend, Delivery
from laravel_cloud_queues.worker import Worker, WorkerOptions


class Consumer:
    supports_renewal = False

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery:
        return self.delivery

    def complete(self, delivery: Delivery) -> None:
        os.write(1, b"completed\n")

    def interrupt(self) -> None:
        pass

    def close(self) -> None:
        pass


def main(case: str) -> None:
    managed = case == "observability"
    telemetry = Telemetry(
        sink=SocketEventSink("/no-such-lcq-socket") if managed else NullSink(),
        emits_cloud_events=managed,
    )
    consumer = Consumer()
    # Dispatch uses the testing recorder for capabilities; this consumer never sends.
    registry = Registry(config=QueueConfig(mode="managed" if managed else "sqs"))
    registry._telemetry = telemetry

    @registry.job(name="locked", timeout=0.15)
    def locked() -> None:
        if case == "buffered_stdout":

            class LockedStream:
                def write(self, text: str) -> None:
                    threading.Event().wait(10)

                def flush(self) -> None:
                    threading.Event().wait(10)

            sys.stdout = LockedStream()  # type: ignore[assignment]
        elif case == "stderr_pipe":
            read_fd, write_fd = os.pipe()
            os.set_blocking(write_fd, False)
            try:
                while True:
                    os.write(write_fd, b"x" * 4096)
            except BlockingIOError:
                pass
            os.dup2(write_fd, 2)
            os.set_blocking(2, True)
            # Keep the read end open without a reader until the alarm exits the process.
            assert read_fd >= 0
        elif case == "stdout":
            telemetry._lock.acquire()
        else:
            logger = logging.getLogger(f"laravel_cloud_queues.{case}")
            handler = logging.StreamHandler(io.StringIO())
            logger.addHandler(handler)
            logger.setLevel(logging.WARNING)
            held = threading.Event()

            def hold() -> None:
                handler.acquire()
                held.set()
                threading.Event().wait()

            threading.Thread(target=hold, daemon=True).start()
            assert held.wait(2)
        time.sleep(10)

    with registry.testing(eager=False):
        body = prepare_dispatch(locked, (), {}, locked.dispatch_options).message.body
    consumer.delivery = Delivery(message_id="m", queue="q", body=body, attempt=1, receipt="r")
    registry._backend = Backend(
        mode="sqs",
        producer=None,
        consumer_factory=lambda **_: consumer,  # type: ignore[arg-type]
    )
    Worker(registry, WorkerOptions(max_jobs=1)).run()


if __name__ == "__main__":
    main(sys.argv[1])
