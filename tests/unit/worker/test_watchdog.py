from __future__ import annotations

import threading
import time
from typing import Any, cast

from laravel_cloud_queues.errors import LeaseLostError, TransportError
from laravel_cloud_queues.transports import Delivery
from laravel_cloud_queues.worker._watchdog import Watchdog

DELIVERY = Delivery(message_id="m1", queue="q", body="{}", attempt=1, receipt="r")


class Renewer:
    def __init__(self, *errors: Exception | None) -> None:
        self.errors = list(errors)
        self.calls: list[float] = []
        self.threads: set[str] = set()

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        self.calls.append(time.monotonic())
        self.threads.add(threading.current_thread().name)
        error = self.errors.pop(0) if self.errors else None
        if error is not None:
            raise error


def start(renewer: Renewer, lease: float) -> Watchdog:
    watchdog = Watchdog(cast(Any, renewer), DELIVERY, cast(int, lease))
    watchdog.start()
    return watchdog


def test_renews_every_third_of_the_lease_off_the_main_thread() -> None:
    renewer = Renewer()
    watchdog = start(renewer, 0.3)
    time.sleep(0.55)
    watchdog.stop()
    assert 4 <= len(renewer.calls) <= 6
    assert watchdog.renewals == len(renewer.calls)
    assert threading.main_thread().name not in renewer.threads
    assert not watchdog.lost


def test_stop_ends_renewals() -> None:
    renewer = Renewer()
    watchdog = start(renewer, 0.3)
    watchdog.stop()
    count = len(renewer.calls)
    time.sleep(0.25)
    assert len(renewer.calls) == count


def test_lease_lost_error_is_immediate() -> None:
    renewer = Renewer(LeaseLostError("gone"))
    watchdog = start(renewer, 0.15)
    time.sleep(0.2)
    assert watchdog.lost
    watchdog.stop()
    assert len(renewer.calls) == 1


def test_three_consecutive_failures_lose_the_lease() -> None:
    renewer = Renewer(TransportError("x"), TransportError("x"), None, TransportError("x"))
    watchdog = start(renewer, 0.15)
    time.sleep(0.4)
    assert not watchdog.lost  # a success reset the count
    renewer.errors = [TransportError("x")] * 3
    time.sleep(0.25)
    watchdog.stop()
    assert watchdog.lost
