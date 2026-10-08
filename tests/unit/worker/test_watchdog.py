from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any, cast

import anyio
import pytest

from laravel_cloud_queues.errors import LeaseLostError, TransportError
from laravel_cloud_queues.transports import Delivery
from laravel_cloud_queues.worker._watchdog import AsyncWatchdog, Watchdog

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


def test_starved_watchdog_confirms_ownership_before_reporting() -> None:
    """FINDINGS.md: a GIL-holding call starves renewal; the lease may have lapsed."""
    renewer = Renewer(LeaseLostError("re-received elsewhere"))
    watchdog = Watchdog(cast(Any, renewer), DELIVERY, cast(int, 0.2))
    watchdog.start()
    watchdog._stop.set()  # the thread never renews, as if starved
    time.sleep(0.25)
    watchdog.stop()
    assert watchdog.lost
    assert len(renewer.calls) == 1


def test_recent_renewal_skips_the_final_check() -> None:
    renewer = Renewer()
    watchdog = start(renewer, 30)
    watchdog.stop()
    assert renewer.calls == []
    assert not watchdog.lost


def test_stop_marks_an_in_flight_renewal_lost(monkeypatch: Any) -> None:
    from laravel_cloud_queues.worker import _watchdog

    monkeypatch.setattr(_watchdog, "JOIN_TIMEOUT", 0.01)
    entered, finish = threading.Event(), threading.Event()

    class SlowRenewer(Renewer):
        def renew(self, delivery: Delivery, lease_seconds: int) -> None:
            entered.set()
            assert finish.wait(5)

    watchdog = start(SlowRenewer(), 0.15)
    try:
        assert entered.wait(2)
        watchdog.stop()
        assert watchdog.lost
    finally:
        finish.set()
        watchdog._thread.join(2)


def test_renewal_failure_during_stop_is_lost() -> None:
    entered = threading.Event()

    class FailingRenewer(Renewer):
        def renew(self, delivery: Delivery, lease_seconds: int) -> None:
            entered.set()
            assert watchdog._stop.wait(2)
            raise TransportError("renewal failed during shutdown")

    watchdog = start(FailingRenewer(), 0.3)
    assert entered.wait(2)
    watchdog.stop()
    assert watchdog.lost


def test_stop_joins_for_at_least_thirty_seconds_or_the_lease() -> None:
    from unittest.mock import Mock

    for lease in (1, 60):
        watchdog = Watchdog(cast(Any, Renewer()), DELIVERY, lease)
        thread = Mock()
        thread.is_alive.return_value = False
        watchdog._thread = thread
        watchdog.stop()
        thread.join.assert_called_once_with(max(lease, 30))


# --- AsyncWatchdog (async def handlers) ----------------------------------------------------


class AsyncRenewer(Renewer):
    def __init__(self, *errors: Exception | None, delay: float = 0.0) -> None:
        super().__init__(*errors)
        self.delay = delay

    async def renew(self, delivery: Delivery, lease_seconds: int) -> None:  # type: ignore[override]
        await asyncio.sleep(self.delay)
        super().renew(delivery, lease_seconds)


def run_async(renewer: AsyncRenewer, lease: float, body: Callable[[AsyncWatchdog], Any]) -> Any:
    async def main() -> AsyncWatchdog:
        watchdog = AsyncWatchdog(cast(Any, renewer), DELIVERY, cast(int, lease))
        watchdog.start()
        await body(watchdog)
        return watchdog

    return anyio.run(main, backend="asyncio")


def test_async_watchdog_renews_on_the_loop_while_the_handler_awaits() -> None:
    renewer = AsyncRenewer()

    async def body(watchdog: AsyncWatchdog) -> None:
        await asyncio.sleep(0.55)
        await watchdog.stop()
        count = len(renewer.calls)
        await asyncio.sleep(0.25)
        assert len(renewer.calls) == count

    watchdog = run_async(renewer, 0.3, body)
    assert 4 <= len(renewer.calls) <= 6
    assert watchdog.renewals == len(renewer.calls)
    assert renewer.threads == {threading.main_thread().name}
    assert not watchdog.lost


def test_async_watchdog_lease_lost_error_is_immediate() -> None:
    renewer = AsyncRenewer(LeaseLostError("gone"))

    async def body(watchdog: AsyncWatchdog) -> None:
        await asyncio.sleep(0.2)
        assert watchdog.lost
        await watchdog.stop()

    run_async(renewer, 0.15, body)
    assert len(renewer.calls) == 1


def test_async_watchdog_three_consecutive_failures_lose_the_lease() -> None:
    renewer = AsyncRenewer(TransportError("x"), TransportError("x"), None, TransportError("x"))

    async def body(watchdog: AsyncWatchdog) -> None:
        await asyncio.sleep(0.4)
        assert not watchdog.lost  # a success reset the count
        renewer.errors = [TransportError("x")] * 3
        await asyncio.sleep(0.25)
        await watchdog.stop()

    assert run_async(renewer, 0.15, body).lost


@pytest.mark.parametrize(("error", "lost"), [(LeaseLostError("elsewhere"), True), (None, False)])
def test_async_watchdog_confirms_ownership_after_a_blocked_loop(
    error: Exception | None, lost: bool
) -> None:
    """An async handler that blocks the loop starves the task; stop renews once to check."""
    renewer = AsyncRenewer(error)

    async def body(watchdog: AsyncWatchdog) -> None:
        time.sleep(0.25)  # noqa: ASYNC251
        watchdog.signal_stop()  # the task wakes up stopped and never renews
        await asyncio.sleep(0)
        await watchdog.stop()

    assert run_async(renewer, 0.2, body).lost is lost
    assert len(renewer.calls) == 1


def test_async_watchdog_recent_renewal_skips_the_final_check() -> None:
    renewer = AsyncRenewer()

    async def body(watchdog: AsyncWatchdog) -> None:
        await watchdog.stop()

    assert not run_async(renewer, 30, body).lost
    assert renewer.calls == []


def test_async_watchdog_signal_stop_prevents_renewal() -> None:
    renewer = AsyncRenewer()

    async def body(watchdog: AsyncWatchdog) -> None:
        watchdog.signal_stop()
        await asyncio.sleep(0.2)

    run_async(renewer, 0.15, body)
    assert renewer.calls == []


def test_async_watchdog_stop_cancels_an_in_flight_renewal_past_the_deadline(
    monkeypatch: Any,
) -> None:
    from laravel_cloud_queues.worker import _watchdog

    monkeypatch.setattr(_watchdog, "JOIN_TIMEOUT", 0.01)
    renewer = AsyncRenewer(delay=5)

    async def body(watchdog: AsyncWatchdog) -> None:
        await asyncio.sleep(0.1)  # the first renewal starts at 0.05 s and hangs
        await watchdog.stop()
        assert watchdog._task is not None
        await asyncio.sleep(0)
        assert watchdog._task.cancelled()

    assert run_async(renewer, cast(float, 0.15), body).lost


def test_async_watchdog_renewal_failure_during_stop_is_lost() -> None:
    renewer = AsyncRenewer(TransportError("renewal failed during shutdown"), delay=0.1)

    async def body(watchdog: AsyncWatchdog) -> None:
        await asyncio.sleep(0.07)  # the renewal is in flight
        await watchdog.stop()

    assert run_async(renewer, 0.15, body).lost


def test_async_watchdog_stop_without_start_only_checks_the_lease() -> None:
    renewer = AsyncRenewer()

    async def main() -> bool:
        watchdog = AsyncWatchdog(cast(Any, renewer), DELIVERY, 30)
        await watchdog.stop()
        return watchdog.lost

    assert anyio.run(main) is False
    assert renewer.calls == []
