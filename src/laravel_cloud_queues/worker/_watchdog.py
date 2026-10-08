"""The lease watchdogs that keep a delivery hidden while its job runs.

:class:`Watchdog` renews on a separate thread, so the lease is kept even when a synchronous
handler or native code blocks the event loop. :class:`AsyncWatchdog` renews from a task on
the loop, which only runs while an ``async def`` handler awaits.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time

from ..errors import LeaseLostError
from ..transports import AsyncConsumer, Consumer, Delivery

logger: logging.Logger = logging.getLogger("laravel_cloud_queues.worker")
"""The logger used by the worker."""

JOIN_TIMEOUT = 30.0
"""The minimum number of seconds to wait for an in-flight renewal when stopping.

The watchdog always waits for at least one full lease window.
"""


class _Lease:
    """The renewal bookkeeping shared by both watchdogs.

    The lease is renewed for ``lease_seconds`` every third of the lease. The ``lost`` flag
    is set when the consumer reports a lost lease, or when renewals keep failing for a
    whole lease window.
    """

    def __init__(self, delivery: Delivery, lease_seconds: int) -> None:
        """Create a new lease instance."""
        self._delivery = delivery
        self._lease = lease_seconds
        self._failures = 0
        self.lost = False
        """Indicates if the worker may no longer own the delivery."""
        self.renewals = 0
        """The number of successful lease renewals."""
        self._renewed_at = time.monotonic()

    def _renewed(self) -> None:
        """Record a successful renewal."""
        self._failures = 0
        self.renewals += 1
        self._renewed_at = time.monotonic()

    def _failed(self, exc: Exception, *, stopping: bool) -> bool:
        """Record a failed renewal and determine if renewing must end."""
        if isinstance(exc, LeaseLostError):
            self._lose("the message is no longer owned by this worker")
            return True
        if stopping:
            self._lose(f"renewal failed during shutdown ({type(exc).__name__})")
            return True
        self._failures += 1
        # Three failed renewals span a whole lease window: the message may be visible.
        if self._failures >= 3:
            self._lose(f"renewal failed {self._failures} times ({type(exc).__name__})")
            return True
        logger.warning(
            "Lease renewal failed for message %s (%s); retrying.",
            self._delivery.message_id,
            type(exc).__name__,
        )
        return False

    def _lapsed(self) -> bool:
        """Determine if ownership must be confirmed because no renewal landed in a window."""
        return not self.lost and time.monotonic() - self._renewed_at >= self._lease

    def _lose(self, reason: str) -> None:
        """Mark the lease as lost and log the given reason."""
        self.lost = True
        logger.error("Lost the lease on message %s: %s.", self._delivery.message_id, reason)


class Watchdog(_Lease):
    """A background thread that renews a delivery's lease while a sync handler runs."""

    def __init__(self, consumer: Consumer, delivery: Delivery, lease_seconds: int) -> None:
        """Create a new watchdog instance."""
        super().__init__(delivery, lease_seconds)
        self._consumer = consumer
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="lcq-lease-watchdog", daemon=True)

    def start(self) -> None:
        """Start renewing the lease in the background."""
        self._renewed_at = time.monotonic()
        self._thread.start()

    def signal_stop(self) -> None:
        """Signal the watchdog to stop without waiting for it.

        This is safe to call from the ``SIGALRM`` handler.
        """
        self._stop.set()

    def stop(self) -> None:
        """Stop the watchdog and wait for its thread to finish.

        If no renewal landed within the last lease window, such as when native code holding
        the GIL starved the thread, ownership is confirmed once more before the worker
        reports, since another worker may have received the message in the meantime.
        """
        self._stop.set()
        self._thread.join(max(self._lease, JOIN_TIMEOUT))
        if self._thread.is_alive():
            self._lose("renewal is still in flight after the shutdown deadline")
            return
        if not self._lapsed():
            return
        try:
            self._consumer.renew(self._delivery, self._lease)
        except Exception as exc:
            self._lose(f"the lease lapsed while the job ran ({type(exc).__name__})")

    def _run(self) -> None:
        """Renew the lease on an interval until the watchdog is stopped or the lease is lost."""
        while not self._stop.wait(self._lease / 3):
            try:
                self._consumer.renew(self._delivery, self._lease)
            except Exception as exc:
                if self._failed(exc, stopping=self._stop.is_set()):
                    return
            else:
                self._renewed()


class AsyncWatchdog(_Lease):
    """An event loop task that renews a delivery's lease while an async handler runs.

    It has the same semantics as :class:`Watchdog`, but renews only while the handler
    awaits; a handler that blocks the loop is caught by the ownership check in :meth:`stop`.
    """

    def __init__(self, consumer: AsyncConsumer, delivery: Delivery, lease_seconds: int) -> None:
        """Create a new async watchdog instance."""
        super().__init__(delivery, lease_seconds)
        self._consumer = consumer
        self._stop = asyncio.Event()
        # A plain flag: the SIGALRM handler must not touch loop objects.
        self._signalled = False
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start renewing the lease from a task on the running loop."""
        self._renewed_at = time.monotonic()
        self._task = asyncio.get_running_loop().create_task(self._run())

    def signal_stop(self) -> None:
        """Signal the watchdog to stop without waiting for it.

        This is safe to call from the ``SIGALRM`` handler.
        """
        self._signalled = True

    async def stop(self) -> None:
        """Stop the task, wait for an in-flight renewal, and confirm ownership if it lapsed."""
        self._stop.set()
        task = self._task
        if task is not None:
            done, _ = await asyncio.wait({task}, timeout=max(self._lease, JOIN_TIMEOUT))
            if not done:
                task.cancel()
                self._lose("renewal is still in flight after the shutdown deadline")
                return
        if not self._lapsed():
            return
        try:
            await self._consumer.renew(self._delivery, self._lease)
        except Exception as exc:
            self._lose(f"the lease lapsed while the job ran ({type(exc).__name__})")

    def _stopping(self) -> bool:
        """Determine if the watchdog has been asked to stop."""
        return self._stop.is_set() or self._signalled

    async def _run(self) -> None:
        """Renew the lease on an interval until the watchdog is stopped or the lease is lost."""
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self._lease / 3)
            if self._stopping():
                return
            try:
                await self._consumer.renew(self._delivery, self._lease)
            except Exception as exc:
                if self._failed(exc, stopping=self._stopping()):
                    return
            else:
                self._renewed()
