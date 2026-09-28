"""The lease watchdog that keeps a delivery hidden while its job runs.

Renewals happen on a separate thread, so the lease is kept even when a synchronous
handler or native code blocks the event loop.
"""

from __future__ import annotations

import logging
import threading
import time

from ..errors import LeaseLostError
from ..transports import Consumer, Delivery

logger = logging.getLogger("laravel_cloud_queues.worker")
"""The logger used by the worker."""

JOIN_TIMEOUT = 30.0
"""The minimum number of seconds to wait for an in-flight renewal when stopping.

The watchdog always waits for at least one full lease window.
"""


class Watchdog:
    """A background thread that renews a delivery's lease while its job runs.

    The lease is renewed for ``lease_seconds`` every third of the lease. The ``lost`` flag
    is set when the consumer reports a lost lease, or when renewals keep failing for a
    whole lease window.
    """

    def __init__(self, consumer: Consumer, delivery: Delivery, lease_seconds: int) -> None:
        """Create a new watchdog instance."""
        self._consumer = consumer
        self._delivery = delivery
        self._lease = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="lcq-lease-watchdog", daemon=True)
        self.lost = False
        """Indicates if the worker may no longer own the delivery."""
        self.renewals = 0
        """The number of successful lease renewals."""
        self._renewed_at = time.monotonic()

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
        if self.lost or time.monotonic() - self._renewed_at < self._lease:
            return
        try:
            self._consumer.renew(self._delivery, self._lease)
        except Exception as exc:
            self._lose(f"the lease lapsed while the job ran ({type(exc).__name__})")

    def _run(self) -> None:
        """Renew the lease on an interval until the watchdog is stopped or the lease is lost."""
        interval = self._lease / 3
        failures = 0
        while not self._stop.wait(interval):
            try:
                self._consumer.renew(self._delivery, self._lease)
            except LeaseLostError:
                self._lose("the message is no longer owned by this worker")
                return
            except Exception as exc:
                if self._stop.is_set():
                    self._lose(f"renewal failed during shutdown ({type(exc).__name__})")
                    return
                failures += 1
                # Three failed renewals span a whole lease window: the message may be visible.
                if failures >= 3:
                    self._lose(f"renewal failed {failures} times ({type(exc).__name__})")
                    return
                logger.warning(
                    "Lease renewal failed for message %s (%s); retrying.",
                    self._delivery.message_id,
                    type(exc).__name__,
                )
            else:
                failures = 0
                self.renewals += 1
                self._renewed_at = time.monotonic()

    def _lose(self, reason: str) -> None:
        """Mark the lease as lost and log the given reason."""
        self.lost = True
        logger.error("Lost the lease on message %s: %s.", self._delivery.message_id, reason)
