"""Lease renewal thread (D7): keeps a delivery hidden while a job runs, even when a sync
handler or native code blocks the event loop."""

from __future__ import annotations

import logging
import threading
import time

from ..errors import LeaseLostError
from ..transports import Consumer, Delivery

logger = logging.getLogger("laravel_cloud_queues.worker")

JOIN_TIMEOUT = 30.0
"""Minimum bound on waiting for an in-flight renewal; at least one lease window."""


class Watchdog:
    """Renews ``lease_seconds`` every third of the lease. ``lost`` is set when the consumer
    reports a lost lease, or when renewals keep failing for a whole lease window."""

    def __init__(self, consumer: Consumer, delivery: Delivery, lease_seconds: int) -> None:
        self._consumer = consumer
        self._delivery = delivery
        self._lease = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="lcq-lease-watchdog", daemon=True)
        self.lost = False
        self.renewals = 0
        self._renewed_at = time.monotonic()

    def start(self) -> None:
        self._renewed_at = time.monotonic()
        self._thread.start()

    def signal_stop(self) -> None:
        """Stop without waiting (used from the SIGALRM handler)."""
        self._stop.set()

    def stop(self) -> None:
        """Stop and join. If no renewal landed within the last lease window (native code
        holding the GIL starves this thread), confirm ownership once before the worker
        reports: another worker may have received the message meanwhile."""
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
        self.lost = True
        logger.error("Lost the lease on message %s: %s.", self._delivery.message_id, reason)
