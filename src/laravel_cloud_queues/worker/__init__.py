"""The queue worker that pops jobs off of the queue and runs them.

Each delivery moves through a single state machine with exactly one owner of its outcome::

    received -> [decode: defect => terminal] -> [pre-run attempt check] -> running
    -> outcome chosen (success | release | fail | error -> retry/terminal | timeout)
    -> reporting (transport complete/release) -> completed | ambiguous (stop)

The full contract lives in ``docs/contract/worker.md``.

When the worker is idle, ``SIGTERM`` and ``SIGINT`` call ``consumer.interrupt()``, so agent
and Redis receives return promptly. A single-queue SQS long poll (``WaitTimeSeconds=20``) is
not aborted, because an aborted ReceiveMessage may still dequeue a message and burn an
attempt. The worker instead waits for the current poll (at most about 20 seconds), runs any
message it hands over, and then stops.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal
import threading
import time
import types
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import anyio
import anyio.to_thread
from laravel_cloud_logging import configure

from ..config import QueueConfig
from ..errors import (
    AgentProtocolError,
    AgentUnavailableError,
    ConfigurationError,
    DispatchError,
    FatalWorkerError,
    JobDefectError,
    JobFailedError,
    JobTimeoutError,
    MaxAttemptsExceededError,
    TransportError,
)
from ..jobs.context import JobContext
from ..jobs.execution import HandlerResult, prepare_execution, run_prepared
from ..jobs.policy import ResolvedPolicy, WorkerDefaults
from ..observability import Telemetry, failed_job_event, failure_log_record, lifecycle_event
from ..observability._guard import raw_diagnostic, signal_safe
from ..registry import Registry, WorkerTarget
from ..transports import Consumer, Delivery
from ._watchdog import Watchdog

EXIT_OK = 0
"""The exit code used when the worker stops normally."""
EXIT_FATAL = 1
"""The exit code used when the worker stops because of a fatal error."""
EXIT_CONFIG = 2
"""The exit code used when the worker stops because of a configuration error."""
EXIT_TIMEOUT = 124
"""The exit code used when a job exceeds its timeout."""

SQS_WAIT_SECONDS = 20.0
"""The number of seconds a single-queue SQS receive long-polls for a message."""
TRANSIENT_RETRY_SECONDS = 1.0
"""The number of seconds to wait before retrying after a transient receive error."""
ALARM_LOCK_TIMEOUT = 0.5
"""The number of seconds the ``SIGALRM`` handler waits for an output lock.

The wait is bounded because the handler may have interrupted a write holding the lock.
"""

logger: logging.Logger = logging.getLogger("laravel_cloud_queues.worker")
"""The logger used by the worker."""

Status = Literal["processed", "released", "failed"]
"""The status recorded for a job once its outcome has been reported."""
_STATUS: dict[str, Status] = {"complete": "processed", "release": "released", "fail": "failed"}
"""The recorded status for each kind of outcome."""


@dataclass(frozen=True)
class WorkerOptions:
    """The options that control how the worker runs and when it stops."""

    queues: tuple[str, ...] | None = None
    """The queues to process, in priority order.

    When ``None``, the backend's default queue or the Laravel Cloud assignment is used.
    """
    max_jobs: int | None = None
    """The number of jobs to process before stopping."""
    max_time: float | None = None
    """The number of seconds the worker may run before stopping."""
    stop_when_empty: bool = False
    """Indicates if the worker should stop once the queue is empty."""
    stop_when_empty_for: float | None = None
    """The number of seconds the queue may stay empty before the worker stops."""
    timeout: float = 60.0
    """The default number of seconds a job may run when the message omits a timeout.

    A value of zero disables the timeout.
    """
    sleep: float = 3.0
    """The number of seconds to sleep when no job is available."""
    rest: float = 0.0
    """The number of seconds to rest between jobs."""
    lease_seconds: int = 60
    """The number of seconds in the visibility or reservation lease.

    The watchdog renews the lease every third of this window while a job runs.
    """


@dataclass(frozen=True)
class _Runtime:
    """The runtime state resolved when the worker starts.

    Configuration errors surface while this is resolved, and the worker exits with status 2.
    """

    registry: Registry
    """The registry of jobs the worker can run."""
    config: QueueConfig
    """The queue configuration."""
    telemetry: Telemetry
    """The telemetry sink for lifecycle events and log lines."""
    consumer: Consumer
    """The consumer that receives and acknowledges messages."""
    queues: tuple[str, ...]
    """The queues to process, in priority order."""
    wait: float
    """The number of seconds each ``receive`` call waits for a message."""
    sleep_when_empty: bool
    """Indicates if the worker should sleep after an empty poll.

    This is only set when the receive did not already wait for a message.
    """
    defaults: WorkerDefaults
    """The worker-level defaults applied to each job's policy."""
    wake: anyio.Event
    """The event set by a stop signal, which ends a pause early."""


@dataclass(frozen=True)
class _Outcome:
    """The single outcome chosen for a delivery."""

    kind: Literal["complete", "release", "fail"]
    """The kind of outcome to report to the transport."""
    delay: int = 0
    """The number of seconds to wait before a released job becomes available again."""
    exception: BaseException | None = None
    """The exception that caused the job to fail, if any."""


@dataclass(frozen=True)
class _Running:
    """The running delivery that the ``SIGALRM`` handler acts on."""

    delivery: Delivery
    """The delivery being processed."""
    policy: ResolvedPolicy
    """The resolved policy for the running job."""
    job_name: str
    """The name of the running job."""
    started_at: datetime
    """The time the delivery started processing."""
    watchdog: Watchdog | None
    """The watchdog renewing the delivery's lease, if the consumer supports renewal."""


class Worker:
    """A worker that processes jobs from the queue until a stop condition is reached."""

    def __init__(self, target: WorkerTarget, options: WorkerOptions | None = None) -> None:
        """Create a new worker instance."""
        self._target = target
        self._options = options or WorkerOptions()
        self._clock = time.monotonic
        self._runtime: _Runtime | None = None
        self._running: _Running | None = None
        self._stopping = threading.Event()

    def run(self) -> int:
        """Run the worker until a stop condition is reached and get the exit code.

        The exit code is 0, 1 or 2; a job timeout exits the process with 124 directly
        from the timeout handler. The worker must run on the main thread. When the root
        logger has no handlers yet, this calls ``laravel_cloud_logging.configure()``; an app
        that sets up its own logging when imported keeps that configuration.
        """
        if not logging.getLogger().handlers:
            configure()
        return anyio.run(self._main, backend="asyncio")

    # --- process -------------------------------------------------------------------------

    async def _main(self) -> int:
        """Start the worker, run its loop within the target's lifespan, and clean up."""
        try:
            runtime = self._start()
        except ConfigurationError as exc:
            logger.error("Configuration error: %s", exc)
            return EXIT_CONFIG
        self._runtime = runtime
        previous = signal.signal(signal.SIGALRM, self._on_alarm)
        try:
            self._watch_signals(runtime)
            async with self._target.lifespan():
                code = await self._loop(runtime)
        except ConfigurationError as exc:
            logger.error("Configuration error: %s", exc)
            return EXIT_CONFIG
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            loop = asyncio.get_running_loop()
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
            try:
                runtime.consumer.close()
            except Exception as exc:
                logger.warning("Closing the consumer failed (%s).", type(exc).__name__)
        logger.info("Worker stopped (exit %d).", code)
        return code

    def _start(self) -> _Runtime:
        """Resolve the runtime state the worker needs to start processing jobs."""
        registry = self._target.registry
        registry.load()
        config = registry.config
        queues = self._select_queues(config)
        if config.mode == "redis":
            wait, sleep_when_empty = self._options.sleep, False
        elif not config.uses_agent and len(queues) == 1:
            wait, sleep_when_empty = SQS_WAIT_SECONDS, False
        else:
            # Several SQS queues: short polls in priority order. Agent: the agent long-polls
            # ``GET /next`` itself; Laravel still sleeps ``--sleep`` after an empty pop.
            wait, sleep_when_empty = 0.0, True
        telemetry = registry.telemetry
        consumer = registry.backend.consumer_factory(lease_seconds=self._options.lease_seconds)
        logger.info(
            "Worker started (mode %s%s, queues %s).",
            config.mode,
            ", agent" if config.uses_agent else "",
            ",".join(queues),
        )
        return _Runtime(
            registry=registry,
            config=config,
            telemetry=telemetry,
            consumer=consumer,
            queues=queues,
            wait=wait,
            sleep_when_empty=sleep_when_empty,
            defaults=WorkerDefaults(timeout=self._options.timeout),
            wake=anyio.Event(),
        )

    def _select_queues(self, config: QueueConfig) -> tuple[str, ...]:
        """Determine which queues the worker should process.

        Raises a :class:`ConfigurationError` if the requested queues conflict with the queue
        Laravel Cloud assigns to the worker.
        """
        requested = self._options.queues
        if config.managed is not None:
            assigned = config.managed.queue
            if not config.uses_agent:
                return requested or (assigned,)
            if requested and requested != (assigned,):
                raise ConfigurationError(
                    f"Laravel Cloud assigns queue [{assigned}] to this worker; "
                    f"--queue {','.join(requested)} conflicts with it. Remove --queue."
                )
            return (assigned,)
        return requested or (config.default_queue,)

    def _watch_signals(self, runtime: _Runtime) -> None:
        """Register the handlers that stop the worker on ``SIGTERM`` and ``SIGINT``."""

        # Loop signal handlers (the asyncio backend is fixed): a signal that arrives while a
        # sync handler blocks the loop is handled once it returns, so the current job still
        # completes and reports. Repeated signals only repeat the request.
        def stop(signum: signal.Signals) -> None:
            """Handle a stop signal by asking the worker to stop after the current job."""
            logger.info("Received %s; stopping after the current job.", signum.name)
            self._stopping.set()
            runtime.wake.set()
            runtime.consumer.interrupt()

        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop, signum)

    async def _pause(self, runtime: _Runtime, seconds: float) -> None:
        """Sleep for the given number of seconds, waking early on a stop signal."""
        if seconds <= 0 or self._stopping.is_set():
            return
        with anyio.move_on_after(seconds):
            await runtime.wake.wait()

    # --- loop ----------------------------------------------------------------------------

    async def _loop(self, runtime: _Runtime) -> int:
        """Process deliveries until a stop condition is reached and get the exit code."""
        options = self._options
        started = self._clock()
        last_job: float | None = None
        jobs = 0
        while not self._stopping.is_set():
            pause = options.sleep if runtime.sleep_when_empty else 0.0
            receive_failed = False
            try:
                delivery = await anyio.to_thread.run_sync(
                    runtime.consumer.receive, runtime.queues, runtime.wait
                )
            except FatalWorkerError as exc:
                self._log_fatal("receiving", exc)
                return exc.exit_code
            except DispatchError as exc:
                # e.g. the queue does not exist: a configuration problem, not a transient one.
                logger.error("Configuration error: %s", exc)
                return EXIT_CONFIG
            except TransportError as exc:
                logger.warning("Receive failed (%s: %s); retrying.", type(exc).__name__, exc)
                delivery, pause = None, TRANSIENT_RETRY_SECONDS
                receive_failed = True

            if delivery is not None:
                jobs += 1
                code = await self._deliver(runtime, delivery)
                last_job = self._clock()
                if code is not None:
                    return code
                pause = options.rest

            now = self._clock()
            reason = None
            if self._stopping.is_set():
                reason = "signal"
            elif not receive_failed and delivery is None and options.stop_when_empty:
                reason = "queue empty"
            elif (
                delivery is None
                and not receive_failed
                and options.stop_when_empty_for is not None
                and now - (last_job if last_job is not None else started)
                >= options.stop_when_empty_for
            ):
                reason = "queue empty for the configured time"
            elif options.max_time is not None and now - started >= options.max_time:
                reason = "max time reached"
            elif options.max_jobs is not None and jobs >= options.max_jobs:
                reason = "max jobs reached"
            if reason is not None:
                logger.info("Worker stopping: %s.", reason)
                return EXIT_OK
            await self._pause(runtime, pause)
        logger.info("Worker stopping: signal.")
        return EXIT_OK

    # --- one delivery --------------------------------------------------------------------

    async def _deliver(self, runtime: _Runtime, delivery: Delivery) -> int | None:
        """Process a single delivery and get an exit code if the worker must stop."""
        started_at = _utcnow()
        runtime.telemetry.emit(lifecycle_event("started", delivery.queue, timestamp=started_at))
        try:
            prepared = prepare_execution(runtime.registry, delivery.body)
        except JobDefectError as exc:
            logger.error(
                "Message %s on [%s] is not a runnable job (%s: %s); failing it.",
                delivery.message_id,
                delivery.queue,
                type(exc).__name__,
                exc,
            )
            return await self._report(
                runtime, delivery, _Outcome("fail", exception=exc), started_at, None
            )

        job_name = prepared.envelope.job
        policy = prepared.policy(runtime.defaults)
        if policy.exceeded_before_run(delivery.attempt):
            error = MaxAttemptsExceededError(f"{job_name} has been attempted too many times.")
            outcome = _Outcome("fail", exception=error)
            return await self._report(runtime, delivery, outcome, started_at, job_name)

        context = JobContext(
            job_name=job_name,
            uuid=prepared.envelope.uuid,
            message_id=delivery.message_id,
            queue=delivery.queue,
            attempt=delivery.attempt,
            max_tries=policy.tries,
        )
        watchdog = None
        if runtime.consumer.supports_renewal:
            watchdog = Watchdog(runtime.consumer, delivery, self._options.lease_seconds)
            watchdog.start()
        self._running = _Running(delivery, policy, job_name, started_at, watchdog)
        if policy.timeout > 0:
            signal.setitimer(signal.ITIMER_REAL, policy.timeout)
        try:
            result = await run_prepared(prepared, context)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            self._running = None
            if watchdog is not None:
                watchdog.stop()

        if watchdog is not None and watchdog.lost:
            logger.error(
                "Not reporting an outcome for message %s: another worker may own it. Stopping.",
                delivery.message_id,
            )
            return EXIT_FATAL
        outcome = _choose(result, policy, delivery.attempt, job_name)
        return await self._report(runtime, delivery, outcome, started_at, job_name)

    async def _report(
        self,
        runtime: _Runtime,
        delivery: Delivery,
        outcome: _Outcome,
        started_at: datetime,
        job_name: str | None,
    ) -> int | None:
        """Report the outcome to the transport and write the completion records.

        Acknowledgement failures are never handler errors and never lead to a second
        outcome. Terminal failures log their failure record first, so it is not lost if the
        delete succeeds and the process dies.
        """
        code: int | None = None
        if outcome.kind == "fail" and outcome.exception is not None:
            self._log_failure(runtime, delivery, outcome.exception, started_at)
        try:
            if outcome.kind == "release":
                await anyio.to_thread.run_sync(runtime.consumer.release, delivery, outcome.delay)
            else:
                await anyio.to_thread.run_sync(runtime.consumer.complete, delivery)
        except AgentProtocolError as exc:
            logger.error(
                "The agent rejected the outcome for message %s (%s); not reporting it again.",
                delivery.message_id,
                exc,
            )
        except AgentUnavailableError as exc:
            self._log_fatal("reporting", exc)
            code = exc.exit_code
        except FatalWorkerError as exc:
            self._log_fatal("reporting", exc)
            return exc.exit_code
        except TransportError as exc:
            logger.error(
                "Acknowledgement of message %s is ambiguous (%s: %s); stopping.",
                delivery.message_id,
                type(exc).__name__,
                exc,
            )
            return EXIT_FATAL
        self._record(
            runtime, delivery, _STATUS[outcome.kind], outcome.exception, started_at, job_name
        )
        return code

    def _record(
        self,
        runtime: _Runtime,
        delivery: Delivery,
        status: Status,
        exception: BaseException | None,
        started_at: datetime,
        job_name: str | None,
        *,
        lock_timeout: float | None = None,
    ) -> None:
        """Write the completion records for a reported delivery.

        Managed terminal failures emit a ``failed_job`` event first, with the same timestamp
        as the ``failed`` lifecycle event, followed by the lifecycle event and the job log line.
        """
        now = _utcnow()
        duration = max(0, int((now - started_at) / timedelta(milliseconds=1)))
        telemetry = runtime.telemetry
        if status == "failed" and exception is not None and runtime.config.emits_cloud_events:
            event = failed_job_event(
                queue=delivery.queue,
                payload=delivery.body,
                exception=exception,
                attempts=delivery.attempt,
                started_at=started_at,
                timestamp=now,
            )
            telemetry.emit(event, lock_timeout=lock_timeout)
        telemetry.emit(
            lifecycle_event(status, delivery.queue, timestamp=now, duration_ms=duration),
            lock_timeout=lock_timeout,
        )
        telemetry.log_line(
            {
                "laravel_cloud_queues": "job",
                "status": status,
                "job": job_name,
                "queue": delivery.queue,
                "message_id": delivery.message_id,
                "attempt": delivery.attempt,
                "duration_ms": duration,
            },
            message=f"{job_name or 'Unknown job'} {status}.",
            level=logging.ERROR if status == "failed" else logging.INFO,
            lock_timeout=lock_timeout,
        )

    def _log_failure(
        self,
        runtime: _Runtime,
        delivery: Delivery,
        exception: BaseException,
        started_at: datetime,
        *,
        lock_timeout: float | None = None,
    ) -> None:
        """Log the failure record for a terminal failure, before ``complete``, in every mode."""
        record = failure_log_record(
            queue=delivery.queue,
            payload=delivery.body,
            exception=exception,
            attempts=delivery.attempt,
            message_id=delivery.message_id,
            started_at=started_at,
            timestamp=_utcnow(),
        )
        runtime.telemetry.log_line(
            record,
            message=f"Job failed on {delivery.queue}.",
            level=logging.ERROR,
            exception=exception,
            lock_timeout=lock_timeout,
        )

    # --- timeout (SIGALRM, D2) -----------------------------------------------------------

    def _on_alarm(self, signum: int, frame: types.FrameType | None) -> None:
        """Handle the ``SIGALRM`` fired when a job exceeds its timeout, then exit with 124."""
        running, runtime = self._running, self._runtime
        if running is None or runtime is None:
            return  # The timer fired as the job finished; it has been disarmed.
        try:
            with signal_safe():
                self._timed_out(runtime, running, frame)
        finally:
            os._exit(EXIT_TIMEOUT)

    def _timed_out(
        self, runtime: _Runtime, running: _Running, frame: types.FrameType | None
    ) -> None:
        """Handle a job that has exceeded its timeout.

        A terminal timeout writes the failure record, completes the message, and records the
        failure; every timeout then emits its lifecycle event. The message is never released
        and there is no backoff, since the visibility or reservation expiry redelivers it.
        """
        if running.watchdog is not None:
            running.watchdog.signal_stop()
            if running.watchdog.lost:
                raw_diagnostic(
                    f"Not reporting timed-out message {running.delivery.message_id}: lease lost."
                )
                return
        delivery, policy = running.delivery, running.policy
        terminal = policy.fail_on_timeout or policy.is_last_attempt(delivery.attempt)
        raw_diagnostic(
            f"Job {running.job_name} (message {delivery.message_id}, attempt {delivery.attempt}) "
            f"exceeded its {policy.timeout:g} s timeout; "
            + ("failing it." if terminal else "it will be retried.")
        )
        exception = None
        if terminal:
            exception = JobTimeoutError(f"{running.job_name} has timed out.")
            exception.__traceback__ = _traceback(frame)
            self._log_failure(
                runtime, delivery, exception, running.started_at, lock_timeout=ALARM_LOCK_TIMEOUT
            )
            try:
                runtime.consumer.complete(delivery)
            except Exception as exc:
                raw_diagnostic(
                    f"Could not complete timed-out message {delivery.message_id} "
                    f"({type(exc).__name__})."
                )
        self._record(
            runtime,
            delivery,
            "failed" if terminal else "released",
            exception,
            running.started_at,
            running.job_name,
            lock_timeout=ALARM_LOCK_TIMEOUT,
        )

    def _log_fatal(self, stage: str, exc: FatalWorkerError) -> None:
        """Log the fatal error that is stopping the worker."""
        if isinstance(exc, AgentUnavailableError):
            logger.error(
                "Lost the Laravel Cloud agent while %s (%s); stopping. Unacknowledged messages "
                "return through visibility.",
                stage,
                exc,
            )
        else:
            logger.error(
                "Fatal transport error while %s (%s: %s); stopping.", stage, type(exc).__name__, exc
            )


def _choose(result: HandlerResult, policy: ResolvedPolicy, attempt: int, job_name: str) -> _Outcome:
    """Choose the single outcome for a handler result.

    A release or fail recorded by the job has already won inside ``run_prepared``.
    """
    if result.kind == "success":
        return _Outcome("complete")
    if result.kind == "release":
        return _Outcome("release", delay=result.delay)
    if result.kind == "fail":
        reason = result.exception or JobFailedError(f"{job_name} has been marked as failed.")
        return _Outcome("fail", exception=reason)
    error = result.exception or JobFailedError(f"{job_name} failed.")
    logger.error(
        "Job %s failed on attempt %d: %s: %s",
        job_name,
        attempt,
        type(error).__name__,
        error,
        exc_info=(type(error), error, error.__traceback__),
    )
    if policy.is_last_attempt(attempt):
        return _Outcome("fail", exception=error)
    return _Outcome("release", delay=policy.retry_delay(attempt))


def _utcnow() -> datetime:
    """Get the current time in UTC."""
    return datetime.now(timezone.utc)


def _traceback(frame: types.FrameType | None) -> types.TracebackType | None:
    """Build a traceback of where the job was when the alarm fired."""
    tb = None
    while frame is not None:
        tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno or 0)
        frame = frame.f_back
    return tb


def resolve_target(spec: str) -> WorkerTarget:
    """Resolve the worker target named by a ``module:attribute`` spec.

    The target may be a registry, an object exposing ``registry`` and ``lifespan()``, or an
    app whose ``state.laravel_cloud_queues`` is one. Raises a :class:`ConfigurationError`
    otherwise. FastAPI itself is never imported.
    """
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise ConfigurationError(f"Worker target must look like 'module:attribute', got [{spec}].")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing = exc.name or ""
        if module_name == missing or module_name.startswith(missing + "."):
            raise ConfigurationError(
                f"Cannot import module [{module_name}] for worker target [{spec}]; "
                "run from the directory that contains it."
            ) from None
        raise
    obj: object = module
    for part in attribute.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            raise ConfigurationError(
                f"Module [{module_name}] has no attribute [{attribute}]."
            ) from None
    if isinstance(obj, WorkerTarget):
        return obj
    bound = getattr(getattr(obj, "state", None), "laravel_cloud_queues", None)
    if isinstance(bound, WorkerTarget):
        return bound
    raise ConfigurationError(
        f"[{spec}] is not a worker target: expected a Registry, a FastAPI app with "
        "LaravelCloudQueues bound, or an object with 'registry' and 'lifespan()'."
    )


__all__ = [
    "EXIT_CONFIG",
    "EXIT_FATAL",
    "EXIT_OK",
    "EXIT_TIMEOUT",
    "Worker",
    "WorkerOptions",
    "resolve_target",
]
