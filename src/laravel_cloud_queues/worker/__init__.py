"""The queue worker.

State machine per delivery (exactly one outcome owner):
received -> [decode: defect => terminal] -> [pre-run attempt check] -> running
-> outcome chosen (success | release | fail | error->retry/terminal | timeout)
-> reporting (transport complete/release) -> completed | ambiguous(stop).
See docs/contract/worker.md.

Shutdown while idle: SIGTERM/SIGINT call ``consumer.interrupt()``; the agent and Redis
receives return promptly. A single-queue SQS long poll (``WaitTimeSeconds=20``) is not
aborted, because an aborted ReceiveMessage may still dequeue a message and burn an attempt:
the worker waits for the current poll (at most ~20 s), runs any message it hands over, then
stops.
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
EXIT_FATAL = 1
EXIT_CONFIG = 2
EXIT_TIMEOUT = 124

SQS_WAIT_SECONDS = 20.0
TRANSIENT_RETRY_SECONDS = 1.0
ALARM_LOCK_TIMEOUT = 0.5
"""Bounded lock waits for writes from the SIGALRM handler (it may interrupt a write)."""

logger = logging.getLogger("laravel_cloud_queues.worker")

Status = Literal["processed", "released", "failed"]
_STATUS: dict[str, Status] = {"complete": "processed", "release": "released", "fail": "failed"}


@dataclass(frozen=True)
class WorkerOptions:
    queues: tuple[str, ...] | None = None
    """Priority list; ``None`` = backend default / Cloud assignment."""
    max_jobs: int | None = None
    max_time: float | None = None
    stop_when_empty: bool = False
    stop_when_empty_for: float | None = None
    timeout: float = 60.0
    """Default job timeout when the message omits one (0 disables)."""
    sleep: float = 3.0
    rest: float = 0.0
    lease_seconds: int = 60
    """Visibility/reservation lease renewed every third by the watchdog (D7)."""


@dataclass(frozen=True)
class _Runtime:
    """Everything resolved at startup (configuration errors surface here, exit 2)."""

    registry: Registry
    config: QueueConfig
    telemetry: Telemetry
    consumer: Consumer
    queues: tuple[str, ...]
    wait: float
    """``wait_seconds`` passed to ``receive``."""
    sleep_when_empty: bool
    """``--sleep`` after an empty poll (only when the receive did not already wait)."""
    defaults: WorkerDefaults


@dataclass(frozen=True)
class _Outcome:
    kind: Literal["complete", "release", "fail"]
    delay: int = 0
    exception: BaseException | None = None


@dataclass(frozen=True)
class _Running:
    """The delivery the SIGALRM handler acts on."""

    delivery: Delivery
    policy: ResolvedPolicy
    job_name: str
    started_at: datetime
    watchdog: Watchdog | None


class Worker:
    def __init__(self, target: WorkerTarget, options: WorkerOptions | None = None) -> None:
        self._target = target
        self._options = options or WorkerOptions()
        self._clock = time.monotonic
        self._runtime: _Runtime | None = None
        self._running: _Running | None = None
        self._stopping = threading.Event()
        self._wake: anyio.Event | None = None

    def run(self) -> int:
        """Run until a stop condition; returns the process exit code (0/1/2; 124 exits via
        ``os._exit`` from the timeout handler). Must run on the main thread."""
        return anyio.run(self._main, backend="asyncio")

    # --- process -------------------------------------------------------------------------

    async def _main(self) -> int:
        try:
            runtime = self._start()
        except ConfigurationError as exc:
            logger.error("Configuration error: %s", exc)
            return EXIT_CONFIG
        self._runtime = runtime
        self._wake = anyio.Event()
        previous = signal.signal(signal.SIGALRM, self._on_alarm)
        try:
            self._watch_signals(runtime.consumer)
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
        )

    def _select_queues(self, config: QueueConfig) -> tuple[str, ...]:
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

    def _watch_signals(self, consumer: Consumer) -> None:
        # Loop signal handlers (the asyncio backend is fixed): a signal that arrives while a
        # sync handler blocks the loop is handled once it returns, so the current job still
        # completes and reports. Repeated signals only repeat the request.
        def stop(signum: signal.Signals) -> None:
            logger.info("Received %s; stopping after the current job.", signum.name)
            self._stopping.set()
            if self._wake is not None:
                self._wake.set()
            consumer.interrupt()

        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop, signum)

    async def _pause(self, seconds: float) -> None:
        """Sleep, waking early on a stop signal."""
        if seconds <= 0 or self._stopping.is_set() or self._wake is None:
            return
        with anyio.move_on_after(seconds):
            await self._wake.wait()

    # --- loop ----------------------------------------------------------------------------

    async def _loop(self, runtime: _Runtime) -> int:
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
            await self._pause(pause)
        logger.info("Worker stopping: signal.")
        return EXIT_OK

    # --- one delivery --------------------------------------------------------------------

    async def _deliver(self, runtime: _Runtime, delivery: Delivery) -> int | None:
        """Process one delivery; returns an exit code when the worker must stop."""
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
        """One transport call, then the completion records. Acknowledgement failures are
        never handler errors and never lead to a second outcome. Self-managed terminal
        failures log their record first (D6b): it is the only record, so it must not be lost
        if the delete succeeds and the process then dies."""
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
        """Completion records after reporting: ``failed_job`` for managed terminal failures
        (same timestamp as ``failed``), then the lifecycle event and the job log line."""
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
        """D6b failure record on stdout, written before ``complete`` (``sqs`` / ``redis``)."""
        if runtime.config.emits_cloud_events:
            return
        record = failure_log_record(
            queue=delivery.queue,
            payload=delivery.body,
            exception=exception,
            attempts=delivery.attempt,
            message_id=delivery.message_id,
            started_at=started_at,
            timestamp=_utcnow(),
        )
        runtime.telemetry.log_line(record, lock_timeout=lock_timeout)

    # --- timeout (SIGALRM, D2) -----------------------------------------------------------

    def _on_alarm(self, signum: int, frame: types.FrameType | None) -> None:
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
        """Terminal check -> (terminal: [D6b record] -> complete -> [failed_job]) -> lifecycle
        event. No release and no backoff: visibility / reservation expiry redelivers."""
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
    """Exactly one outcome; a recorded release/fail already won inside ``run_prepared``."""
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
    return datetime.now(timezone.utc)


def _traceback(frame: types.FrameType | None) -> types.TracebackType | None:
    """Where the job was when the alarm fired (for the failure record)."""
    tb = None
    while frame is not None:
        tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno or 0)
        frame = frame.f_back
    return tb


def resolve_target(spec: str) -> WorkerTarget:
    """``module:attr`` -> WorkerTarget: a Registry, an object exposing ``registry`` and
    ``lifespan()``, or an app whose ``state.laravel_cloud_queues`` is one. ConfigurationError
    otherwise. Never imports fastapi itself."""
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
