"""Probe worker: `python worker.py`. One job at a time (PROJECT_SCOPE.md §13)."""

import json
import os
import signal
import sys
import time
from typing import Any

import rqueue

RESERVATION_MARGIN = 10  # seconds beyond the job timeout before a crashed job is redelivered


def handle(kind: str, args: dict[str, Any], attempt: int) -> None:
    if kind == "ok":
        return
    if kind == "flaky" and attempt == 1:
        raise RuntimeError("flaky job fails on its first attempt")
    if kind == "fail":
        raise RuntimeError("job always fails")
    if kind == "slow":
        time.sleep(float(args.get("seconds", 10)))
        return
    if kind not in {"flaky"}:
        raise LookupError(f"unknown job kind {kind!r}")


def log_failure(job: dict[str, Any], exc: str) -> None:
    # D6b: log-only terminal failure, one structured JSON line.
    print(json.dumps({"laravel_cloud_queues": "failed_job", "uuid": job["uuid"],
                      "displayName": job["displayName"], "attempts": job["attempts"],
                      "exception": exc}), flush=True)


def fail(r: Any, reserved: str, job: dict[str, Any], exc: str) -> None:
    log_failure(job, exc)
    rqueue.record(r, job["uuid"], "failed", attempt=job["attempts"], exception=exc)
    rqueue.delete(r, reserved)


def run(r: Any, reserved: str) -> None:
    job = json.loads(reserved)
    attempt, tries = job["attempts"], job["tries"]
    rqueue.record(r, job["uuid"], "started", attempt=attempt)

    if tries > 0 and attempt > tries:  # pre-run check (§12)
        fail(r, reserved, job, "MaxAttemptsExceeded")
        return

    def on_timeout(signum: int, frame: Any) -> None:  # D2
        if tries > 0 and attempt >= tries:
            fail(r, reserved, job, "TimeoutExceeded (terminal)")
        else:
            rqueue.record(r, job["uuid"], "timed_out_released", attempt=attempt)
        print(f"job {job['uuid']} timed out on attempt {attempt}; exiting 124", flush=True)
        os._exit(124)

    signal.signal(signal.SIGALRM, on_timeout)
    signal.setitimer(signal.ITIMER_REAL, job["timeout"])
    try:
        handle(job["kind"], job["args"], attempt)
    except Exception as e:  # noqa: BLE001 — probe records every handler failure
        signal.setitimer(signal.ITIMER_REAL, 0)
        if tries > 0 and attempt >= tries:
            fail(r, reserved, job, repr(e))
        else:
            backoff = job["backoff"]
            delay = backoff[min(attempt - 1, len(backoff) - 1)]
            rqueue.record(r, job["uuid"], "released", attempt=attempt, delay=delay, exception=repr(e))
            rqueue.release(r, reserved, delay)
        return
    signal.setitimer(signal.ITIMER_REAL, 0)
    rqueue.record(r, job["uuid"], "processed", attempt=attempt)
    rqueue.delete(r, reserved)


def main() -> None:
    stopping = False

    def stop(signum: int, frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    r = rqueue.client()
    print(f"probe worker {os.getpid()} started", flush=True)
    while not stopping:
        reserved = rqueue.reserve(r, 60 + RESERVATION_MARGIN)
        if reserved is None:
            time.sleep(1)
            continue
        job = json.loads(reserved)
        # Reservation must outlive the job timeout; re-reserve with the real timeout.
        r.zadd(rqueue.keys()["reserved"], {reserved: time.time() + job["timeout"] + RESERVATION_MARGIN})
        run(r, reserved)
    print("probe worker stopping", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
