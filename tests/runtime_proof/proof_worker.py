"""One-job proof worker. Run as ``python proof_worker.py ...``.

Proves PROJECT_SCOPE.md §13/§14 and decisions D2/D7 without importing the package.

The orchestrator is ``asyncio.run``. Async handlers are awaited. Sync handlers
run on this thread and block the loop so ``SIGALRM`` can interrupt them. A
watchdog thread renews visibility every lease/3 and is joined before a success
is reported. Timeout exits with ``os._exit(124)`` from the alarm handler.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import threading
import time
from typing import Any

from broker import Broker

QUEUE = "proof"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process-level timeout and watchdog proof worker")
    parser.add_argument("--broker", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--socket", default="")
    parser.add_argument(
        "--handler", required=True, choices=("async", "sync", "native", "sleep", "noop")
    )
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("--tries", type=int, required=True)
    parser.add_argument("--visibility", type=float, required=True)
    parser.add_argument("--lease", type=float, default=0.0, help="0 disables the renewal watchdog")
    parser.add_argument("--work-seconds", type=float, default=0.0)
    parser.add_argument("--native-iterations", type=int, default=0)
    parser.add_argument("--linger", type=float, default=0.0)
    parser.add_argument(
        "--wait", type=float, default=0.0, help="thief: poll until this many seconds"
    )
    parser.add_argument("--fail-on-timeout", action="store_true")
    parser.add_argument("--role", choices=("owner", "thief"), default="owner")
    parser.add_argument("--queue", default=QUEUE)
    args = parser.parse_args(argv)
    if args.timeout < 0 or args.visibility < 0 or args.lease < 0:
        parser.error("timeout, visibility, and lease must be >= 0")
    if args.handler == "native" and args.role == "owner" and args.native_iterations <= 0:
        parser.error("native handler requires --native-iterations")
    return args


def append_event(path: str, payload: dict[str, Any]) -> None:
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
    data = line.encode()
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def emit_socket(path: str, payload: dict[str, Any]) -> None:
    if not path:
        return
    line = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        conn.settimeout(2)
        conn.connect(path)
        conn.sendall(line)
    finally:
        conn.close()


def utc_timestamp(when: float | None = None) -> str:
    when = time.time() if when is None else when
    seconds = int(when)
    micros = int(round((when - seconds) * 1_000_000))
    if micros >= 1_000_000:
        seconds += 1
        micros -= 1_000_000
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(seconds)) + f".{micros:06d}"


def lifecycle(queue: str, type_: str, started: float) -> dict[str, Any]:
    return {
        "_cloud_event": "queue",
        "timestamp": utc_timestamp(),
        "type": type_,
        "queue": queue,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


async def run_handler(
    handler: str, seconds: float, iterations: int, native_window: dict[str, float]
) -> None:
    # Sync and native calls stay on this thread. A thread pool would hide them
    # from SIGALRM, and an AnyIO worker thread would hide them from the watchdog's GIL story.
    if handler == "async":
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        return
    if handler == "sync":
        deadline = time.monotonic() + seconds
        value = 0
        while time.monotonic() < deadline:
            value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
        return
    if handler == "native":
        # sum() iterates in C, holds the GIL, and does not check signals.
        # The alarm handler runs only after this call returns.
        native_window["start"] = time.time()
        sum(range(iterations))
        native_window["end"] = time.time()
        return
    if handler == "sleep":
        time.sleep(seconds)
        return
    if handler == "noop":
        return
    raise RuntimeError(f"unknown handler {handler}")


def watchdog(
    stop: threading.Event, lost: threading.Event, broker_path: str, receipt: str, lease: float
) -> None:
    broker = Broker(broker_path)
    interval = lease / 3.0
    while not stop.wait(interval):
        try:
            ok = broker.renew(receipt, lease)
        except Exception:
            lost.set()
            return
        if not ok:
            lost.set()
            return


def lost_lease(events: str, message_id: str, attempt: int) -> None:
    append_event(events, {"kind": "lease_lost", "message_id": message_id, "attempt": attempt})


def main(argv: list[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    broker = Broker(args.broker)
    deadline = time.monotonic() + args.wait
    delivery = None
    while True:
        delivery = broker.receive(args.visibility)
        if delivery is not None:
            break
        if args.role != "thief" or time.monotonic() >= deadline:
            break
        time.sleep(0.05)
    if delivery is None:
        sys.exit(2)

    if args.role == "thief":
        append_event(
            args.events,
            {
                "kind": "stolen",
                "message_id": delivery.message_id,
                "attempt": delivery.receive_count,
                "receipt": delivery.receipt,
            },
        )
        if not broker.delete(delivery.receipt):
            sys.exit(1)
        sys.exit(0)

    attempt = delivery.receive_count
    append_event(
        args.events,
        {
            "kind": "started",
            "message_id": delivery.message_id,
            "attempt": attempt,
            "receipt": delivery.receipt,
            "pid": os.getpid(),
        },
    )

    if args.tries > 0 and attempt > args.tries:
        append_event(
            args.events,
            {
                "kind": "failure",
                "reason": "MaxAttemptsExceeded",
                "message_id": delivery.message_id,
                "attempt": attempt,
                "tries": args.tries,
                "fail_on_timeout": args.fail_on_timeout,
                "payload": delivery.payload,
            },
        )
        broker.delete(delivery.receipt)
        started = time.monotonic()
        event = lifecycle(args.queue, "failed", started)
        append_event(args.events, event)
        sys.exit(0)

    stop = threading.Event()
    lost = threading.Event()
    thread: threading.Thread | None = None
    if args.lease > 0:
        thread = threading.Thread(
            target=watchdog,
            args=(stop, lost, args.broker, delivery.receipt, args.lease),
            name="visibility-watchdog",
        )
        thread.start()
        time.sleep(0.02)

    started = time.monotonic()

    def on_alarm(signum: int, frame: object) -> None:
        try:
            terminal = (args.tries > 0 and attempt >= args.tries) or args.fail_on_timeout
            if terminal:
                append_event(
                    args.events,
                    {
                        "kind": "failure",
                        "reason": "TimeoutExceeded",
                        "message_id": delivery.message_id,
                        "attempt": attempt,
                        "tries": args.tries,
                        "fail_on_timeout": args.fail_on_timeout,
                        "payload": delivery.payload,
                    },
                )
                Broker(args.broker).delete(delivery.receipt)
            event = lifecycle(args.queue, "failed" if terminal else "released", started)
            append_event(args.events, event)
            emit_socket(args.socket, event)
        finally:
            os._exit(124)

    signal.signal(signal.SIGALRM, on_alarm)
    native_window: dict[str, float] = {}
    if args.timeout > 0:
        signal.setitimer(signal.ITIMER_REAL, args.timeout)
    try:
        asyncio.run(
            run_handler(args.handler, args.work_seconds, args.native_iterations, native_window)
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)

    if "end" in native_window:
        append_event(
            args.events,
            {"kind": "native_window", "start": native_window["start"], "end": native_window["end"]},
        )

    # A GIL-holding handler delays the watchdog until it returns. Keep the
    # stop flag clear while yielding so the thread can attempt the renewal
    # it missed, and fail it, before success is reported.
    if thread is not None:
        poll_until = time.monotonic() + 0.4
        while time.monotonic() < poll_until and not lost.is_set():
            time.sleep(0.02)
        stop.set()
        thread.join(timeout=2)
        if lost.is_set() or not Broker(args.broker).owns(delivery.receipt):
            lost_lease(args.events, delivery.message_id, attempt)
            sys.exit(1)

    if not broker.delete(delivery.receipt):
        lost_lease(args.events, delivery.message_id, attempt)
        sys.exit(1)
    append_event(
        args.events,
        {"kind": "success", "message_id": delivery.message_id, "attempt": attempt},
    )
    if args.linger > 0:
        time.sleep(args.linger)
    sys.exit(0)


if __name__ == "__main__":
    main()
