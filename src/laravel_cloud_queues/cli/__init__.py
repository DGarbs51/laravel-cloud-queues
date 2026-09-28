"""``laravel-cloud-queues`` console entry point (PROJECT_SCOPE.md §23). CONTRACT — lane L6.
See docs/contract/cli.md."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import traceback
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit

from ..config import QueueConfig, StaticCredentials
from ..errors import ConfigurationError, LaravelCloudQueuesError
from ..jobs.job import AnyJob
from ..worker import EXIT_CONFIG, EXIT_FATAL, Worker, WorkerOptions, resolve_target

PROG = "laravel-cloud-queues"


def main(argv: Sequence[str] | None = None) -> int:
    """``work TARGET``, ``inspect TARGET``, ``conformance ...``. Returns the exit code."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["conformance"]:
        # Everything after the command belongs to the suite, including its options.
        return _conformance(argv[1:])
    args = _parser().parse_args(argv)
    command: Callable[[argparse.Namespace], int] = args.command
    try:
        return command(args)
    except (Exception, KeyboardInterrupt) as exc:
        if args.debug:
            traceback.print_exc()
        print(f"{PROG}: error: {_describe(exc)}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(exc, ConfigurationError) else EXIT_FATAL


def _describe(exc: BaseException) -> str:
    if isinstance(exc, LaravelCloudQueuesError):
        return str(exc)
    return f"{type(exc).__name__}: {exc} (use --debug for the traceback)"


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--debug", action="store_true", help="show tracebacks on errors")
    parser = argparse.ArgumentParser(prog=PROG, description="Laravel Cloud queues for Python.")
    commands = parser.add_subparsers(dest="name", required=True, metavar="COMMAND")

    work = commands.add_parser("work", parents=[common], help="run a queue worker")
    work.add_argument("target", metavar="TARGET", help="module:attribute (registry or app)")
    work.add_argument("--queue", type=_queues, help="comma-separated priority list")
    work.add_argument("--max-jobs", type=_positive_int, help="stop after N deliveries")
    work.add_argument("--max-time", type=_seconds, help="stop after S seconds")
    work.add_argument("--stop-when-empty", action="store_true", help="stop on an empty poll")
    work.add_argument(
        "--stop-when-empty-for", type=_seconds, help="stop after S seconds without a job"
    )
    work.add_argument("--timeout", type=_seconds, default=60.0, help="default job timeout")
    work.add_argument("--sleep", type=_seconds, default=3.0, help="wait after an empty poll")
    work.add_argument("--rest", type=_seconds, default=0.0, help="pause between jobs")
    work.set_defaults(command=_work)

    inspect = commands.add_parser("inspect", parents=[common], help="show jobs and settings")
    inspect.add_argument("target", metavar="TARGET")
    inspect.add_argument("--json", action="store_true", help="machine-readable output")
    inspect.set_defaults(command=_inspect)

    # Listed for --help only; ``main`` passes its arguments through verbatim.
    commands.add_parser("conformance", help="run the repository conformance suite")
    return parser


def _queues(value: str) -> tuple[str, ...]:
    queues = tuple(part.strip() for part in value.split(",") if part.strip())
    if not queues:
        raise argparse.ArgumentTypeError("expected one or more queue names")
    return queues


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {value!r}")
    return number


def _seconds(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        number = -1.0
    if not 0 <= number < float("inf"):
        raise argparse.ArgumentTypeError(
            f"expected a non-negative number of seconds, got {value!r}"
        )
    return number


def _import_path() -> None:
    """Make the current directory importable, like uvicorn."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def _work(args: argparse.Namespace) -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            stream=sys.stderr,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    _import_path()
    target = resolve_target(args.target)
    options = WorkerOptions(
        queues=args.queue,
        max_jobs=args.max_jobs,
        max_time=args.max_time,
        stop_when_empty=args.stop_when_empty,
        stop_when_empty_for=args.stop_when_empty_for,
        timeout=args.timeout,
        sleep=args.sleep,
        rest=args.rest,
    )
    return Worker(target, options).run()


def _inspect(args: argparse.Namespace) -> int:
    _import_path()
    registry = resolve_target(args.target).registry
    registry.load()
    report = _report(registry.config, registry.jobs())
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(_render(report))
    return 0


def _report(config: QueueConfig, jobs: Mapping[str, AnyJob]) -> dict[str, object]:
    """Built field by field so nothing secret (credentials, Redis passwords) can leak."""
    queues: dict[str, object] = {"default": config.default_queue}
    settings: dict[str, object] = {}
    if config.managed is not None:
        queues["worker_assignment"] = config.managed.queue
        queues["managed_inventory"] = list(config.managed.queues)
        settings["agent_enabled"] = config.managed.agent.enabled
        settings["agent_socket"] = config.managed.agent.socket
        settings["after_commit"] = config.managed.after_commit
    if config.sqs is not None:
        credentials = config.sqs.credentials
        settings["sqs_prefix"] = config.sqs.prefix
        settings["sqs_suffix"] = config.sqs.suffix
        settings["sqs_region"] = config.sqs.region
        settings["sqs_endpoint"] = config.sqs.endpoint_url
        settings["sqs_credentials"] = (
            "static" if isinstance(credentials, StaticCredentials) else credentials
        )
    if config.redis is not None:
        settings["redis_url"] = _redact_url(config.redis.url)
        settings["redis_prefix"] = config.redis.prefix
    if config.emits_cloud_events:
        settings["log_socket"] = config.log_socket
    registered = []
    for name, job in sorted(jobs.items()):
        policy = job.policy
        backoff = policy.backoff
        if backoff is not None and not isinstance(backoff, (int, float)):
            backoff = list(backoff)
        registered.append(
            {
                "name": name,
                "queue": job.queue,
                "tries": policy.tries,
                "backoff": backoff,
                "timeout": policy.timeout,
                "fail_on_timeout": policy.fail_on_timeout,
            }
        )
    return {"mode": config.mode, "queues": queues, "jobs": registered, "settings": settings}


def _redact_url(url: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}{parts.path}"


def _render(report: dict[str, object]) -> str:
    lines = [f"Mode: {report['mode']}", "Queues:"]
    lines += [f"  {key}: {_text(value)}" for key, value in _items(report["queues"])]
    jobs = report["jobs"]
    assert isinstance(jobs, list)
    lines.append(f"Jobs ({len(jobs)}):")
    for job in jobs:
        declared = ", ".join(
            f"{key}={_text(value)}"
            for key, value in job.items()
            if key != "name" and value is not None
        )
        lines.append(f"  {job['name']}" + (f" ({declared})" if declared else ""))
    lines.append("Settings:")
    lines += [f"  {key}: {_text(value)}" for key, value in _items(report["settings"])]
    return "\n".join(lines)


def _items(value: object) -> list[tuple[str, object]]:
    assert isinstance(value, dict)
    return list(value.items())


def _text(value: object) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value) or "-"
    return "-" if value is None else str(value)


def _conformance(args: Sequence[str]) -> int:
    root = _checkout_root(Path.cwd())
    if root is None:
        print(
            f"{PROG}: error: the conformance suite needs a repository checkout "
            "(demo/conformance and the harness emulators are not installed with the package). "
            "Clone the laravel-cloud-queues repository, run `uv sync` in it and run "
            f"`{PROG} conformance` (or `python -m demo.conformance`) from its root.",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    command = [sys.executable, "-m", "demo.conformance", *args]
    return subprocess.call(command, cwd=root)


def _checkout_root(start: Path) -> Path | None:
    for directory in (start, *start.parents):
        if (directory / "demo" / "conformance" / "__main__.py").is_file():
            return directory
    return None


__all__ = ["main"]
