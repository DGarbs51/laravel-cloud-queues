"""The ``laravel-cloud-queues`` console entry point.

The commands and exit codes follow the contract in ``docs/contract/cli.md``.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import re
import subprocess
import sys
import traceback
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from urllib.parse import unquote_plus

import click

from ..config import QueueConfig, StaticCredentials
from ..errors import ConfigurationError
from ..jobs.job import AnyJob
from ..worker import EXIT_CONFIG, EXIT_FATAL, Worker, WorkerOptions, resolve_target

PROG = "laravel-cloud-queues"
"""The program name shown in usage and error messages."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the console application and return its exit code.

    The available commands are ``work TARGET``, ``inspect TARGET`` and ``conformance ...``.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        code = cli.main(args, prog_name=PROG, standalone_mode=False)
    except click.UsageError as exc:
        exc.show()
        raise SystemExit(exc.exit_code) from None
    return code if isinstance(code, int) else 0


@click.group(help="Laravel Cloud queues for Python.")
def cli() -> None:
    """Get the command group for the queue commands.

    The group may be mounted in any click CLI, e.g. with Flask:
    ``app.cli.add_command(cli, "queues")``.
    """


def _command(func: Callable[..., int]) -> Callable[..., None]:
    """Wrap the given function as a command that exits with its returned code.

    The wrapper adds a ``--debug`` flag and turns any failure into one redacted error line.
    """

    @click.option("--debug", is_flag=True, help="Show tracebacks on errors.")
    @functools.wraps(func)
    def wrapper(debug: bool, **params: object) -> None:
        """Run the command and exit with its code, reporting failures on stderr."""
        try:
            code = func(**params)
        except (click.ClickException, click.exceptions.Exit, click.Abort):
            raise
        except (Exception, KeyboardInterrupt) as exc:
            if debug:
                click.echo(_redact(traceback.format_exc()), err=True, nl=False)
            click.echo(f"{PROG}: error: {_describe(exc)}", err=True)
            code = EXIT_CONFIG if isinstance(exc, ConfigurationError) else EXIT_FATAL
        click.get_current_context().exit(code)

    return wrapper


def _describe(exc: BaseException) -> str:
    """Format the given exception as a single redacted error message."""
    return f"{type(exc).__name__}: {_redact(str(exc))} (use --debug for the traceback)"


class _Queues(click.ParamType[tuple[str, ...], str]):
    """The parameter type for a comma-separated list of queue names."""

    name = "Q[,Q...]"
    """The placeholder shown for the parameter in help output."""

    def convert(
        self, value: str, param: click.Parameter | None, ctx: click.Context | None
    ) -> tuple[str, ...]:
        """Parse the given value into a tuple of queue names."""
        queues = tuple(part.strip() for part in value.split(",") if part.strip())
        if not queues:
            self.fail("expected one or more queue names")
        return queues


class _Seconds(click.ParamType[float, "str | float"]):
    """The parameter type for a finite, non-negative number of seconds.

    Unlike ``click.FloatRange``, this type rejects ``nan`` and ``inf``.
    """

    name = "S"
    """The placeholder shown for the parameter in help output."""

    def convert(
        self, value: str | float, param: click.Parameter | None, ctx: click.Context | None
    ) -> float:
        """Parse the given value into a number of seconds."""
        try:
            number = float(value)
        except ValueError:
            number = -1.0
        if not 0 <= number < float("inf"):
            self.fail(f"expected a non-negative number of seconds, got {value!r}")
        return number


SECONDS = _Seconds()
"""The shared parameter type for options given in seconds."""


def _import_path() -> None:
    """Make the current directory importable, like uvicorn does."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


@cli.command(short_help="Run a queue worker.")
@click.argument("target", metavar="TARGET")
@click.option("--queue", type=_Queues(), help="Comma-separated priority list.")
@click.option("--max-jobs", type=click.IntRange(min=1), help="Stop after N deliveries.")
@click.option("--max-time", type=SECONDS, help="Stop after S seconds.")
@click.option("--stop-when-empty", is_flag=True, help="Stop on an empty poll.")
@click.option("--stop-when-empty-for", type=SECONDS, help="Stop after S seconds without a job.")
@click.option(
    "--timeout", type=SECONDS, default=60.0, show_default=True, help="Default job timeout."
)
@click.option(
    "--sleep", type=SECONDS, default=3.0, show_default=True, help="Wait after an empty poll."
)
@click.option("--rest", type=SECONDS, default=0.0, show_default=True, help="Pause between jobs.")
@_command
def work(
    target: str,
    queue: tuple[str, ...] | None,
    max_jobs: int | None,
    max_time: float | None,
    stop_when_empty: bool,
    stop_when_empty_for: float | None,
    timeout: float,
    sleep: float,
    rest: float,
) -> int:
    """Run a queue worker for TARGET (module:attribute, a registry or an app)."""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            stream=sys.stderr,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    _import_path()
    options = WorkerOptions(
        queues=queue,
        max_jobs=max_jobs,
        max_time=max_time,
        stop_when_empty=stop_when_empty,
        stop_when_empty_for=stop_when_empty_for,
        timeout=timeout,
        sleep=sleep,
        rest=rest,
    )
    return Worker(resolve_target(target), options).run()


@cli.command(short_help="Show jobs and settings.")
@click.argument("target", metavar="TARGET")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
@_command
def inspect(target: str, as_json: bool) -> int:
    """Show the mode, queues, registered jobs and non-secret settings of TARGET."""
    _import_path()
    registry = resolve_target(target).registry
    registry.load()
    report = _report(registry.config, registry.jobs())
    click.echo(json.dumps(report, indent=2) if as_json else _render(report))
    return 0


def _report(config: QueueConfig, jobs: Mapping[str, AnyJob]) -> dict[str, object]:
    """Build the inspection report for the given configuration and jobs.

    The report is built field by field so nothing secret (credentials, Redis passwords)
    can leak into it.
    """
    queues: dict[str, object] = {"default": _redact(config.default_queue)}
    settings: dict[str, object] = {}
    if config.managed is not None:
        queues["worker_assignment"] = _redact(config.managed.queue)
        queues["managed_inventory"] = [_redact(queue) for queue in config.managed.queues]
        settings["agent_enabled"] = config.managed.agent.enabled
        settings["agent_socket"] = _redact(config.managed.agent.socket)
        settings["after_commit"] = config.managed.after_commit
    if config.sqs is not None:
        credentials = config.sqs.credentials
        settings["sqs_prefix"] = _redact(config.sqs.prefix)
        settings["sqs_suffix"] = config.sqs.suffix
        settings["sqs_region"] = config.sqs.region
        settings["sqs_endpoint"] = (
            _redact(config.sqs.endpoint_url) if config.sqs.endpoint_url is not None else None
        )
        settings["sqs_credentials"] = (
            "static" if isinstance(credentials, StaticCredentials) else credentials
        )
    if config.redis is not None:
        settings["redis_url"] = _redact(config.redis.url)
        settings["redis_prefix"] = config.redis.prefix
    if config.emits_cloud_events:
        settings["log_socket"] = _redact(config.log_socket)
    registered = []
    for name, job in sorted(jobs.items()):
        policy = job.policy
        backoff = policy.backoff
        if backoff is not None and not isinstance(backoff, (int, float)):
            backoff = list(backoff)
        registered.append(
            {
                "name": name,
                "queue": _redact(job.queue) if job.queue is not None else None,
                "tries": policy.tries,
                "backoff": backoff,
                "timeout": policy.timeout,
                "fail_on_timeout": policy.fail_on_timeout,
            }
        )
    return {"mode": config.mode, "queues": queues, "jobs": registered, "settings": settings}


def _redact(text: str) -> str:
    """Remove URL userinfo and redact credential query values from the given text.

    This is also applied to tracebacks.
    """
    text = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/\s?#]*@", r"\1", text)
    return re.sub(
        r"([?&]([^=&#\s]+)=)([^&#\s\"'<>]*)",
        lambda match: (
            match[1]
            + (
                "[REDACTED]"
                if any(
                    key in unquote_plus(match[2]).lower()
                    for key in ("token", "secret", "password", "key", "signature", "credential")
                )
                else match[3]
            )
        ),
        text,
    )


def _render(report: dict[str, object]) -> str:
    """Format the inspection report as human-readable text."""
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
    """Get the key and value pairs of the given report section."""
    assert isinstance(value, dict)
    return list(value.items())


def _text(value: object) -> str:
    """Format the given report value for display."""
    if isinstance(value, list):
        return ",".join(str(item) for item in value) or "-"
    return "-" if value is None else str(value)


@cli.command(
    short_help="Run the repository conformance suite.",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    add_help_option=False,
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def conformance(args: tuple[str, ...]) -> None:
    """Run the repository conformance suite; every argument passes through to it."""
    click.get_current_context().exit(_conformance(args))


def _conformance(args: Sequence[str]) -> int:
    """Run the conformance suite from the repository checkout and return its exit code.

    Outside a checkout, an explanation is written to stderr and the configuration error
    exit code is returned.
    """
    root = _checkout_root(Path.cwd())
    if root is None:
        click.echo(
            f"{PROG}: error: the conformance suite needs a repository checkout "
            "(tests/conformance and the harness emulators are not installed with the package). "
            "Clone the laravel-cloud-queues repository, run `uv sync` in it and run "
            f"`{PROG} conformance` (or `python -m tests.conformance`) from its root.",
            err=True,
        )
        return EXIT_CONFIG
    command = [sys.executable, "-m", "tests.conformance", *args]
    return subprocess.call(command, cwd=root)


def _checkout_root(start: Path) -> Path | None:
    """Find the repository checkout that contains the given directory, if any."""
    for directory in (start, *start.parents):
        if (directory / "tests" / "conformance" / "__main__.py").is_file():
            return directory
    return None


__all__ = ["cli", "main"]
