"""Load with ``pytest -p harness.pytest_plugin`` (fixtures are function-scoped)."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import NoReturn, Protocol

import pytest

from harness.agent_emulator import AgentEmulator
from harness.log_collector import LogCollector
from harness.process import Process
from harness.redis import connect, redis_service
from harness.redis import redis_url as configured_redis_url
from harness.sqs import ServiceUnavailable, SQSEndpoint
from harness.sqs import sqs_endpoint as start_sqs


def pytest_configure(config: pytest.Config) -> None:
    for marker in ("agent", "sqs", "redis", "socket", "subprocess"):
        config.addinivalue_line("markers", f"{marker}: uses the local {marker} test harness")


def _unavailable(exc: ServiceUnavailable) -> NoReturn:
    if os.environ.get("LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES") == "1":
        pytest.fail(str(exc))
    pytest.skip(str(exc))


@pytest.fixture
def agent_emulator() -> Iterator[AgentEmulator]:
    with AgentEmulator.start() as emulator:
        yield emulator


@pytest.fixture
def log_collector() -> Iterator[LogCollector]:
    with LogCollector() as collector:
        yield collector


@pytest.fixture
def sqs_endpoint() -> Iterator[SQSEndpoint]:
    with ExitStack() as stack:
        try:
            endpoint = stack.enter_context(start_sqs())
        except ServiceUnavailable as exc:
            _unavailable(exc)
        yield endpoint


@pytest.fixture
def redis_url() -> str:
    url = configured_redis_url()
    try:
        client = connect(url)
    except ServiceUnavailable as exc:
        _unavailable(exc)
    client.close()
    return url


@pytest.fixture
def redis_prefix(redis_url: str) -> Iterator[str]:
    with ExitStack() as stack:
        try:
            _, prefix = stack.enter_context(redis_service(redis_url))
        except ServiceUnavailable as exc:
            _unavailable(exc)
        yield prefix


class ProcessFactory(Protocol):
    def __call__(
        self,
        command: Sequence[str],
        *,
        env: Mapping[str, str | None] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        output_dir: str | os.PathLike[str] | None = None,
    ) -> Process: ...


@pytest.fixture
def run_process(tmp_path: Path) -> Iterator[ProcessFactory]:
    with ExitStack() as stack:

        def run(
            command: Sequence[str],
            *,
            env: Mapping[str, str | None] | None = None,
            cwd: str | os.PathLike[str] | None = None,
            output_dir: str | os.PathLike[str] | None = None,
        ) -> Process:
            return stack.enter_context(
                Process(command, env=env, cwd=cwd, output_dir=output_dir or tmp_path)
            )

        yield run
