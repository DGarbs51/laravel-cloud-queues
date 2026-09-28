"""Worker subprocess: lifespan once, teardown before ack, SIGTERM shutdown.

Requires the redis transport (L5), registry/dispatch (L3c), and the worker CLI (L6).
Until those land the test fails; it is not skipped when Valkey is up.
"""

from __future__ import annotations

import importlib
import os
import signal
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _events(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


@pytest.mark.redis
@pytest.mark.subprocess
def test_teardown_runs_before_ack_and_lifespan_once(
    redis_url: str,
    redis_prefix: str,
    tmp_path: Path,
    run_process: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teardown writes a marker while the message is still reserved.

    Laravel removes the reserved member on success (``RedisQueue.php:619``). The marker
    must show ``reserved>=1`` at teardown; after the worker exits the sorted set is empty.
    """

    events_path = tmp_path / "events"
    _prepare_env(monkeypatch, redis_url, redis_prefix, events_path)
    main = importlib.import_module("tests.integration.fastapi.apps.main")
    importlib.reload(main)
    main.touch.dispatch(1)

    process = run_process(  # type: ignore[operator]
        [
            sys.executable,
            "-m",
            "laravel_cloud_queues",
            "work",
            "tests.integration.fastapi.apps.main:app",
            "--max-jobs",
            "1",
            "--stop-when-empty",
            "--sleep",
            "0.2",
        ],
        env=_child_env(redis_url, redis_prefix, events_path),
        cwd=REPO_ROOT,
    )
    result = process.wait(timeout=30)
    assert result.returncode == 0, result.stderr
    lines = _events(events_path)
    assert lines.count("lifespan-start") == 1
    assert lines.count("lifespan-stop") == 1
    teardown = next(line for line in lines if line.startswith("dep-teardown"))
    reserved = int(teardown.rsplit("=", 1)[1])
    assert reserved >= 1
    assert lines.index("handled 1") < lines.index(teardown)
    assert lines.index(teardown) < lines.index("lifespan-stop")

    import redis

    client = redis.Redis.from_url(redis_url)
    try:
        assert int(client.zcard(f"{redis_prefix}queues:default:reserved")) == 0
    finally:
        client.close()


@pytest.mark.redis
@pytest.mark.subprocess
def test_sigterm_runs_lifespan_shutdown(
    redis_url: str,
    redis_prefix: str,
    tmp_path: Path,
    run_process: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events_path = tmp_path / "events"
    _prepare_env(monkeypatch, redis_url, redis_prefix, events_path)
    process = run_process(  # type: ignore[operator]
        [
            sys.executable,
            "-m",
            "laravel_cloud_queues",
            "work",
            "tests.integration.fastapi.apps.main:app",
            "--sleep",
            "30",
        ],
        env=_child_env(redis_url, redis_prefix, events_path),
        cwd=REPO_ROOT,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and "lifespan-start" not in _events(events_path):
        time.sleep(0.05)
    assert "lifespan-start" in _events(events_path), process.stderr_path.read_text(errors="replace")
    process.send_signal(signal.SIGTERM)
    result = process.wait(timeout=20)
    assert result.returncode == 0, result.stderr
    lines = _events(events_path)
    assert lines.count("lifespan-start") == 1
    assert lines.count("lifespan-stop") == 1


def _prepare_env(
    monkeypatch: pytest.MonkeyPatch,
    redis_url: str,
    redis_prefix: str,
    events_path: Path,
) -> None:
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_BACKEND", "redis")
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_REDIS_URL", redis_url)
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_REDIS_PREFIX", redis_prefix)
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_REDIS_QUEUE", "default")
    monkeypatch.setenv("LCQ_FA_EVENTS", str(events_path))
    monkeypatch.setenv("PYTHONPATH", str(REPO_ROOT))


def _child_env(redis_url: str, redis_prefix: str, events_path: Path) -> dict[str, str]:
    return {
        "LARAVEL_CLOUD_QUEUES_BACKEND": "redis",
        "LARAVEL_CLOUD_QUEUES_REDIS_URL": redis_url,
        "LARAVEL_CLOUD_QUEUES_REDIS_PREFIX": redis_prefix,
        "LARAVEL_CLOUD_QUEUES_REDIS_QUEUE": "default",
        "LCQ_FA_EVENTS": str(events_path),
        "PYTHONPATH": os.pathsep.join([str(REPO_ROOT), os.environ.get("PYTHONPATH", "")]).rstrip(
            os.pathsep
        ),
    }
