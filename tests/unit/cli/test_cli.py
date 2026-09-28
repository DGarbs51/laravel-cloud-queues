from __future__ import annotations

import json
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import laravel_cloud_queues.cli as cli
from laravel_cloud_queues.worker import WorkerOptions

APP = """
from contextlib import asynccontextmanager
from types import SimpleNamespace

from laravel_cloud_queues.config import (
    AgentConfig, ManagedQueuesConfig, QueueConfig, RedisConfig, SqsConnectionConfig,
    StaticCredentials,
)

SQS = SqsConnectionConfig(
    prefix="https://sqs.us-east-2.amazonaws.com/123", region="us-east-2",
    credentials=StaticCredentials("AKIDKEY", "TOPSECRET", "SESSIONTOKEN"), queue="emails",
)
CONFIGS = {
    "sqs": QueueConfig(mode="sqs", sqs=SQS),
    "redis": QueueConfig(
        mode="redis", redis=RedisConfig(url="rediss://user:REDISPASS@cache.test:6380/2")
    ),
    "managed": QueueConfig(
        mode="managed", sqs=SQS,
        managed=ManagedQueuesConfig(
            connection=SQS, agent=AgentConfig(enabled=True, socket="/tmp/agent.sock"),
            queue="assigned", queues=("assigned", "emails"),
        ),
    ),
}


class Registry:
    def __init__(self, mode):
        self.config = CONFIGS[mode]
        self.loaded = 0

    @property
    def registry(self):
        return self

    @property
    def backend(self):
        raise AssertionError("inspect must not build the backend")

    @property
    def telemetry(self):
        raise AssertionError("inspect must not build telemetry")

    def load(self):
        self.loaded += 1

    def jobs(self):
        policy = SimpleNamespace(tries=3, backoff=(1, 5), timeout=None, fail_on_timeout=None)
        return {
            "emails.send": SimpleNamespace(queue="emails", policy=policy),
            "reports.build": SimpleNamespace(
                queue=None, policy=SimpleNamespace(
                    tries=None, backoff=None, timeout=None, fail_on_timeout=None
                ),
            ),
        }

    @asynccontextmanager
    async def lifespan(self):
        yield


sqs = Registry("sqs")
redis = Registry("redis")
managed = Registry("managed")
"""


@pytest.fixture
def app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    name = f"lcq_cli_app_{uuid.uuid4().hex}"
    (tmp_path / f"{name}.py").write_text(APP)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    yield name
    sys.modules.pop(name, None)


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, WorkerOptions]]:
    runs: list[tuple[Any, WorkerOptions]] = []

    class FakeWorker:
        def __init__(self, target: Any, options: WorkerOptions) -> None:
            runs.append((target, options))

        def run(self) -> int:
            return 7

    monkeypatch.setattr(cli, "Worker", FakeWorker)
    return runs


def test_work_puts_cwd_on_path_and_maps_options(
    app: str, captured: list[tuple[Any, WorkerOptions]]
) -> None:
    code = cli.main(
        [
            "work",
            f"{app}:sqs",
            "--queue",
            "high, low,",
            "--max-jobs",
            "5",
            "--max-time",
            "60",
            "--stop-when-empty-for",
            "9",
            "--timeout",
            "0",
            "--sleep",
            "1.5",
            "--rest",
            "0.25",
        ]
    )
    assert code == 7
    [(target, options)] = captured
    assert target is sys.modules[app].sqs
    assert options == WorkerOptions(
        queues=("high", "low"),
        max_jobs=5,
        max_time=60.0,
        stop_when_empty=False,
        stop_when_empty_for=9.0,
        timeout=0.0,
        sleep=1.5,
        rest=0.25,
    )


def test_work_defaults(app: str, captured: list[tuple[Any, WorkerOptions]]) -> None:
    cli.main(["work", f"{app}:sqs", "--stop-when-empty"])
    assert captured[0][1] == WorkerOptions(stop_when_empty=True)


@pytest.mark.parametrize(
    "argv",
    [
        ["work", "x:y", "--max-jobs", "0"],
        ["work", "x:y", "--sleep", "-1"],
        ["work", "x:y", "--timeout", "nan"],
        ["work", "x:y", "--queue", ","],
        ["work"],
        [],
    ],
)
def test_invalid_arguments_exit_2(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(argv)
    assert raised.value.code == 2


def test_bad_target_is_one_actionable_line(app: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["work", f"{app}:missing"]) == 2
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith("laravel-cloud-queues: error:") and "missing" in err
    assert "Traceback" not in err


def test_debug_adds_traceback(app: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["work", f"{app}:missing", "--debug"]) == 2
    assert "Traceback" in capsys.readouterr().err


def test_unexpected_error_exits_1(
    app: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class Boom:
        def __init__(self, *args: object) -> None:
            pass

        def run(self) -> int:
            raise RuntimeError("socket exploded")

    monkeypatch.setattr(cli, "Worker", Boom)
    assert cli.main(["work", f"{app}:sqs"]) == 1
    assert "RuntimeError: socket exploded (use --debug" in capsys.readouterr().err


SECRETS = ("TOPSECRET", "SESSIONTOKEN", "REDISPASS", "AKIDKEY", "user:")


@pytest.mark.parametrize("attr", ["sqs", "redis", "managed"])
def test_inspect_never_prints_secrets(
    app: str, attr: str, capsys: pytest.CaptureFixture[str]
) -> None:
    for flag in ([], ["--json"]):
        assert cli.main(["inspect", f"{app}:{attr}", *flag]) == 0
        out = capsys.readouterr().out
        for secret in SECRETS:
            assert secret not in out


def test_inspect_json(app: str, capsys: pytest.CaptureFixture[str]) -> None:
    cli.main(["inspect", f"{app}:managed", "--json"])
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "managed"
    assert report["queues"] == {
        "default": "emails",
        "worker_assignment": "assigned",
        "managed_inventory": ["assigned", "emails"],
    }
    assert report["jobs"][0] == {
        "name": "emails.send",
        "queue": "emails",
        "tries": 3,
        "backoff": [1, 5],
        "timeout": None,
        "fail_on_timeout": None,
    }
    assert report["settings"]["agent_enabled"] is True
    assert report["settings"]["sqs_credentials"] == "static"
    assert sys.modules[app].managed.loaded == 1


def test_inspect_text(app: str, capsys: pytest.CaptureFixture[str]) -> None:
    cli.main(["inspect", f"{app}:redis"])
    out = capsys.readouterr().out
    assert "Mode: redis" in out
    assert "emails.send (queue=emails, tries=3, backoff=1,5)" in out
    assert "reports.build\n" in out
    assert "redis_url: rediss://cache.test:6380/2" in out


def test_inspect_config_error_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name = f"lcq_cli_bad_{uuid.uuid4().hex}"
    (tmp_path / f"{name}.py").write_text(
        "from laravel_cloud_queues.errors import ConfigurationError\n"
        "class R:\n"
        "    registry = property(lambda self: self)\n"
        "    def lifespan(self): ...\n"
        "    def load(self): pass\n"
        "    @property\n"
        "    def config(self): raise ConfigurationError('Set LARAVEL_CLOUD_QUEUES_BACKEND.')\n"
        "r = R()\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    try:
        assert cli.main(["inspect", f"{name}:r"]) == 2
    finally:
        sys.modules.pop(name, None)


def test_conformance_without_checkout_explains_and_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli.main(["conformance", "--json"]) == 2
    assert "repository checkout" in capsys.readouterr().err


def test_conformance_delegates_from_a_subdirectory_of_the_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    suite = tmp_path / "demo" / "conformance"
    suite.mkdir(parents=True)
    (tmp_path / "demo" / "__init__.py").write_text("")
    (suite / "__init__.py").write_text("")
    (suite / "__main__.py").write_text(
        "import json, os, sys\n"
        "open('ran.json', 'w').write(json.dumps([os.getcwd(), sys.argv[1:]]))\n"
        "sys.exit(3)\n"
    )
    nested = tmp_path / "some" / "where"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert cli.main(["conformance", "--json", "--only", "timeouts"]) == 3
    cwd, args = json.loads((tmp_path / "ran.json").read_text())
    assert Path(cwd).resolve() == tmp_path.resolve()
    assert args == ["--json", "--only", "timeouts"]


def test_module_entry_point() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "laravel_cloud_queues.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    for command in ("work", "inspect", "conformance"):
        assert command in result.stdout
