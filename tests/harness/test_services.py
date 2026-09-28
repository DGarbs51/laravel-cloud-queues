from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from harness.agent_emulator import AgentEmulator
from harness.process import Process
from harness.pytest_plugin import ProcessFactory
from harness.redis import cleanup, connect, redis_service, unique_prefix
from harness.sqs import SQSEndpoint


@pytest.mark.sqs
@pytest.mark.parametrize("fifo", [False, True])
def test_sqs_http_endpoint(sqs_endpoint: SQSEndpoint, fifo: bool) -> None:
    queue = sqs_endpoint.create_queue(fifo=fifo)
    extra = {"MessageGroupId": "test"} if fifo else {}
    sqs_endpoint.client.send_message(QueueUrl=queue, MessageBody="hello", **extra)
    received = sqs_endpoint.client.receive_message(QueueUrl=queue)["Messages"][0]
    assert received["Body"] == "hello"
    assert queue.endswith(".fifo") == fifo
    sqs_endpoint.cleanup()
    queues = sqs_endpoint.client.list_queues(QueueNamePrefix=sqs_endpoint.prefix)
    assert not queues.get("QueueUrls")


@pytest.mark.sqs
@pytest.mark.subprocess
def test_sqs_reachable_from_subprocess(
    sqs_endpoint: SQSEndpoint, run_process: ProcessFactory
) -> None:
    queue = sqs_endpoint.create_queue()
    code = """
import boto3, sys
client = boto3.client('sqs', endpoint_url=sys.argv[1], region_name='us-east-1',
                      aws_access_key_id='testing', aws_secret_access_key='testing',
                      aws_session_token='testing')
client.send_message(QueueUrl=sys.argv[2], MessageBody='child')
"""
    result = run_process([sys.executable, "-c", code, sqs_endpoint.url, queue]).wait()
    assert result.returncode == 0, result.stderr
    assert sqs_endpoint.client.receive_message(QueueUrl=queue)["Messages"][0]["Body"] == "child"


@pytest.mark.sqs
def test_sqs_ignores_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    from harness.sqs import sqs_endpoint

    for key, value in {
        "AWS_PROFILE": "nonexistent-lcq-profile",
        "AWS_ACCESS_KEY_ID": "wrong",
        "AWS_SECRET_ACCESS_KEY": "wrong",
        "AWS_SESSION_TOKEN": "wrong",
        "AWS_DEFAULT_REGION": "not-a-region",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:1",
    }.items():
        monkeypatch.setenv(key, value)
    with sqs_endpoint() as endpoint:
        assert endpoint.create_queue()
        credentials = endpoint.client._request_signer._credentials
        assert credentials.access_key == "testing"
        assert credentials.token == "testing"


@pytest.mark.redis
def test_redis_prefix_cleanup_preserves_other_keys(redis_url: str, redis_prefix: str) -> None:
    client = connect(redis_url)
    other = unique_prefix() + "keep"
    try:
        client.set(other, "keep")
        with redis_service(redis_url) as (scoped, prefix):
            for index in range(205):
                scoped.set(prefix + str(index), "value")
            scoped.set(redis_prefix + "fixture", "value")
        assert not list(client.scan_iter(match=prefix + "*"))
        assert client.get(other) == b"keep"
        for prefix in ("", "*", "lcq:?"):
            with pytest.raises(ValueError):
                cleanup(client, prefix)
    finally:
        client.delete(other)
        client.close()


@pytest.mark.subprocess
def test_process_output_env_timeout_signals_and_teardown(tmp_path: Path) -> None:
    code = "import os, sys; print(os.environ['LCQ_TEST']); print('error', file=sys.stderr)"
    with Process(
        [sys.executable, "-c", code], env={"LCQ_TEST": "value"}, output_dir=tmp_path
    ) as child:
        result = child.wait()
        assert result.returncode == 0
        assert result.stdout == "value\n"
        assert result.stderr == "error\n"
    assert child.stdout_path.read_text() == "value\n"
    with Process([sys.executable, "-c", "import time; time.sleep(60)"]) as child:
        with pytest.raises(subprocess.TimeoutExpired):
            child.wait(timeout=0.02)
        child.send_signal(signal.SIGTERM)
        assert child.wait().returncode == -signal.SIGTERM
    with Process([sys.executable, "-c", "import time; time.sleep(60)"]) as killed:
        pass
    assert killed.process.poll() == -signal.SIGKILL


@pytest.mark.subprocess
def test_agent_module_cli(run_process: ProcessFactory) -> None:
    # Reserve a short private directory without binding its socket.
    emu = AgentEmulator()
    try:
        child = run_process(
            [sys.executable, "-m", "harness.agent_emulator", "--socket", emu.socket_path]
        )
        import time

        deadline = time.monotonic() + 5
        while not Path(emu.socket_path).exists() and time.monotonic() < deadline:
            assert child.process.poll() is None
            time.sleep(0.01)
        assert Path(emu.socket_path).exists()
        child.send_signal(signal.SIGTERM)
        result = child.wait()
        assert result.returncode == 0
        assert "listening" in result.stdout
        assert not Path(emu.socket_path).exists()
    finally:
        emu.close()


@pytest.mark.subprocess
@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("service", ["redis_url", "sqs_endpoint"])
def test_unavailable_service_skip_or_fail(
    tmp_path: Path,
    run_process: ProcessFactory,
    required: bool,
    service: str,
) -> None:
    test = tmp_path / "test_gate.py"
    test.write_text(f"def test_service({service}):\n    pass\n")
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]  # Bound but not listening: guaranteed refusal.
        result = run_process(
            [sys.executable, "-m", "pytest", "-p", "harness.pytest_plugin", "-q", str(test)],
            env={
                "LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL": f"redis://127.0.0.1:{port}/15",
                "LARAVEL_CLOUD_QUEUES_TEST_SQS": "localstack",
                "LARAVEL_CLOUD_QUEUES_TEST_SQS_ENDPOINT": f"http://127.0.0.1:{port}",
                "LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES": "1" if required else "0",
                "PYTHONPATH": os.getcwd(),
            },
        ).wait(timeout=10)
    assert result.returncode == (1 if required else 0), result.stdout + result.stderr
    assert ("error" if required else "skipped") in result.stdout
