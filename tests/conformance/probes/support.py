from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from harness.agent_emulator import AgentEmulator
from harness.log_collector import LogCollector
from harness.process import Process
from harness.pytest_plugin import ProcessFactory
from harness.sqs import SQSEndpoint
from tests.conformance import ROOT
from tests.conformance.plugin import Evidence, safe


def environment(artifacts: Path) -> dict[str, str | None]:
    env: dict[str, str | None] = {
        key: None
        for key in os.environ
        if key.startswith("LARAVEL_CLOUD_QUEUES_")
        or key
        in {
            "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG",
            "LARAVEL_CLOUD_LOG_SOCKET",
            "LARAVEL_CLOUD_AGENT_SOCKET",
        }
    }
    env.update(PYTHONUNBUFFERED="1", LCQ_DEMO_EVENTS=str(artifacts / "worker.jsonl"))
    return env


def managed_env(
    artifacts: Path, agent: AgentEmulator, collector: LogCollector
) -> dict[str, str | None]:
    env = environment(artifacts)
    env.update(
        LARAVEL_CLOUD_QUEUES_BACKEND="managed",
        LARAVEL_CLOUD_LOG_SOCKET=collector.socket_path,
        LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG=json.dumps(
            {
                "driver": "cloud",
                "queue": "default",
                "connection": {
                    "prefix": "https://sqs.us-east-1.amazonaws.com/123456789012",
                    "region": "us-east-1",
                    "credentials": "ecs",
                    "queue": "default",
                },
                "agent": {"enabled": True, "socket": agent.socket_path},
            }
        ),
    )
    return env


def sqs_env(artifacts: Path, endpoint: SQSEndpoint, queue_url: str) -> dict[str, str | None]:
    env = environment(artifacts)
    prefix, queue = queue_url.rsplit("/", 1)
    env.update(
        LARAVEL_CLOUD_QUEUES_BACKEND="sqs",
        LARAVEL_CLOUD_QUEUES_SQS_PREFIX=prefix,
        LARAVEL_CLOUD_QUEUES_SQS_QUEUE=queue,
        LARAVEL_CLOUD_QUEUES_SQS_REGION=endpoint.region,
        LARAVEL_CLOUD_QUEUES_SQS_KEY=endpoint.access_key,
        LARAVEL_CLOUD_QUEUES_SQS_SECRET=endpoint.secret_key,
        LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT=endpoint.url,
    )
    return env


def envelope(
    job: str = "demo.sync",
    *,
    kwargs: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> str:
    """Real wire fixture; managed producer cannot use a local _SQS_ENDPOINT (D6a)."""
    return json.dumps(
        {
            "uuid": str(uuid4()),
            "displayName": job,
            "laravel_cloud_queues": {
                "version": 1,
                "job": job,
                "args": [],
                "kwargs": kwargs or {"label": job},
                "policy": policy or {},
                "context": {"traceparent": f"00-{trace_id}-0123456789abcdef-01"}
                if trace_id
                else {},
            },
        }
    )


def worker(
    run: ProcessFactory, artifacts: Path, env: dict[str, str | None], *options: str
) -> Process:
    return run(
        [
            sys.executable,
            "-m",
            "laravel_cloud_queues.cli",
            "work",
            "tests.conformance.app:app",
            "--sleep",
            "0.05",
            *options,
        ],
        env=env,
        cwd=ROOT,
        output_dir=artifacts,
    )


def produce(
    run: ProcessFactory,
    artifacts: Path,
    env: dict[str, str | None],
    job: str = "demo.sync",
    *,
    kwargs: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    process = run(
        [
            sys.executable,
            "-m",
            "tests.conformance.produce",
            job,
            "--kwargs",
            json.dumps(kwargs or {"label": job}),
            "--options",
            json.dumps(options or {}),
        ],
        cwd=ROOT,
        output_dir=artifacts,
        env={**env, "LCQ_DEMO_EVENTS": str(artifacts / "producer.jsonl")},
    )
    result = process.wait(15)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def rows(artifacts: Path, kind: str | None = None) -> list[dict[str, Any]]:
    path = artifacts / "worker.jsonl"
    values = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    return [row for row in values if kind is None or row["kind"] == kind]


def until(predicate: Any, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "Timed out waiting for probe observation"
        time.sleep(0.02)


def capture(
    evidence: Evidence,
    artifacts: Path,
    collector: LogCollector | None = None,
    agent: AgentEmulator | None = None,
) -> None:
    evidence.record("worker_observations", rows(artifacts))
    evidence.record("artifacts", str(artifacts.relative_to(ROOT)))
    if collector is not None:
        events = []
        for event in collector.events:
            value = dict(event)
            if "payload" in value:
                payload = str(value.pop("payload"))
                value["payload_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
                value["payload_bytes"] = len(payload.encode())
            events.append(safe(value))
        (artifacts / "collector.json").write_text(json.dumps(events, indent=2) + "\n")
        evidence.record("collector", events)
    if agent is not None:
        evidence.record(
            "outcomes",
            [
                {key: value for key, value in result.body.items() if key != "receiptHandle"}
                for result in agent.results
            ],
        )
