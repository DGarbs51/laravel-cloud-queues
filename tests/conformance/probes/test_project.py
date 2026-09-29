"""Repository quality and explicitly pending platform checks."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conformance import ROOT


@pytest.mark.conformance("cloud.live_managed", tier="live")
def test_live_managed():
    pytest.skip("Laravel Cloud has not enabled managed queues for Python")


@pytest.mark.conformance("cloud.dashboard_retry_live", tier="live")
def test_dashboard_retry_live():
    pytest.skip("D3: live dashboard retry awaits Python managed queues")


@pytest.mark.conformance("packaging.typing_strict", tier="emulated")
def test_typing(evidence):
    result = subprocess.run(
        [sys.executable, "-m", "ty", "check", "--python", sys.executable],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    evidence.record("ty", result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    samples = list((ROOT / "tests/typing").glob("test_*.py"))
    assert samples, "Downstream typing tests have not landed"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/typing", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    evidence.record("downstream_typing", result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.conformance("ci.full_gate", tier="emulated")
def test_ci_gate(evidence):
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    for required in (
        "branches: [main]",
        "pull_request:",
        '"3.10"',
        '"3.11"',
        '"3.12"',
        '"3.13"',
        '"3.14"',
        "uv run ty check",
        "uv run mypy",
        "uv run pyright",
        "coverage run -m pytest",
        "uv run coverage combine",
        "ruff check",
        "--sqs localstack",
        "conformance-artifacts",
        "compatibility-report.json",
        "redis:",
        "valkey:",
        "LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES",
        "conformance:",
    ):
        assert required in workflow, required
    assert "needs: [lint, typecheck, docs, test, coverage, packaging, conformance]" in workflow
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "src", "tests"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    evidence.record("lint", result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "format", "--check", "src", "tests"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    evidence.record("format", result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    evidence.record("github_actions", os.environ.get("GITHUB_ACTIONS") == "true")
    evidence.observed(
        "Executed local quality tools and checked CI wiring; remote matrix result is external"
    )


@pytest.mark.conformance("docs.readme", tier="unit")
def test_readme():
    text = (ROOT / "README.md").read_text().lower()
    for term in (
        "install",
        "fastapi",
        "depends",
        "dispatch",
        "work",
        "queue",
        "delay",
        "retry",
        "fifo",
        "fair",
        "jobcontext",
        "localstack",
        "eager",
        "conformance",
        "managed",
        "redis",
        "aws_",
        "3.10",
        "limitation",
        "roadmap",
        "tests/conformance",
    ):
        assert term in text, f"README missing required topic: {term}"
    assert Path(ROOT / "tests/conformance/README.md").is_file()


@pytest.mark.conformance("cli.commands_and_exit_codes", tier="emulated")
@pytest.mark.parametrize(
    ("target", "job"),
    [
        ("tests.integration.worker.apps.basic:registry", "sync_ok"),
        ("tests.conformance.app:app", "demo.sync"),
    ],
    ids=["registry", "fastapi"],
)
def test_cli(run_process, artifacts, evidence, target, job):
    from .support import environment

    env = environment(artifacts)
    env.update(
        LARAVEL_CLOUD_QUEUES_BACKEND="redis",
        LARAVEL_CLOUD_QUEUES_REDIS_URL="redis://127.0.0.1:1/15",
    )
    # A dead broker endpoint proves inspect does not open connections.
    result = run_process(
        [sys.executable, "-m", "laravel_cloud_queues.cli", "inspect", target, "--json"],
        env=env,
        cwd=ROOT,
        output_dir=artifacts,
    ).wait(10)
    assert result.returncode == 0, result.stderr
    assert job in result.stdout
    invalid = run_process(
        [sys.executable, "-m", "laravel_cloud_queues.cli", "work", target],
        env=environment(artifacts),
        cwd=ROOT,
        output_dir=artifacts,
    ).wait(10)
    assert invalid.returncode == 2
    evidence.record("inspect_exit", result.returncode)
    evidence.record("invalid_config_exit", invalid.returncode)
