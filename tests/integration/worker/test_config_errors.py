"""Startup configuration errors exit 2 with one clear line on every start (D8, §13)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.integration.worker.support import Workers, clean_env

pytestmark = pytest.mark.subprocess


def test_missing_backend_exits_2_with_a_clear_message(run_process: Any, tmp_path: Path) -> None:
    workers = Workers(run_process, clean_env({}), tmp_path / "records.jsonl")
    for _ in range(2):
        run = workers.run("--max-jobs", "1", timeout=30)
        assert run.code == 2, run.describe()
        assert "LARAVEL_CLOUD_QUEUES_BACKEND" in run.result.stderr
        assert "Traceback" not in run.result.stderr


def test_unknown_target_exits_2(run_process: Any, tmp_path: Path) -> None:
    workers = Workers(run_process, clean_env({}), tmp_path / "records.jsonl")
    process = workers.start(target="tests.integration.worker.apps.missing:registry")
    run = workers.wait(process, timeout=30)
    assert run.code == 2, run.describe()
    assert run.result.stderr.count("\n") == 1
