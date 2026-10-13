"""Prove the work command passes memory options and recycling exit codes through."""

from __future__ import annotations

import pytest

import laravel_cloud_queues.cli as cli
from tests.unit.cli.test_cli import app as app
from tests.unit.cli.test_cli import captured as captured


@pytest.mark.parametrize("value", ["64", "90%", "0"])
def test_work_maps_memory_option(app, captured, value):
    assert cli.main(["work", f"{app}:sqs", "--memory", value]) == 7
    assert captured[0][1].memory == value


def test_work_without_memory_leaves_resolution_to_worker(app, captured, monkeypatch):
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_MEMORY", "90%")
    cli.main(["work", f"{app}:managed"])
    assert captured[0][1].memory is None


def test_work_returns_memory_recycling_exit_code(app, monkeypatch):
    class Recycling:
        def __init__(self, target, options):
            assert options.memory == "64"

        def run(self):
            return 12

    monkeypatch.setattr(cli, "Worker", Recycling)
    assert cli.main(["work", f"{app}:sqs", "--memory", "64"]) == 12
