"""Checked-in event fixtures stay aligned with the builders and upstream citations."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from laravel_cloud_queues.observability import lifecycle_event

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "events"
_TS = datetime(2026, 9, 27, 12, 0, 0, 123456, tzinfo=timezone.utc)
_FAILED_JOB_KEYS = [
    "_cloud_event",
    "id",
    "queue",
    "started_at",
    "attempts",
    "payload",
    "exception_preview",
    "job_name",
    "exception",
]


def _load(name: str) -> dict[str, object]:
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def test_lifecycle_fixtures_match_the_builder() -> None:
    expectations = {
        "queued.json": ("queued", False),
        "started.json": ("started", False),
        "processed.json": ("processed", True),
        "released.json": ("released", True),
        "failed.json": ("failed", True),
    }
    for name, (event_type, with_duration) in expectations.items():
        fixture = _load(name)
        built = lifecycle_event(
            event_type,
            "emails",
            timestamp=_TS,
            duration_ms=42 if with_duration else None,
        )
        assert fixture == built
        assert list(fixture) == list(built)


def test_failed_job_fixtures_cover_the_three_d1_branches() -> None:
    full = _load("failed_job.json")
    trimmed = _load("failed_job_exception_trimmed.json")
    withheld = _load("failed_job_not_replayable.json")
    assert list(full) == _FAILED_JOB_KEYS
    assert "replayable" not in full
    assert "[truncated " not in str(full["exception"])
    assert list(trimmed) == _FAILED_JOB_KEYS
    assert "\n... [truncated " in str(trimmed["exception"])
    assert trimmed["payload"] == full["payload"]
    assert "replayable" not in trimmed
    assert list(withheld) == [*_FAILED_JOB_KEYS, "replayable"]
    assert withheld["replayable"] is False
    assert str(full["payload"]).startswith(str(withheld["payload"]))
    assert withheld["payload"] != full["payload"]
    assert "\n... [truncated " in str(withheld["exception"])


def test_fixture_readme_cites_upstream_lines() -> None:
    text = (_FIXTURES / "README.md").read_text(encoding="utf-8")
    for citation in (
        "Queue.php:520-525",
        "Queue.php:545-550",
        "Queue.php:481-491",
        "FailedJobProvider.php:70-87",
        "FailedJobProvider.php:77-84",
        "FailedJobProvider.php:72",
        "Events.php:124",
        "Events.php:77-107",
        "QueueEventSubscriber.php:50-60",
        "QueueEventSubscriber.php:202-219",
    ):
        assert citation in text
