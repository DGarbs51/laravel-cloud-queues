"""Offline unit tests for .github/scripts/upstream_drift.py (PROJECT_SCOPE.md §2).

Exercises only the pure parsing/comparison functions with canned GitHub API JSON.
No network access; ``github_get``/``fetch_*`` are not called here.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.packaging

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / ".github" / "scripts" / "upstream_drift.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("upstream_drift", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


drift = _load_module()


BASELINE = drift.DEFAULT_BASELINE


def test_load_baseline_without_catalog_uses_defaults(tmp_path: Path) -> None:
    assert drift.load_baseline(tmp_path / "missing.json") == BASELINE


def test_load_baseline_reads_catalog_override(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"baseline": {"framework_tag": "v13.34.0"}}))
    baseline = drift.load_baseline(catalog)
    assert baseline["framework_tag"] == "v13.34.0"
    assert baseline["framework_repo"] == BASELINE["framework_repo"]


def test_load_baseline_ignores_malformed_catalog(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.json"
    catalog.write_text("not json")
    assert drift.load_baseline(catalog) == BASELINE


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v13.33.0", (13, 33, 0)),
        ("v13.34.1", (13, 34, 1)),
        ("v12.9.0", None),
        ("v14.0.0", None),
        ("13.33.0", None),
        ("v13.33", None),
        ("not-a-tag", None),
    ],
)
def test_parse_v13_tag(tag: str, expected: tuple[int, int, int] | None) -> None:
    assert drift.parse_v13_tag(tag) == expected


def test_newest_v13_tag_picks_highest_version() -> None:
    tags = [
        {"name": "v13.33.0", "commit": {"sha": "aaa"}},
        {"name": "v13.34.0", "commit": {"sha": "bbb"}},
        {"name": "v13.9.0", "commit": {"sha": "ccc"}},
        {"name": "v12.99.0", "commit": {"sha": "ddd"}},
        {"name": "v14.0.0", "commit": {"sha": "eee"}},
    ]
    newest = drift.newest_v13_tag(tags)
    assert newest is not None
    assert newest["name"] == "v13.34.0"


def test_newest_v13_tag_none_when_no_match() -> None:
    tags = [
        {"name": "v12.9.0", "commit": {"sha": "a"}},
        {"name": "v14.0.0", "commit": {"sha": "b"}},
    ]
    assert drift.newest_v13_tag(tags) is None


def test_newest_v13_tag_handles_empty_and_malformed_entries() -> None:
    assert drift.newest_v13_tag([]) is None
    assert drift.newest_v13_tag([{}, {"name": None}]) is None


def test_filter_watched_files_matches_prefixes() -> None:
    files = [
        {"filename": "src/Illuminate/Queue/SqsQueue.php"},
        {"filename": "src/Illuminate/Queue/RedisQueue.php"},
        {"filename": "src/Illuminate/Foundation/Cloud/Queue.php"},
        {"filename": "src/Illuminate/Http/Request.php"},
        {"filename": "tests/Queue/SqsQueueTest.php"},
    ]
    matched = drift.filter_watched_files(files, drift.WATCHED_FRAMEWORK_PREFIXES)
    assert matched == [
        "src/Illuminate/Foundation/Cloud/Queue.php",
        "src/Illuminate/Queue/RedisQueue.php",
        "src/Illuminate/Queue/SqsQueue.php",
    ]


def test_filter_watched_files_symfony_prefixes() -> None:
    files = [
        {"filename": "src/Queue/Agent/AgentClient.php"},
        {"filename": "src/Observability/Events.php"},
        {"filename": "README.md"},
        {"filename": "tests/Queue/AgentClientTest.php"},
    ]
    matched = drift.filter_watched_files(files, drift.WATCHED_SYMFONY_PREFIXES)
    assert matched == ["src/Observability/Events.php", "src/Queue/Agent/AgentClient.php"]


def test_filter_watched_files_ignores_malformed_entries() -> None:
    files = [{"filename": None}, {}, {"filename": "src/Queue/Agent/AgentClient.php"}]
    assert drift.filter_watched_files(files, drift.WATCHED_SYMFONY_PREFIXES) == [
        "src/Queue/Agent/AgentClient.php"
    ]


def test_build_report_baseline_current() -> None:
    report = drift.build_report(
        BASELINE,
        framework_newest={"name": "v13.33.0", "commit": {"sha": BASELINE["framework_sha"]}},
        framework_changed_files=[],
        symfony_changed_files=[],
        symfony_commit_count=0,
        errors=[],
    )
    assert "Baseline `v13.33.0` is the newest" in report
    assert "No new commits on the default branch" in report


def test_build_report_notes_drift_and_errors() -> None:
    report = drift.build_report(
        BASELINE,
        framework_newest={"name": "v13.34.0", "commit": {"sha": "bbbbbbbbbbbb"}},
        framework_changed_files=["src/Illuminate/Queue/SqsQueue.php"],
        symfony_changed_files=["src/Queue/Agent/AgentClient.php"],
        symfony_commit_count=3,
        errors=["laravel/framework tag lookup failed: boom"],
    )
    assert "## Errors" in report
    assert "laravel/framework tag lookup failed: boom" in report
    assert "Newer tag available: `v13.34.0`" in report
    assert "src/Illuminate/Queue/SqsQueue.php" in report
    assert "3 new commit(s)" in report
    assert "src/Queue/Agent/AgentClient.php" in report


def test_warnings_for_report_empty_when_no_drift() -> None:
    assert (
        drift.warnings_for_report(
            {"name": "v13.33.0", "commit": {"sha": BASELINE["framework_sha"]}}, BASELINE, [], []
        )
        == []
    )


def test_warnings_for_report_flags_watched_changes() -> None:
    warnings = drift.warnings_for_report(
        {"name": "v13.34.0", "commit": {"sha": "bbb"}},
        BASELINE,
        ["src/Illuminate/Queue/SqsQueue.php"],
        ["src/Queue/Agent/AgentClient.php"],
    )
    assert len(warnings) == 2
    assert "review before" in warnings[0]
    assert "symfony-on-cloud has 1 changed watched file" in warnings[1]


def test_warnings_for_report_newer_tag_no_watched_changes() -> None:
    warnings = drift.warnings_for_report(
        {"name": "v13.34.0", "commit": {"sha": "bbb"}}, BASELINE, [], []
    )
    assert len(warnings) == 1
    assert "no watched paths changed" in warnings[0]
