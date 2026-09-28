#!/usr/bin/env python3
"""Weekly advisory upstream drift check (PROJECT_SCOPE.md §2, decision D11).

Stdlib only. Compares the pinned compatibility baseline (laravel/framework and
laravel/symfony-on-cloud, see docs/decisions.md D13 and docs/references.md) against
upstream's current state and reports drift. Never fails the build and never edits the
pin: it writes a Markdown summary (``$GITHUB_STEP_SUMMARY``) and ``::warning::``
annotations, then always exits 0.

The network-fetching functions (``fetch_json``, ``github_get``) are kept separate from
the pure parsing/comparison functions below so the latter can be unit tested offline
with canned API JSON (see tests/packaging/test_drift_script.py).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = REPO_ROOT / "docs" / "contract" / "catalog.json"

GITHUB_API = "https://api.github.com"

# Fallback baseline (docs/decisions.md D13, docs/references.md). Overridden by
# docs/contract/catalog.json's "baseline" key when that file exists and has it.
DEFAULT_BASELINE = {
    "framework_repo": "laravel/framework",
    "framework_tag": "v13.33.0",
    "framework_sha": "91188a17ceaa3dbace6e8a5f7abd0d042e466359",
    "symfony_repo": "laravel/symfony-on-cloud",
    "symfony_sha": "50c945170b6cb5690370d15fd725c6f82495e9ba",
}

# PROJECT_SCOPE.md §2 "Primary Laravel Framework references" plus the Redis/Valkey
# files this project also depends on for compatibility (Foundation/Cloud/* covers the
# whole directory).
WATCHED_FRAMEWORK_PREFIXES = (
    "src/Illuminate/Foundation/Cloud/",
    "src/Illuminate/Foundation/CloudBootstrapper.php",
    "src/Illuminate/Queue/SqsQueue.php",
    "src/Illuminate/Queue/Worker.php",
    "src/Illuminate/Queue/WorkerOptions.php",
    "src/Illuminate/Queue/Queue.php",
    "src/Illuminate/Queue/RedisQueue.php",
    "src/Illuminate/Queue/LuaScripts.php",
    "src/Illuminate/Queue/Jobs/Job.php",
    "src/Illuminate/Queue/Jobs/SqsJob.php",
    "src/Illuminate/Queue/Jobs/RedisJob.php",
    "src/Illuminate/Queue/Connectors/SqsConnector.php",
)

# PROJECT_SCOPE.md §2 "Secondary cross-framework reference".
WATCHED_SYMFONY_PREFIXES = (
    "src/Queue",
    "src/Observability",
)

_SEMVER_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def load_baseline(catalog_path: Path = CATALOG_PATH) -> dict[str, str]:
    """Read the pinned baseline from the conformance catalog if present, else fall
    back to the constants above (docs/decisions.md D13)."""
    if catalog_path.is_file():
        try:
            data = json.loads(catalog_path.read_text())
        except (OSError, json.JSONDecodeError):
            return dict(DEFAULT_BASELINE)
        baseline = data.get("baseline")
        if isinstance(baseline, dict) and baseline:
            merged = dict(DEFAULT_BASELINE)
            merged.update({k: v for k, v in baseline.items() if isinstance(v, str)})
            return merged
    return dict(DEFAULT_BASELINE)


def parse_v13_tag(tag_name: str) -> tuple[int, int, int] | None:
    """Return (major, minor, patch) for a ``v13.x.y`` tag, else None."""
    match = _SEMVER_TAG_RE.match(tag_name)
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    if major != 13:
        return None
    return (major, minor, patch)


def newest_v13_tag(tags: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Given GitHub's ``/repos/{repo}/tags`` JSON, return the tag entry with the
    highest ``v13.x.y`` version, or None if none is present."""
    best: tuple[tuple[int, int, int], dict[str, Any]] | None = None
    for entry in tags:
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        version = parse_v13_tag(name)
        if version is None:
            continue
        if best is None or version > best[0]:
            best = (version, entry)
    return best[1] if best is not None else None


def filter_watched_files(files: list[dict[str, Any]], prefixes: tuple[str, ...]) -> list[str]:
    """Given GitHub's compare-API ``files`` list, return filenames under any of the
    watched prefixes, sorted and deduplicated."""
    matched = {
        f["filename"]
        for f in files
        if isinstance(f.get("filename"), str) and f["filename"].startswith(prefixes)
    }
    return sorted(matched)


def build_report(
    baseline: dict[str, str],
    framework_newest: dict[str, Any] | None,
    framework_changed_files: list[str],
    symfony_changed_files: list[str],
    symfony_commit_count: int,
    errors: list[str],
) -> str:
    lines = ["# Upstream drift report", ""]
    lines.append(
        f"Pinned baseline: `{baseline['framework_repo']}@{baseline['framework_tag']}` "
        f"(`{baseline['framework_sha'][:12]}`), "
        f"`{baseline['symfony_repo']}@{baseline['symfony_sha'][:12]}`."
    )
    lines.append("")

    if errors:
        lines.append("## Errors")
        lines.extend(f"- {e}" for e in errors)
        lines.append("")

    lines.append("## laravel/framework")
    if framework_newest is None:
        lines.append("No newer `v13.*` tag found (or lookup failed); baseline is current.")
    else:
        newest_tag = framework_newest.get("name")
        newest_sha = framework_newest.get("commit", {}).get("sha", "")
        if newest_tag == baseline["framework_tag"]:
            lines.append(f"Baseline `{baseline['framework_tag']}` is the newest `v13.*` tag.")
        else:
            lines.append(f"Newer tag available: `{newest_tag}` (`{newest_sha[:12]}`).")
            if framework_changed_files:
                lines.append("")
                lines.append("Changed files under watched paths:")
                lines.extend(f"- `{f}`" for f in framework_changed_files)
            else:
                lines.append("No watched paths changed between the baseline and this tag.")
    lines.append("")

    lines.append("## laravel/symfony-on-cloud")
    if symfony_commit_count == 0:
        lines.append("No new commits on the default branch since the pinned baseline.")
    else:
        lines.append(
            f"{symfony_commit_count} new commit(s) on the default branch since the baseline."
        )
        if symfony_changed_files:
            lines.append("")
            lines.append("Changed files under watched paths:")
            lines.extend(f"- `{f}`" for f in symfony_changed_files)
        else:
            lines.append("No watched paths changed.")
    lines.append("")

    return "\n".join(lines)


def warnings_for_report(
    framework_newest: dict[str, Any] | None,
    baseline: dict[str, str],
    framework_changed_files: list[str],
    symfony_changed_files: list[str],
) -> list[str]:
    warnings = []
    if framework_newest is not None and framework_newest.get("name") != baseline["framework_tag"]:
        if framework_changed_files:
            warnings.append(
                f"laravel/framework has a newer tag ({framework_newest.get('name')}) with "
                f"changes to {len(framework_changed_files)} watched file(s); review before "
                "updating the pin."
            )
        else:
            warnings.append(
                f"laravel/framework has a newer tag ({framework_newest.get('name')}) but no "
                "watched paths changed."
            )
    if symfony_changed_files:
        warnings.append(
            f"laravel/symfony-on-cloud has {len(symfony_changed_files)} changed watched file(s) "
            "since the pinned baseline."
        )
    return warnings


# --- Network I/O (not exercised by offline unit tests) ---------------------------


def github_get(path: str, token: str | None) -> Any:
    request = urllib.request.Request(f"{GITHUB_API}{path}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def fetch_framework_drift(
    baseline: dict[str, str], token: str | None, errors: list[str]
) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        tags = github_get(f"/repos/{baseline['framework_repo']}/tags?per_page=100", token)
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        errors.append(f"laravel/framework tag lookup failed: {exc}")
        return None, []

    newest = newest_v13_tag(tags)
    if newest is None or newest.get("name") == baseline["framework_tag"]:
        return newest, []

    newest_sha = newest.get("commit", {}).get("sha", "")
    try:
        compare = github_get(
            f"/repos/{baseline['framework_repo']}/compare/"
            f"{baseline['framework_sha']}...{newest_sha}",
            token,
        )
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        errors.append(f"laravel/framework compare lookup failed: {exc}")
        return newest, []

    changed = filter_watched_files(compare.get("files", []), WATCHED_FRAMEWORK_PREFIXES)
    return newest, changed


def fetch_symfony_drift(
    baseline: dict[str, str], token: str | None, errors: list[str]
) -> tuple[int, list[str]]:
    try:
        repo_info = github_get(f"/repos/{baseline['symfony_repo']}", token)
        default_branch = repo_info.get("default_branch", "main")
        compare = github_get(
            f"/repos/{baseline['symfony_repo']}/compare/"
            f"{baseline['symfony_sha']}...{default_branch}",
            token,
        )
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        errors.append(f"laravel/symfony-on-cloud lookup failed: {exc}")
        return 0, []

    changed = filter_watched_files(compare.get("files", []), WATCHED_SYMFONY_PREFIXES)
    commit_count = len(compare.get("commits", []))
    return commit_count, changed


def main() -> int:
    baseline = load_baseline()
    token = os.environ.get("GITHUB_TOKEN")
    errors: list[str] = []

    framework_newest, framework_changed = fetch_framework_drift(baseline, token, errors)
    symfony_commit_count, symfony_changed = fetch_symfony_drift(baseline, token, errors)

    report = build_report(
        baseline, framework_newest, framework_changed, symfony_changed, symfony_commit_count, errors
    )
    warnings = warnings_for_report(framework_newest, baseline, framework_changed, symfony_changed)

    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as fh:
            fh.write(report)
            fh.write("\n")

    for warning in warnings:
        print(f"::warning::{warning}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
