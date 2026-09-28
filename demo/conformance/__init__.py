"""Catalog-driven pytest runner and fail-closed compatibility report (§21–22)."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import re
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest

from .plugin import TIERS, ConformancePlugin, safe

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "docs/contract/catalog.json"


def catalog_errors(catalog: dict[str, Any], scope: str | None = None) -> list[str]:
    errors = []
    features = catalog.get("features", [])
    if not features:
        errors.append("Catalog has no feature records")
    ids = [feature["id"] for feature in features]
    errors.extend(f"Duplicate feature ID: {key}" for key, n in Counter(ids).items() if n > 1)
    exceptions = catalog.get("exception_list", [])
    exception_ids = [entry["id"] for entry in exceptions]
    if len(exception_ids) != len(set(exception_ids)):
        errors.append("Duplicate exception ID")
    for entry in exceptions:
        if (
            entry["id"] not in ids
            or not entry.get("approval")
            or not entry.get("reason")
            or not set(entry["allowed_statuses"]) <= {"skipped", "partial", "unsupported"}
        ):
            errors.append(f"Invalid exception: {entry['id']}")
    if scope is not None:
        block = scope.split("### Required demo capabilities\n", 1)[1].split(
            "### Human-readable output", 1
        )[0]
        required = [
            line[2:].rstrip().rstrip(";.").strip()
            for line in block.splitlines()
            if line.startswith("- ")
        ]
        acceptance = scope.split("## 30. Acceptance criteria", 1)[1].split("## 31.", 1)[0]
        required += [f"§30.{number}" for number in re.findall(r"^(\d+)\. ", acceptance, re.M)]
        covers = Counter(item for feature in features for item in feature.get("covers", []))
        errors.extend(
            f"Missing or duplicate catalog coverage: {item}"
            for item in required
            if covers[item] == 0 or (not item.startswith("§30.") and covers[item] != 1)
        )
    return errors


def build_report(
    catalog: dict[str, Any],
    plugin: ConformancePlugin,
    *,
    only: list[str] | None = None,
    errors: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    problems = catalog_errors(catalog) + list(errors or []) + plugin.errors
    selected = set(only or [])
    known = {f["id"] for f in catalog["features"]}
    problems.extend(f"Unknown feature: {feature}" for feature in sorted(selected - known))
    exceptions = {entry["id"]: entry for entry in catalog.get("exception_list", [])}
    records = []
    for feature in catalog["features"]:
        if selected and feature["id"] not in selected:
            continue
        refs = feature.get("probe")
        valid = isinstance(refs, list) and bool(refs) and all(isinstance(r, str) for r in refs)
        refs = refs if valid else []
        nodes = sorted({node for ref in refs for node in plugin.matches.get(ref, [])})
        results = [plugin.results[node] for node in nodes]
        missing = not refs or any(not plugin.matches.get(ref) for ref in refs)
        missing |= any("teardown" not in result.phases for result in results)
        statuses = {result.status for result in results}
        tier_errors = [
            f"{node}: exercises {plugin.results[node].tier}, requires {feature['tier']}"
            for node in nodes
            if plugin.results[node].tier is not None
            and TIERS[plugin.results[node].tier or "unit"] < TIERS[feature["tier"]]
        ]
        status = next(
            (
                s
                for s in ("fail", "missing", "unsupported", "partial", "skipped", "pass")
                if s in statuses
            ),
            "missing",
        )
        if missing:
            status = "missing"
        elif tier_errors:
            status = "fail"
        exception = exceptions.get(feature["id"])
        approved = bool(exception and status in exception["allowed_statuses"])
        if status == "missing" or (feature["required"] and status != "pass" and not approved):
            problems.append(f"{feature['id']}: {status}")
        records.append(
            {
                **{
                    key: feature[key]
                    for key in (
                        "id",
                        "title",
                        "expected",
                        "laravel_source",
                        "symfony_source",
                        "tier",
                        "required",
                    )
                },
                "deviation": feature["deviation"],
                "status": status,
                "observed": "; ".join(result.observed for result in results if result.observed)
                or f"{len(nodes)} pytest cases: {dict(Counter(r.status for r in results))}",
                "evidence": {
                    node: {
                        "duration_seconds": plugin.results[node].duration,
                        "tier": plugin.results[node].tier or feature["tier"],
                        **plugin.results[node].evidence,
                    }
                    for node in nodes
                },
                "error": "\n".join([r.error for r in results if r.error] + tier_errors) or None,
                "unresolved_probes": [ref for ref in refs if not plugin.matches.get(ref)],
                "exception_approval": exception if approved else None,
            }
        )
    return {
        "schema_version": 1,
        "run": metadata or {},
        "baseline": catalog.get("baseline", {}),
        "selection": sorted(selected) if selected else "all",
        "features": records,
        "summary": dict(Counter(record["status"] for record in records)),
        "gate": {"passed": not problems, "errors": problems},
    }


def endpoint(value: str) -> str:
    parts = urlsplit(value)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if parts.port:
        host += f":{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def run_metadata(sqs: str) -> dict[str, Any]:
    revision = (
        subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()
        or "unknown"
    )
    versions: dict[str, str | None] = {}
    for name in (
        "laravel-cloud-queues",
        "pytest",
        "fastapi",
        "anyio",
        "boto3",
        "redis",
        "moto",
        "opentelemetry-api",
    ):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return {
        "revision": revision,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "os": platform.platform(),
            "python": platform.python_version(),
            "versions": versions,
            "backends": {
                "sqs": sqs,
                "sqs_endpoint": endpoint(
                    os.environ.get(
                        "LARAVEL_CLOUD_QUEUES_TEST_SQS_ENDPOINT", "http://localhost:4566"
                    )
                )
                if sqs == "localstack"
                else "loopback:ephemeral (moto)",
                "redis_endpoint": endpoint(
                    os.environ.get(
                        "LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL", "redis://127.0.0.1:6379/15"
                    )
                ),
                "agent": "local Unix socket emulator",
                "collector": "local Unix socket",
            },
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("compatibility-report.json"))
    parser.add_argument("--only", nargs="+", action="extend")
    parser.add_argument(
        "--sqs",
        choices=("moto", "localstack"),
        default=os.environ.get("LARAVEL_CLOUD_QUEUES_TEST_SQS", "moto"),
    )
    args, pytest_args = parser.parse_known_args(argv)
    catalog = json.loads(CATALOG.read_text())
    errors = catalog_errors(catalog, (ROOT / "PROJECT_SCOPE.md").read_text())
    features = [f for f in catalog["features"] if not args.only or f["id"] in args.only]
    references = sorted({ref for f in features for ref in (f.get("probe") or [])})
    plugin = ConformancePlugin(references)
    paths = []
    for ref in references:
        path = ROOT / ref.split("::", 1)[0]
        if not path.resolve().is_relative_to(ROOT) or not path.is_file():
            errors.append(f"Unresolved probe file: {ref}")
        elif str(path) not in paths:
            paths.append(str(path))
    os.environ["LARAVEL_CLOUD_QUEUES_TEST_SQS"] = args.sqs
    artifacts = ROOT / "conformance-artifacts"
    artifacts.mkdir(exist_ok=True)
    if paths:
        code = pytest.main([*paths, "--tb=short", *pytest_args], plugins=[plugin])
        if code != pytest.ExitCode.OK:
            errors.append(f"pytest exited {int(code)}")
    report = build_report(
        catalog, plugin, only=args.only, errors=errors, metadata=run_metadata(args.sqs)
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(safe(report), indent=2, allow_nan=False) + "\n")
    for record in report["features"]:
        print(f"{record['status'].upper():11} {record['id']}")
    print(f"Summary: {report['summary']}")
    print(f"Gate: {'PASS' if report['gate']['passed'] else 'FAIL'} ({args.report})")
    for error in report["gate"]["errors"]:
        print(f"  {error}")
    return 0 if report["gate"]["passed"] else 1
