"""Record pytest outcomes without letting deselection or collection errors pass the gate."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import pytest

TIERS = {"unit": 0, "socket": 1, "emulated": 2, "live": 3}


def safe(value: Any) -> Any:
    """Evidence is JSON data. Raw envelopes, credentials and receipts stay out of reports."""
    if isinstance(value, dict):
        return {
            str(key): "[redacted]"
            if re.search(
                r"secret|password|credential|token|receipt|^payload$|^body$", str(key), re.I
            )
            else safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [safe(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"(\w+://)[^/@\s]+:[^/@\s]+@", r"\1[redacted]@", value)
        return re.sub(
            r'(?i)(["\']?(?:secret|password|receiptHandle|payload|body|token)["\']?\s*[:=]\s*)'
            r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|\S+)',
            r'\1"[redacted]"',
            value,
        )
    return value


@dataclass
class NodeResult:
    status: str = "missing"
    duration: float = 0
    evidence: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    phases: set[str] = field(default_factory=set)
    tier: str | None = None
    observed: str = ""


class Evidence:
    def __init__(self, result: NodeResult) -> None:
        self.result = result

    def record(self, key: str, value: Any) -> None:
        json.dumps(value, allow_nan=False)
        self.result.evidence.update(safe({key: value}))

    def observed(self, summary: str) -> None:
        self.result.observed = safe(summary)

    def status(self, status: str, reason: str) -> None:
        if status not in {"partial", "unsupported"} or not reason:
            raise ValueError("Explicit status must be partial/unsupported with a reason")
        self.result.status = status
        self.observed(reason)

    def tier(self, tier: str) -> None:
        if tier not in TIERS:
            raise ValueError(f"Unknown evidence tier: {tier}")
        self.result.tier = tier


class ConformancePlugin:
    def __init__(self, references: list[str]) -> None:
        self.references = references
        self.matches: dict[str, list[str]] = {ref: [] for ref in references}
        self.results: dict[str, NodeResult] = {}
        self.errors: list[str] = []

    @pytest.fixture
    def evidence(self, request: pytest.FixtureRequest) -> Evidence:
        return Evidence(self.results[request.node.nodeid])

    @pytest.hookimpl(tryfirst=True)
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        selected, deselected = [], []
        for item in items:
            refs = [
                ref
                for ref in self.references
                if item.nodeid == ref or item.nodeid.startswith(ref + "[")
            ]
            if not refs:
                deselected.append(item)
                continue
            selected.append(item)
            result = self.results.setdefault(item.nodeid, NodeResult())
            for ref in refs:
                self.matches[ref].append(item.nodeid)
            for marker in item.iter_markers("conformance"):
                tier = marker.kwargs.get("tier")
                if tier is not None:
                    if tier not in TIERS:
                        self.errors.append(f"{item.nodeid}: invalid tier {tier}")
                    else:
                        result.tier = tier
                    break
        items[:] = selected
        if deselected:
            deselected[0].config.hook.pytest_deselected(items=deselected)

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed:
            self.errors.append(f"Collection failed: {report.nodeid}: {safe(str(report.longrepr))}")

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        result = self.results.get(report.nodeid)
        if result is None:
            return
        result.phases.add(report.when)
        result.duration += report.duration
        if report.failed:
            result.status = "fail"
            result.error = safe(str(report.longrepr))
        elif report.skipped and result.status != "fail":
            result.status = "skipped"
            result.observed = safe(str(report.longrepr))
        elif report.when == "call" and result.status == "missing":
            # An xpass is not affirmative conformance evidence.
            result.status = "partial" if hasattr(report, "wasxfail") else "pass"
        for name, content in report.sections:
            result.evidence[name] = safe(content)
