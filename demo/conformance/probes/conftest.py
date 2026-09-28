from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from demo.conformance import ROOT
from demo.conformance.plugin import Evidence, NodeResult, safe


@pytest.fixture
def artifacts(request: pytest.FixtureRequest) -> Iterator[Path]:
    suffix = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:12]
    path = ROOT / "conformance-artifacts" / (request.node.name[:60] + "-" + suffix)
    path.mkdir(parents=True, exist_ok=True)
    # A rerun must never mistake previous handler output for this run's observations.
    for filename in ("worker.jsonl", "producer.jsonl"):
        (path / filename).unlink(missing_ok=True)
    yield path
    for log in path.glob("*.log"):
        sanitized = []
        for line in log.read_text(errors="replace").splitlines():
            try:
                sanitized.append(json.dumps(safe(json.loads(line)), ensure_ascii=False))
            except (ValueError, TypeError):
                sanitized.append(safe(line))
        log.write_text("\n".join(sanitized) + "\n")


@pytest.fixture
def evidence(request: pytest.FixtureRequest) -> Evidence:
    """Standalone pytest remains useful; the runner supplies its recording plugin."""
    from demo.conformance.plugin import ConformancePlugin

    for plugin in request.config.pluginmanager.get_plugins():
        if isinstance(plugin, ConformancePlugin):
            return Evidence(plugin.results[request.node.nodeid])
    return Evidence(NodeResult())
