from __future__ import annotations

from pathlib import Path

import pytest


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Probes need brokers and run through ``python -m tests.conformance``, which passes
    their paths explicitly. A plain ``pytest`` run skips them."""
    if collection_path.name != "probes":
        return None
    probes = collection_path.resolve()
    for arg in config.args:
        target = Path(arg.split("::", 1)[0]).resolve()
        if target == probes or probes in target.parents:
            return None
    return True
