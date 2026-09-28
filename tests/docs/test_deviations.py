"""docs/deviations.md documents every deviation label in the conformance catalog."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_every_catalog_deviation_is_documented() -> None:
    catalog = json.loads((ROOT / "docs/contract/catalog.json").read_text(encoding="utf-8"))
    labels = {f["deviation"]["label"] for f in catalog["features"] if f.get("deviation")}
    assert labels
    documented = set(
        re.findall(r"^\| `([a-z0-9-]+)` \|", (ROOT / "docs/deviations.md").read_text(), re.M)
    )
    assert labels <= documented, f"undocumented deviations: {sorted(labels - documented)}"
    assert documented <= labels, f"stale deviation rows: {sorted(documented - labels)}"
