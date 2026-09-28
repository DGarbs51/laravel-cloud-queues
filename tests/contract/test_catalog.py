"""Coverage and integrity checks for docs/contract/catalog.json.

Stdlib only. Run with:
    uv run --no-project --with pytest pytest tests/contract -q
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "docs" / "contract" / "catalog.json"
SCHEMA = ROOT / "docs" / "contract" / "catalog.schema.json"

TIERS = {"unit", "socket", "emulated", "live"}
DEVIATION_FOLLOWS = {"symfony", "project"}
RECORD_FIELDS = {
    "id",
    "title",
    "expected",
    "tier",
    "required",
    "laravel_source",
    "symfony_source",
    "deviation",
    "project_decision",
    "probe",
}
SOURCE_FIELDS = {"path", "lines", "symbol"}

ID_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
# Decision records (D4), or "python-only" for behaviour with no Laravel counterpart.
DECISION_LIST_RE = r"^(python-only|D\d+[a-z]?(, D\d+[a-z]?)*)$"


def load_catalog() -> dict:
    return json.loads(CATALOG.read_text(encoding="utf-8"))


def test_catalog_is_valid_json_with_baseline():
    catalog = load_catalog()
    assert catalog["schema_version"] == 1
    baseline = catalog["baseline"]
    assert baseline["laravel/framework"]["version"] == "v13.33.0"
    assert baseline["laravel/framework"]["commit"] == "91188a17ceaa3dbace6e8a5f7abd0d042e466359"
    symfony_pin = "50c945170b6cb5690370d15fd725c6f82495e9ba"
    assert baseline["laravel/symfony-on-cloud"]["commit"] == symfony_pin
    assert set(catalog) == {"schema_version", "baseline", "exception_list", "features"}


def test_schema_document_matches_catalog_shape():
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert set(schema["properties"]) == {"schema_version", "baseline", "exception_list", "features"}
    record = schema["$defs"]["feature"]
    assert set(record["properties"]) == RECORD_FIELDS
    assert set(record["required"]) == RECORD_FIELDS
    assert set(record["properties"]["tier"]["enum"]) == TIERS


def test_record_shape_and_unique_ids():
    features = load_catalog()["features"]
    ids = [f["id"] for f in features]
    assert len(ids) == len(set(ids)), "duplicate feature IDs"
    for f in features:
        assert set(f) == RECORD_FIELDS, f"{f.get('id')}: fields {set(f) ^ RECORD_FIELDS}"
        assert ID_RE.match(f["id"]), f["id"]
        assert f["title"] and f["expected"], f["id"]
        assert f["tier"] in TIERS, f["id"]
        assert isinstance(f["required"], bool), f["id"]
        assert isinstance(f["probe"], list) and f["probe"], f["id"]
        assert len(f["probe"]) == len(set(f["probe"])), f["id"]
        for probe in f["probe"]:
            assert isinstance(probe, str) and "::" in probe, f["id"]
            path, node = probe.split("::", 1)
            assert (ROOT / path).resolve().is_relative_to(ROOT), probe
            assert (ROOT / path).is_file() and node.startswith("test_"), probe
            function = node.split("[", 1)[0]
            assert re.search(
                rf"^(async )?def {re.escape(function)}\(",
                (ROOT / path).read_text(encoding="utf-8"),
                re.M,
            ), probe
        assert "status" not in f and "result" not in f, f["id"]
        for src_key in ("laravel_source", "symfony_source"):
            for src in f[src_key]:
                assert set(src) == SOURCE_FIELDS, f"{f['id']}: {src_key} {src}"
                assert re.match(r"^\d+(-\d+)?$", src["lines"]), f"{f['id']}: {src}"
        if not f["laravel_source"]:
            assert f["project_decision"] or f["deviation"], (
                f"{f['id']}: no Laravel source and no project decision/deviation"
            )
        if f["project_decision"] is not None:
            assert re.match(DECISION_LIST_RE, f["project_decision"]), f["id"]
        dev = f["deviation"]
        if dev is not None:
            assert set(dev) == {"label", "follows", "summary", "decision"}, f["id"]
            assert re.match(r"^[a-z0-9]+(-[a-z0-9]+)*$", dev["label"]), f["id"]
            assert dev["follows"] in DEVIATION_FOLLOWS, f["id"]
            assert dev["summary"], f["id"]
            assert dev["decision"] is None or re.match(r"^D\d+[a-z]?$", dev["decision"]), f["id"]


def test_exception_list_is_only_live_managed_queue_checks():
    catalog = load_catalog()
    ids = {f["id"] for f in catalog["features"]}
    assert catalog["exception_list"], "exception list must name the live checks"
    for entry in catalog["exception_list"]:
        assert set(entry) == {"id", "allowed_statuses", "reason", "approval"}
        assert entry["id"] in ids, entry["id"]
        assert entry["id"].startswith("cloud."), entry["id"]
        assert set(entry["allowed_statuses"]) <= {"skipped", "partial", "unsupported"}
        assert entry["reason"] and entry["approval"]
    for f in catalog["features"]:
        if f["tier"] == "live":
            assert f["id"] in {e["id"] for e in catalog["exception_list"]}, f["id"]
