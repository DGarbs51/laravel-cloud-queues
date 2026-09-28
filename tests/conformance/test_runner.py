from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from demo.conformance import ROOT, build_report, catalog_errors, endpoint
from demo.conformance.plugin import ConformancePlugin, Evidence, NodeResult, safe


def catalog():
    return {
        "schema_version": 1,
        "baseline": {},
        "exception_list": [],
        "features": [
            {
                "id": "demo.feature",
                "title": "Feature",
                "expected": "Works",
                "tier": "unit",
                "required": True,
                "laravel_source": [{"path": "source.php", "lines": "1"}],
                "symfony_source": [],
                "deviation": None,
                "probe": ["test_demo.py::test_feature"],
            }
        ],
    }


def plugin(status="pass"):
    ref = "test_demo.py::test_feature"
    result = ConformancePlugin([ref])
    result.matches[ref] = [ref]
    result.results[ref] = NodeResult(status=status, phases={"setup", "call", "teardown"})
    return result


def test_missing_record_fails():
    assert not build_report(catalog(), ConformancePlugin([]))["gate"]["passed"]
    data = catalog()
    data["features"][0]["probe"] = None
    report = build_report(data, plugin())
    assert report["features"][0]["status"] == "missing"
    assert not report["gate"]["passed"]
    assert catalog_errors({"features": []})


def test_removed_catalog_record_fails_scope_coverage():
    data = json.loads((ROOT / "docs/contract/catalog.json").read_text())
    scope = (ROOT / "PROJECT_SCOPE.md").read_text()
    assert not catalog_errors(data, scope)
    data["features"] = [f for f in data["features"] if f["id"] != "dispatch.standard"]
    assert catalog_errors(data, scope)


@pytest.mark.parametrize("status", ["skipped", "partial", "unsupported", "fail"])
def test_unapproved_nonpass_fails(status):
    assert not build_report(catalog(), plugin(status))["gate"]["passed"]


def test_approved_skip_passes():
    data = catalog()
    data["exception_list"] = [
        {
            "id": "demo.feature",
            "allowed_statuses": ["skipped"],
            "reason": "Platform unavailable",
            "approval": "Scope §1",
        }
    ]
    report = build_report(data, plugin("skipped"))
    assert report["gate"]["passed"]
    assert report["features"][0]["exception_approval"]["approval"] == "Scope §1"
    assert not build_report(data, plugin("fail"))["gate"]["passed"]


def test_duplicate_id_fails():
    data = catalog()
    data["features"].append(copy.deepcopy(data["features"][0]))
    assert not build_report(data, plugin())["gate"]["passed"]


def test_evidence_tier_and_error_are_preserved():
    recorder = plugin()
    result = next(iter(recorder.results.values()))
    evidence = Evidence(result)
    evidence.record("message_ids", ["same", "same"])
    evidence.record("attempts", [1, 2])
    evidence.tier("unit")
    report = build_report(catalog(), recorder)
    feature = report["features"][0]
    assert feature["tier"] == "unit"
    assert next(iter(feature["evidence"].values()))["attempts"] == [1, 2]
    data = catalog()
    data["features"][0]["tier"] = "emulated"
    assert not build_report(data, recorder)["gate"]["passed"]
    assert "requires emulated" in build_report(data, recorder)["features"][0]["error"]


def test_partial_explicit_and_incomplete_run():
    recorder = plugin()
    result = next(iter(recorder.results.values()))
    Evidence(result).status("partial", "Only emulated fairness")
    assert build_report(catalog(), recorder)["features"][0]["status"] == "partial"
    result.phases.remove("teardown")
    assert build_report(catalog(), recorder)["features"][0]["status"] == "missing"


def test_report_redacts_sensitive_fields_and_endpoint():
    assert safe({"receiptHandle": "private", "payload": "full body"}) == {
        "receiptHandle": "[redacted]",
        "payload": "[redacted]",
    }
    assert endpoint("rediss://user:secret@localhost:6380/15?password=secret") == (
        "rediss://localhost:6380/15"
    )


def test_unknown_selection_and_collection_failure_fail():
    assert not build_report(catalog(), plugin(), only=["not.a.feature"])["gate"]["passed"]
    recorder = plugin()
    recorder.errors.append("Collection error")
    assert not build_report(catalog(), recorder)["gate"]["passed"]


@pytest.mark.parametrize(
    ("entrypoint", "selection", "passes"),
    [
        ([sys.executable, "-m", "demo.conformance"], [], True),
        ([str(Path(sys.executable).with_name("laravel-cloud-queues")), "conformance"], [], True),
        (
            [str(Path(sys.executable).with_name("laravel-cloud-queues")), "conformance"],
            ["-k", "no_such_conformance_case"],
            False,
        ),
    ],
    ids=["module", "console", "console-failed-gate"],
)
def test_only_smoke(tmp_path: Path, entrypoint, selection, passes):
    report = tmp_path / "report.json"
    run = subprocess.run(
        [
            *entrypoint,
            "--report",
            str(report),
            "--only",
            "envelope.v1_shape",
            "envelope.codecs",
            "agent.receive_count_default",
            "-q",
            *selection,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert run.returncode == (0 if passes else 1), run.stdout + run.stderr
    data = json.loads(report.read_text())
    assert data["gate"]["passed"] == passes
    assert len(data["features"]) == 3
    assert all(f["status"] == ("pass" if passes else "missing") for f in data["features"])
    assert data["selection"] == [
        "agent.receive_count_default",
        "envelope.codecs",
        "envelope.v1_shape",
    ]
    assert data["run"]["revision"]
    assert ("Gate: PASS" if passes else "Gate: FAIL") in run.stdout


@pytest.mark.parametrize("selection", [[], ["-k", "one"]])
def test_parameter_deselection_cannot_pass(tmp_path, selection):
    import os

    (tmp_path / "test_cases.py").write_text("""
import pytest
@pytest.mark.parametrize("value", [1, 2], ids=["one", "two"])
def test_value(value, evidence):
    evidence.record("value", value)
""")
    script = """
import json
import pytest
from demo.conformance.plugin import ConformancePlugin
from demo.conformance import build_report
plugin = ConformancePlugin(["test_cases.py::test_value"])
pytest.main(["test_cases.py", "-q", *SELECTION], plugins=[plugin])
report = build_report(CATALOG, plugin)
open("result.json", "w").write(json.dumps(report))
""".replace("SELECTION", repr(selection)).replace("CATALOG", repr(catalog()))
    script = script.replace("test_demo.py::test_feature", "test_cases.py::test_value")
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "result.json").read_text())
    assert report["gate"]["passed"] == (not selection)
    assert len(report["features"][0]["evidence"]) == 2


def test_explicit_nonpass_is_not_overwritten_by_passing_call():
    recorder = plugin()
    node = next(iter(recorder.results))
    result = recorder.results[node]
    Evidence(result).status("unsupported", "Runtime does not support capability")
    report = pytest.TestReport(
        node, ("test_demo.py", 1, "test_feature"), {}, "passed", None, "call", duration=0.1
    )
    recorder.pytest_runtest_logreport(report)
    assert result.status == "unsupported"
    report = pytest.TestReport(
        node, ("test_demo.py", 1, "test_feature"), {}, "failed", "cleanup", "teardown", duration=0.1
    )
    recorder.pytest_runtest_logreport(report)
    assert result.status == "fail"
    assert result.error == "cleanup"


def test_all_catalog_nodes_resolve(tmp_path):
    report = tmp_path / "collection.json"
    result = subprocess.run(
        [sys.executable, "-m", "demo.conformance", "--collect-only", "-q", "--report", str(report)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Collection alone cannot pass a conformance gate, but every reference must resolve.
    assert result.returncode == 1, result.stdout + result.stderr
    data = json.loads(report.read_text())
    assert all(not feature["unresolved_probes"] for feature in data["features"])
    assert not any("Collection failed" in error for error in data["gate"]["errors"])
