"""Downstream typing samples checked with ``ty`` (PROJECT_SCOPE.md §5, D5).

``samples/positive`` must type-check cleanly. In ``samples/negative`` every line carrying a
``# E: code[, code...]`` marker must produce exactly those error codes, and no other line
may produce errors.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SAMPLES = Path(__file__).resolve().parent / "samples"
MARKER = re.compile(r"#\s*E:\s*(?P<codes>[\w-]+(?:\s*,\s*[\w-]+)*)\s*$")
ERROR = re.compile(r"^(?P<path>[^:]+):(?P<line>\d+):\d+: error\[(?P<code>[\w-]+)\] ")


def expected_errors() -> Counter[tuple[str, int, str]]:
    expected: Counter[tuple[str, int, str]] = Counter()
    for path in sorted((SAMPLES / "negative").glob("*.py")):
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            match = MARKER.search(line)
            if match:
                for code in match["codes"].split(","):
                    expected[(path.name, number, code.strip())] += 1
    return expected


def test_samples_type_check() -> None:
    files = sorted(str(p.relative_to(ROOT)) for p in SAMPLES.rglob("*.py"))
    assert any("positive" in f for f in files)
    assert any("negative" in f for f in files)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ty",
            "check",
            "--python",
            sys.executable,
            "--output-format",
            "concise",
            "--color",
            "never",
            *files,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    output = completed.stdout + completed.stderr
    actual: Counter[tuple[str, int, str]] = Counter()
    for line in output.splitlines():
        match = ERROR.match(line.strip())
        if match:
            actual[(Path(match["path"]).name, int(match["line"]), match["code"])] += 1
    assert completed.returncode in (0, 1), output
    positive = {name for name, _, _ in actual} & {
        p.name for p in (SAMPLES / "positive").glob("*.py")
    }
    assert not positive, output
    assert actual == expected_errors(), output
