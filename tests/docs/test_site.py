"""Read the Docs site examples stay valid, like the README's (see test_readme.py): every
fenced ```python block on a user-facing page compiles, and blocks preceded by
``<!-- runnable -->`` execute in-process (eager mode / test doubles only, no network)."""

from __future__ import annotations

import ast
import re
import sys
import types
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[2] / "docs"
RUNNABLE_MARKER = "<!-- runnable -->"
_BLOCK = re.compile(r"^(?P<prefix>.*?)```python\n(?P<code>.*?)^```", re.MULTILINE | re.DOTALL)
# Internal engineering records share docs/ but are excluded from the site (docs/conf.py).
_INTERNAL = {"architecture.md", "decisions.md", "deviations.md", "references.md"}
PAGES = sorted(
    path
    for path in DOCS.rglob("*.md")
    if path.relative_to(DOCS).parts[0] not in {"_build", "audits", "contract"}
    and path.name not in _INTERNAL
)


def _blocks() -> list[tuple[str, bool, str]]:
    """``(page:line, runnable, code)`` for each fenced python block on every page."""
    blocks = []
    for page in PAGES:
        text = page.read_text(encoding="utf-8")
        for match in _BLOCK.finditer(text):
            # Runnable when the marker is the last non-blank line before the fence.
            preceding = match.group("prefix").rstrip().splitlines()
            runnable = bool(preceding) and preceding[-1].strip() == RUNNABLE_MARKER
            line = text.count("\n", 0, match.start("code")) + 1
            blocks.append((f"{page.relative_to(DOCS)}:{line}", runnable, match.group("code")))
    return blocks


BLOCKS = _blocks()
_ALL = [pytest.param(where, code, id=where) for where, _, code in BLOCKS]
_RUNNABLE = [pytest.param(where, code, id=where) for where, runnable, code in BLOCKS if runnable]


def test_site_pages_exist() -> None:
    assert (DOCS / "index.md") in PAGES
    assert (DOCS / "conf.py").is_file()


@pytest.mark.parametrize(("where", "code"), _ALL)
def test_python_blocks_compile(where: str, code: str) -> None:
    # Snippets may use top-level ``await`` (they show code inside async handlers).
    compile(code, where, "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


@pytest.mark.parametrize(("where", "code"), _RUNNABLE)
def test_runnable_blocks_execute(where: str, code: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Examples never load configuration from the environment; make sure a stray backend
    # setting cannot turn an eager example into a real dispatch.
    for name in ("LARAVEL_CLOUD_QUEUES_BACKEND", "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG"):
        monkeypatch.delenv(name, raising=False)
    # A real module so dataclasses/type hints defined in the example resolve normally.
    module = types.ModuleType("docs_example_" + re.sub(r"\W", "_", where))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(code, where, "exec"), module.__dict__)


def test_runnable_examples_exist() -> None:
    assert len(_RUNNABLE) >= 10
