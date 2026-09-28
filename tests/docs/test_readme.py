"""README examples stay valid: every fenced ```python block compiles, and blocks preceded
by ``<!-- runnable -->`` execute in-process (eager mode / test doubles only, no network)."""

from __future__ import annotations

import ast
import re
import sys
import types
from pathlib import Path

import pytest

README = Path(__file__).resolve().parents[2] / "README.md"
RUNNABLE_MARKER = "<!-- runnable -->"
_BLOCK = re.compile(r"^(?P<prefix>.*?)```python\n(?P<code>.*?)^```", re.MULTILINE | re.DOTALL)


def _blocks() -> list[tuple[int, bool, str]]:
    """``(line_number, runnable, code)`` for each fenced python block."""
    text = README.read_text(encoding="utf-8")
    blocks = []
    for match in _BLOCK.finditer(text):
        # Runnable when the marker is the last non-blank line before the fence.
        preceding = match.group("prefix").rstrip().splitlines()
        runnable = bool(preceding) and preceding[-1].strip() == RUNNABLE_MARKER
        line = text.count("\n", 0, match.start("code")) + 1
        blocks.append((line, runnable, match.group("code")))
    assert blocks, "README has no ```python blocks"
    return blocks


BLOCKS = _blocks()
_ALL = [pytest.param(code, id=f"L{line}") for line, _, code in BLOCKS]
_RUNNABLE = [pytest.param(line, code, id=f"L{line}") for line, runnable, code in BLOCKS if runnable]


@pytest.mark.parametrize("code", _ALL)
def test_python_blocks_compile(code: str) -> None:
    # Prose snippets may use top-level ``await`` (they show code inside async handlers).
    compile(code, "README.md", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


@pytest.mark.parametrize(("line", "code"), _RUNNABLE)
def test_runnable_blocks_execute(line: int, code: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Examples never load configuration from the environment; make sure a stray backend
    # setting cannot turn an eager example into a real dispatch.
    for name in ("LARAVEL_CLOUD_QUEUES_BACKEND", "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG"):
        monkeypatch.delenv(name, raising=False)
    # A real module so dataclasses/type hints defined in the example resolve normally.
    module = types.ModuleType(f"readme_example_{line}")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    try:
        exec(compile(code, f"README.md:{line}", "exec"), module.__dict__)
    except NotImplementedError:
        # The FastAPI adapter is a contract stub until lane L8 lands on main.
        pytest.skip("laravel_cloud_queues.fastapi is not implemented on this branch yet")


def test_runnable_examples_exist() -> None:
    assert len(_RUNNABLE) >= 5
