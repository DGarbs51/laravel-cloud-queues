from __future__ import annotations

import subprocess
import sys


def _import_with(prelude: str) -> subprocess.CompletedProcess[str]:
    code = prelude + "\nimport laravel_cloud_queues.fastapi\n"
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)


def test_missing_fastapi_names_the_extra() -> None:
    result = _import_with("import sys; sys.modules['fastapi'] = None")
    assert result.returncode != 0
    assert 'pip install "laravel-cloud-queues[fastapi]"' in result.stderr


def test_old_fastapi_is_refused() -> None:
    result = _import_with("import fastapi; fastapi.__version__ = '0.120.4'")
    assert result.returncode != 0
    assert "requires FastAPI >= 0.121 (found 0.120.4)" in result.stderr
