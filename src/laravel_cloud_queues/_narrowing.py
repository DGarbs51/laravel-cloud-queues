"""Type narrowing helpers shared across the package."""

from __future__ import annotations

from collections.abc import Mapping

from typing_extensions import TypeIs


def is_mapping(value: object) -> TypeIs[Mapping[object, object]]:
    """Determine if the value is a mapping, without assuming anything about its keys or values."""
    return isinstance(value, Mapping)
