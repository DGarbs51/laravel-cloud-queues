"""Type narrowing helpers shared across the package.

``isinstance`` narrows an ``object`` to a generic class with unknown type arguments, which
strict type checkers reject. These helpers narrow to the ``object``-parameterized type
instead, so the contents still have to be checked before use.
"""

from __future__ import annotations

from collections.abc import Mapping

from typing_extensions import TypeIs


def is_mapping(value: object) -> TypeIs[Mapping[object, object]]:
    """Determine if the value is a mapping."""
    return isinstance(value, Mapping)


def is_list(value: object) -> TypeIs[list[object]]:
    """Determine if the value is a list."""
    return isinstance(value, list)
