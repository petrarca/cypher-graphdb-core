"""Parameter primitives for the fluent Cypher query builder.

Defines the reserved auto-name prefix, the ``UNSET`` sentinel for deferred
parameters, the ``Param`` value object, and the ``param()`` helper.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..exceptions import CypherQueryError

# Reserved prefix for auto-generated bound-parameter names. The leading
# underscore keeps auto-names disjoint from caller-supplied params and from
# param() names; callers must not use this prefix for their own names.
AUTO_PREFIX = "_p"


class _Unset:
    """Sentinel for a deferred (value-less) named parameter."""

    _instance: _Unset | None = None

    def __new__(cls) -> _Unset:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"


UNSET = _Unset()


@dataclass(frozen=True)
class Param:
    """A named, optionally-bound Cypher parameter.

    A *bound* ``Param`` (``value`` supplied) contributes its value to the query
    params under its own name. A *deferred* ``Param`` (no value) emits its
    ``$name`` placeholder but is omitted from the params dict; the caller must
    supply it at execute time.
    """

    name: str
    value: Any = UNSET

    @property
    def is_deferred(self) -> bool:
        return self.value is UNSET


def param(name: str, value: Any = UNSET) -> Param:
    """Create a named bound parameter for use in a fluent query.

    Use when the same placeholder must appear in multiple clauses, or when a
    stable, readable parameter name is wanted. Omit ``value`` to defer it to
    execute time.

    Args:
        name: Parameter name (without the leading ``$``). Must not start with
            the reserved ``_p`` prefix.
        value: The bound value. Omit to defer to execute time.

    Returns:
        A ``Param`` instance.

    Raises:
        CypherQueryError: If ``name`` uses the reserved auto-name prefix.
    """
    if name.startswith(AUTO_PREFIX):
        raise CypherQueryError(f"param name {name!r} must not start with reserved prefix {AUTO_PREFIX!r}")
    return Param(name=name, value=value)
