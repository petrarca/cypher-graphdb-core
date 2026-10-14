"""Composite endpoint keys for bulk edge creation.

``bulk_create_edges`` matches each endpoint node by a *reference key* -- one or
more properties whose values identify the node. A single property is the common
case (``"symbol"``), but it is only correct when that property is unique within
the label. When it is not (e.g. the same ``symbol`` exists in several sources of
a shared graph), the key must be composite (``("symbol", "source_key")``) so an
edge binds to the intended node rather than an arbitrary match.

These helpers normalise the ``str | Sequence[str]`` ref-prop argument and turn an
edge's ``src``/``dst`` value (scalar or tuple) into a hashable key aligned with
the prop order.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def as_ref_props(ref_prop: str | Sequence[str]) -> tuple[str, ...]:
    """Normalise a ref-prop argument to an ordered tuple of property names.

    A plain string becomes a one-tuple; a sequence is kept in order. Empty
    sequences are rejected -- an endpoint must be matched on at least one prop.
    """
    if isinstance(ref_prop, str):
        return (ref_prop,)
    props = tuple(ref_prop)
    if not props:
        raise ValueError("ref_prop must name at least one property")
    if not all(isinstance(p, str) for p in props):
        raise TypeError("ref_prop sequence must contain only property-name strings")
    return props


def ref_key(value: Any, n_props: int) -> tuple[Any, ...]:
    """Turn an edge's src/dst match value into a key tuple of length ``n_props``.

    For a single-prop key the value is a scalar (wrapped into a one-tuple); for a
    composite key the value must be a sequence whose length matches ``n_props``.
    A tuple key is always returned so callers can index a composite map uniformly.
    """
    if n_props == 1:
        # A one-element sequence is accepted too, but a bare scalar is the norm.
        if isinstance(value, (list, tuple)):
            seq = tuple(value)
            if len(seq) != 1:
                raise ValueError(f"expected 1 match value, got {len(seq)}")
            return seq
        return (value,)
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"composite key needs a sequence of {n_props} values, got {type(value).__name__}")
    seq = tuple(value)
    if len(seq) != n_props:
        raise ValueError(f"expected {n_props} match values, got {len(seq)}")
    return seq
