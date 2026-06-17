"""Typed pattern and predicate objects for the fluent Cypher query builder.

An optional, richer alternative to raw pattern strings. ``node()`` and ``rel()``
build composable graph patterns; property access (``p["age"]``) yields
``PropertyRef`` objects whose comparison operators produce ``Predicate`` objects.

Pattern and predicate objects are **query-independent**: they carry their values
unbound and expose a ``render(allocate)`` method. The owning ``CypherQuery``
supplies an ``allocate(value) -> placeholder`` callback at attach time, so the
query remains the single owner of parameter naming (no collision risk).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# A callback supplied by CypherQuery: binds a value and returns its placeholder
# (e.g. "$_p0"). Pattern/predicate objects use it to defer parameter naming.
Allocate = Callable[[Any], str]


@dataclass(frozen=True)
class Predicate:
    """A boolean expression rendered with deferred parameter binding.

    ``template`` is a Cypher fragment with ``{}`` slots; ``values`` are filled
    into those slots as bound parameters when ``render`` is called.
    """

    template: str
    values: tuple[Any, ...] = ()

    def render(self, allocate: Allocate) -> str:
        """Render the predicate, binding each value via ``allocate``."""
        placeholders = tuple(allocate(v) for v in self.values)
        return self.template.format(*placeholders)

    def __and__(self, other: Predicate) -> Predicate:
        return Predicate(f"({self.template}) AND ({other.template})", (*self.values, *other.values))

    def __or__(self, other: Predicate) -> Predicate:
        return Predicate(f"({self.template}) OR ({other.template})", (*self.values, *other.values))

    def __invert__(self) -> Predicate:
        return Predicate(f"NOT ({self.template})", self.values)


@dataclass(frozen=True)
class PropertyRef:
    """A reference to a property of an aliased node or relationship (e.g. ``p.age``)."""

    expr: str

    def _compare(self, op: str, other: Any) -> Predicate:
        if isinstance(other, PropertyRef):
            return Predicate(f"{self.expr} {op} {other.expr}")
        return Predicate(f"{self.expr} {op} {{}}", (other,))

    def __eq__(self, other: Any) -> Predicate:  # type: ignore[override]
        return self._compare("=", other)

    def __ne__(self, other: Any) -> Predicate:  # type: ignore[override]
        return self._compare("<>", other)

    def __gt__(self, other: Any) -> Predicate:
        return self._compare(">", other)

    def __ge__(self, other: Any) -> Predicate:
        return self._compare(">=", other)

    def __lt__(self, other: Any) -> Predicate:
        return self._compare("<", other)

    def __le__(self, other: Any) -> Predicate:
        return self._compare("<=", other)

    def in_(self, values: list[Any]) -> Predicate:
        """Build an ``expr IN $param`` predicate binding ``values`` as one list param."""
        return Predicate(f"{self.expr} IN {{}}", (list(values),))

    def is_null(self) -> Predicate:
        """Build an ``expr IS NULL`` predicate."""
        return Predicate(f"{self.expr} IS NULL")

    def is_not_null(self) -> Predicate:
        """Build an ``expr IS NOT NULL`` predicate."""
        return Predicate(f"{self.expr} IS NOT NULL")

    def starts_with(self, value: str) -> Predicate:
        """Build an ``expr STARTS WITH $param`` predicate."""
        return Predicate(f"{self.expr} STARTS WITH {{}}", (value,))

    def contains(self, value: str) -> Predicate:
        """Build an ``expr CONTAINS $param`` predicate."""
        return Predicate(f"{self.expr} CONTAINS {{}}", (value,))


@dataclass(frozen=True)
class NodePattern:
    """A node in a graph pattern, e.g. ``(p:Person {name: ...})``.

    Inline property values are carried unbound and rendered via the owning
    query's ``allocate`` callback.
    """

    labels: tuple[str, ...] = ()
    alias: str | None = None
    props: tuple[tuple[str, Any], ...] = ()

    def __getitem__(self, prop: str) -> PropertyRef:
        if not self.alias:
            raise ValueError("property access requires an aliased node (set alias=...)")
        return PropertyRef(f"{self.alias}.{prop}")

    def to(self, rel: RelPattern, other: NodePattern) -> PathPattern:
        """Build an outgoing path: ``self -[rel]-> other``."""
        return PathPattern((self,), (("->", rel, other),))

    def __sub__(self, rel: RelPattern) -> _PartialPath:
        # Enables: node - rel - other_node  (undirected unless rel sets direction)
        return _PartialPath(self, rel)

    def render(self, allocate: Allocate) -> str:
        """Render this node, binding any inline property values."""
        label_part = "".join(f":{lbl}" for lbl in self.labels)
        head = f"{self.alias or ''}{label_part}"
        if self.props:
            rendered = ", ".join(f"{k}: {allocate(v)}" for k, v in self.props)
            return f"({head} {{{rendered}}})"
        return f"({head})"


@dataclass(frozen=True)
class RelPattern:
    """A relationship in a graph pattern, e.g. ``-[k:KNOWS]->``."""

    types: tuple[str, ...] = ()
    alias: str | None = None
    direction: str = "->"  # one of "->", "<-", "--"
    props: tuple[tuple[str, Any], ...] = ()

    def __getitem__(self, prop: str) -> PropertyRef:
        if not self.alias:
            raise ValueError("property access requires an aliased relationship (set alias=...)")
        return PropertyRef(f"{self.alias}.{prop}")

    def _inner(self, allocate: Allocate) -> str:
        type_part = ":" + "|".join(self.types) if self.types else ""
        head = f"{self.alias or ''}{type_part}"
        if self.props:
            rendered = ", ".join(f"{k}: {allocate(v)}" for k, v in self.props)
            return f"[{head} {{{rendered}}}]"
        return f"[{head}]" if head else "[]"

    def render(self, allocate: Allocate, left: str, right: str) -> str:
        """Render the relationship between two already-rendered node strings."""
        inner = self._inner(allocate)
        if self.direction == "->":
            return f"{left}-{inner}->{right}"
        if self.direction == "<-":
            return f"{left}<-{inner}-{right}"
        return f"{left}-{inner}-{right}"


@dataclass(frozen=True)
class _PartialPath:
    """Intermediate result of ``node - rel`` awaiting the target node."""

    left: NodePattern
    rel: RelPattern

    def __sub__(self, right: NodePattern) -> PathPattern:
        return PathPattern((self.left,), ((self.rel.direction, self.rel, right),))


@dataclass(frozen=True)
class PathPattern:
    """A multi-hop path: a head node followed by (direction, rel, node) hops."""

    heads: tuple[NodePattern, ...]
    hops: tuple[tuple[str, RelPattern, NodePattern], ...] = field(default_factory=tuple)

    def to(self, rel: RelPattern, other: NodePattern) -> PathPattern:
        """Extend the path with another outgoing hop."""
        return PathPattern(self.heads, (*self.hops, (rel.direction, rel, other)))

    def render(self, allocate: Allocate) -> str:
        """Render the whole path into a single Cypher pattern string."""
        result = self.heads[0].render(allocate)
        for _direction, rel, node in self.hops:
            node_str = node.render(allocate)
            result = rel.render(allocate, result, node_str)
        return result


def node(*labels: str, alias: str | None = None, **props: Any) -> NodePattern:
    """Create a node pattern.

    Exposed only from ``cypher_graphdb.cypherquery`` (not the top-level package),
    so the bare name does not collide with the top-level ``@node`` model
    decorator. Callers who need both in one scope alias on import.

    Args:
        *labels: Zero or more node labels.
        alias: Optional query variable (required for property access).
        **props: Inline property values (bound as parameters at render time).
    """
    return NodePattern(labels=labels, alias=alias, props=tuple(props.items()))


def rel(*types: str, alias: str | None = None, direction: str = "->", **props: Any) -> RelPattern:
    """Create a relationship pattern.

    Exposed only from ``cypher_graphdb.cypherquery`` (not the top-level package).

    Args:
        *types: Zero or more relationship types (rendered as ``:A|B``).
        alias: Optional query variable (required for property access).
        direction: ``"->"`` (default), ``"<-"``, or ``"--"`` (undirected).
        **props: Inline property values (bound as parameters at render time).
    """
    return RelPattern(types=types, alias=alias, direction=direction, props=tuple(props.items()))
