"""Fluent Cypher query builder.

Provides an optional, opt-in chainable builder for constructing Cypher queries
with automatic parameter binding, analogous to SQLAlchemy Core.

The builder is purely additive: raw Cypher strings, ``MatchCriteria`` objects,
and the typed ``create_or_merge`` API all remain first-class and unchanged.

A ``CypherQuery`` is **generative/immutable** -- every chained call returns a new
instance, so a base query can be safely branched into specialised forms:

    base = cdb.query().match("(p:Person)").where_eq("p.active", True)
    page = base.return_("p").order_by("p.name").limit(10)   # base untouched
    total = base.return_("count(p) AS n")                    # base untouched

Clauses are stored as an **ordered list of segments** and rendered in the order
they were added. Read queries therefore come out in canonical order (because
that is the order callers chain them), while write queries can interleave
``UNWIND -> MATCH -> MERGE -> SET`` as Cypher requires.

Values passed to convenience methods are bound automatically under a reserved
``_p`` prefix (``$_p0``, ``$_p1``, ...). Callers must not use that prefix for
their own ``param()`` names.

``match``/``optional_match``/``where``/``create``/``merge`` accept either a raw
Cypher string or a typed pattern/predicate object (see ``patterns.py``).

Terminal methods:
    ``build()``     -> ``(cypher_str, params)`` with ``$_pN`` placeholders intact.
    ``to_parsed()`` -> ``ParsedCypherQuery`` (reuses the existing parser).
    ``str(q)``      -> the Cypher string with placeholders (safe for logging).

``build(literal_binds=True)`` renders an inlined, **non-executable** form for
debugging only -- never execute that string.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from typing import Any, Literal, overload

from ..cypherparser import ParsedCypherQuery, parse_cypher_query
from ..exceptions import CypherQueryError
from .params import AUTO_PREFIX, Param
from .patterns import NodePattern, PathPattern, Predicate, RelPattern

# Pattern-like inputs accepted where a Cypher pattern string is expected.
_PatternInput = str | NodePattern | RelPattern | PathPattern


@dataclass(frozen=True)
class CypherQuery:
    """A generative, immutable fluent builder for Cypher queries.

    Each chained method returns a new ``CypherQuery``; the receiver is never
    mutated. Terminal methods (``build``, ``to_parsed``, ``str``) emit the
    accumulated query.

    See the module docstring for usage.
    """

    _segments: tuple[tuple[str, str], ...] = ()
    _params: dict[str, Any] = field(default_factory=dict)
    _counter: int = 0
    _cypher_cache: str | None = field(default=None, init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        # Pre-compute the Cypher string for non-empty frozen instances. Empty
        # instances compute lazily (and raise) only when build()/str() is called.
        if self._segments:
            object.__setattr__(self, "_cypher_cache", self._build_cypher())

    def _build_cypher(self) -> str:
        """Render the ordered segments into a Cypher string."""
        return "\n".join(f"{keyword} {body}" if keyword else body for keyword, body in self._segments)

    def _make_allocator(self, params: dict[str, Any], counter: list[int]):
        """Return an allocate(value) -> placeholder callback backed by params/counter."""

        def allocate(value: Any) -> str:
            name = f"{AUTO_PREFIX}{counter[0]}"
            params[name] = value
            counter[0] += 1
            return f"${name}"

        return allocate

    def _resolve_pattern(self, pattern: _PatternInput, params: dict[str, Any], counter: list[int]) -> str:
        """Render a pattern input (string or pattern object) to a Cypher string."""
        if isinstance(pattern, str):
            return pattern
        allocate = self._make_allocator(params, counter)
        return pattern.render(allocate)

    def _append(self, keyword: str, body: str, params: dict[str, Any], counter: int) -> CypherQuery:
        """Return a new query with one segment appended and params/counter updated."""
        return replace(self, _segments=(*self._segments, (keyword, body)), _params=params, _counter=counter)

    def _merge_binds(self, binds: dict[str, Any]) -> dict[str, Any]:
        """Merge caller-named **binds (values or Param) into a copy of params."""
        merged = dict(self._params)
        for key, val in binds.items():
            if isinstance(val, Param):
                if val.name != key:
                    raise CypherQueryError(f"bind keyword {key!r} must match param name {val.name!r}")
                if not val.is_deferred:
                    merged[val.name] = val.value
            else:
                merged[key] = val
        return merged

    def _bind_one(self, value: Any) -> tuple[str, dict[str, Any], int]:
        """Bind a single value to a new auto-named param. Returns (placeholder, params, counter)."""
        params = dict(self._params)
        name = f"{AUTO_PREFIX}{self._counter}"
        params[name] = value
        return f"${name}", params, self._counter + 1

    def _emit_pattern(self, keyword: str, pattern: _PatternInput, binds: dict[str, Any]) -> CypherQuery:
        """Append a pattern-bearing clause (MATCH/CREATE/MERGE/...).

        For a string ``pattern``, ``binds`` maps named placeholders in it to
        values. For a pattern object, inline property values auto-bind and
        ``binds`` must be empty.
        """
        if not isinstance(pattern, str):
            if binds:
                raise CypherQueryError("**binds are not allowed with a pattern object; use inline props instead")
            params = dict(self._params)
            counter = [self._counter]
            body = pattern.render(self._make_allocator(params, counter))
            return self._append(keyword, body, params, counter[0])
        merged = self._merge_binds(binds)
        return self._append(keyword, pattern, merged, self._counter)

    def match(self, pattern: _PatternInput, **binds: Any) -> CypherQuery:
        """Append a ``MATCH`` clause. Accepts a string (with ``**binds``) or pattern object."""
        return self._emit_pattern("MATCH", pattern, binds)

    def optional_match(self, pattern: _PatternInput, **binds: Any) -> CypherQuery:
        """Append an ``OPTIONAL MATCH`` clause. Accepts a string (with ``**binds``) or pattern object."""
        return self._emit_pattern("OPTIONAL MATCH", pattern, binds)

    def _append_where(self, condition: str, params: dict[str, Any], counter: int) -> CypherQuery:
        """Append a WHERE condition, merging into a trailing WHERE with ``AND``."""
        if self._segments and self._segments[-1][0] == "WHERE":
            prev = self._segments[-1][1]
            merged_segments = (*self._segments[:-1], ("WHERE", f"{prev} AND {condition}"))
            return replace(self, _segments=merged_segments, _params=params, _counter=counter)
        return self._append("WHERE", condition, params, counter)

    def where(self, condition: str | Predicate, **binds: Any) -> CypherQuery:
        """Append a boolean condition. Multiple calls join with ``AND``.

        ``condition`` is either a ``Predicate`` (from a typed property comparison)
        or a raw Cypher fragment. For a raw fragment, ``**binds`` maps named
        placeholders in it to values (plain value or ``param()``); the keyword
        key must equal the param name.
        """
        if isinstance(condition, Predicate):
            params = dict(self._params)
            counter = [self._counter]
            rendered = condition.render(self._make_allocator(params, counter))
            return self._append_where(rendered, params, counter[0])
        merged = self._merge_binds(binds)
        return self._append_where(condition, merged, self._counter)

    def where_id(self, var: str, value: int) -> CypherQuery:
        """Append ``id(var) = $_pN`` with ``value`` bound automatically."""
        placeholder, params, counter = self._bind_one(value)
        return self._append_where(f"id({var}) = {placeholder}", params, counter)

    def where_eq(self, prop: str, value: Any) -> CypherQuery:
        """Append ``prop = $_pN`` with ``value`` bound automatically."""
        placeholder, params, counter = self._bind_one(value)
        return self._append_where(f"{prop} = {placeholder}", params, counter)

    def where_in(self, expr: str, values: list[Any]) -> CypherQuery:
        """Append ``expr IN $_pN`` binding ``values`` as a single list param."""
        placeholder, params, counter = self._bind_one(list(values))
        return self._append_where(f"{expr} IN {placeholder}", params, counter)

    def with_(self, *items: str) -> CypherQuery:
        """Append a ``WITH`` clause."""
        return self._append("WITH", ", ".join(items), dict(self._params), self._counter)

    def return_(self, *items: str) -> CypherQuery:
        """Append a ``RETURN`` clause."""
        return self._append("RETURN", ", ".join(items), dict(self._params), self._counter)

    def return_distinct(self, *items: str) -> CypherQuery:
        """Append a ``RETURN DISTINCT`` clause."""
        return self._append("RETURN DISTINCT", ", ".join(items), dict(self._params), self._counter)

    def order_by(self, *items: str) -> CypherQuery:
        """Append an ``ORDER BY`` clause. Accepts ``"x DESC"`` etc."""
        return self._append("ORDER BY", ", ".join(items), dict(self._params), self._counter)

    def skip(self, n: int) -> CypherQuery:
        """Append a ``SKIP`` clause with ``n`` bound as a parameter."""
        placeholder, params, counter = self._bind_one(int(n))
        return self._append("SKIP", placeholder, params, counter)

    def limit(self, n: int) -> CypherQuery:
        """Append a ``LIMIT`` clause with ``n`` bound as a parameter."""
        placeholder, params, counter = self._bind_one(int(n))
        return self._append("LIMIT", placeholder, params, counter)

    def unwind(self, param_ref: str, as_: str) -> CypherQuery:
        """Append ``UNWIND <param_ref> AS <as_>``.

        ``param_ref`` is a parameter reference (e.g. ``"$rows"``) supplied by the
        caller at execute time -- it is NOT bound by the builder, since bulk
        payloads belong at the execute layer.
        """
        return self._append("UNWIND", f"{param_ref} AS {as_}", dict(self._params), self._counter)

    def create(self, pattern: _PatternInput, **binds: Any) -> CypherQuery:
        """Append a ``CREATE`` clause. Accepts a string (with ``**binds``) or pattern object."""
        return self._emit_pattern("CREATE", pattern, binds)

    def merge(self, pattern: _PatternInput, **binds: Any) -> CypherQuery:
        """Append a ``MERGE`` clause. Accepts a string (with ``**binds``) or pattern object."""
        return self._emit_pattern("MERGE", pattern, binds)

    def set(self, assignment: str, **binds: Any) -> CypherQuery:
        """Append a ``SET`` clause with optional named ``**binds``."""
        merged = self._merge_binds(binds)
        return self._append("SET", assignment, merged, self._counter)

    def on_create_set(self, assignment: str, **binds: Any) -> CypherQuery:
        """Append an ``ON CREATE SET`` clause (for a preceding ``MERGE``)."""
        merged = self._merge_binds(binds)
        return self._append("ON CREATE SET", assignment, merged, self._counter)

    def on_match_set(self, assignment: str, **binds: Any) -> CypherQuery:
        """Append an ``ON MATCH SET`` clause (for a preceding ``MERGE``)."""
        merged = self._merge_binds(binds)
        return self._append("ON MATCH SET", assignment, merged, self._counter)

    def delete(self, *vars_: str, detach: bool = False) -> CypherQuery:
        """Append a ``DELETE`` (or ``DETACH DELETE``) clause."""
        keyword = "DETACH DELETE" if detach else "DELETE"
        return self._append(keyword, ", ".join(vars_), dict(self._params), self._counter)

    def remove(self, *items: str) -> CypherQuery:
        """Append a ``REMOVE`` clause (e.g. ``remove("n.prop", "n:Label")``)."""
        return self._append("REMOVE", ", ".join(items), dict(self._params), self._counter)

    def raw(self, fragment: str, **binds: Any) -> CypherQuery:
        """Append a verbatim Cypher fragment, with optional named ``**binds``.

        The fragment is appended as its own segment in call order. Use sparingly;
        it bypasses the structured clause model.
        """
        merged = self._merge_binds(binds)
        return self._append("", fragment, merged, self._counter)

    def _assemble(self) -> str:
        if not self._segments:
            raise CypherQueryError("CypherQuery requires at least one clause before build()")
        if self._cypher_cache is None:
            object.__setattr__(self, "_cypher_cache", self._build_cypher())
        return self._cypher_cache

    @overload
    def build(self, literal_binds: Literal[False] = ...) -> tuple[str, dict[str, Any]]: ...

    @overload
    def build(self, literal_binds: Literal[True]) -> str: ...

    def build(self, literal_binds: bool = False) -> tuple[str, dict[str, Any]] | str:
        """Build the query.

        Returns:
            ``(cypher_str, params)`` with ``$_pN`` placeholders intact (default).
            With ``literal_binds=True``, returns a single ``str`` with values
            inlined -- for **debugging only**. Never execute the inlined form.
        """
        cypher = self._assemble()
        if literal_binds:
            return self._inline(cypher)
        return cypher, dict(self._params)

    def _inline(self, cypher: str) -> str:
        # Replace each $name placeholder with an inlined literal for human inspection.
        # Uses a regex negative lookahead so that $name is never replaced inside
        # a longer token (e.g. $name_full would not match when replacing $name).
        rendered = cypher
        for name, value in self._params.items():
            rendered = re.sub(rf"\${re.escape(name)}(?!\w)", self._literal(value), rendered)
        return rendered

    @staticmethod
    def _literal(value: Any) -> str:
        if isinstance(value, str):
            return json.dumps(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(CypherQuery._literal(v) for v in value) + "]"
        return json.dumps(value)

    def to_parsed(self) -> ParsedCypherQuery:
        """Parse the built query into a ``ParsedCypherQuery`` (reuses the parser)."""
        return parse_cypher_query(self._assemble())

    @property
    def params(self) -> dict[str, Any]:
        """The bound parameter dict (same as ``build()[1]``)."""
        return dict(self._params)

    @property
    def cypher(self) -> str:
        """The built Cypher string with ``$_pN`` placeholders intact."""
        return self._assemble()

    def __str__(self) -> str:
        return self._assemble()
