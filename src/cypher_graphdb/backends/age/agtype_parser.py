"""JSON-based agtype parser -- replaces the age package's ANTLR parser.

AGE's agtype text format is JSON with ``::vertex``, ``::edge``, and ``::path``
type annotations. The official AGE Python driver uses an ANTLR grammar to parse
this, but its ``visitStringValue`` implementation has a bug: it strips surrounding
quotes with ``strip('"')`` without unescaping ``\\"`` inside the string, causing
double-quote characters in property values to be returned with literal backslash
escapes.

This module replaces the ANTLR parser entirely -- no dependency on the ``age``
package or ANTLR runtime. Uses Python's ``json.loads`` for correct JSON escape
handling, and defines its own Vertex/Edge/Path data classes that are API-compatible
with the ``age.models`` originals.

Performance: ``json.loads`` is implemented in C and is significantly faster than
the ANTLR parser for large result sets.
"""

from __future__ import annotations

import json
import re
from typing import Any

# ── Graph data classes (replace age.models) ───────────────────────────────────


class Vertex:
    """A graph vertex parsed from agtype ``::vertex`` data."""

    def __init__(self, id=None, label=None, properties=None):
        self.id = id
        self.label = label
        self.properties = properties or {}

    def __repr__(self):
        return f"Vertex(id={self.id}, label={self.label!r}, properties={self.properties})"

    def __getitem__(self, name):
        return self.properties.get(name)

    def __setitem__(self, name, value):
        self.properties[name] = value


class Edge:
    """A graph edge parsed from agtype ``::edge`` data."""

    def __init__(self, id=None, label=None, start_id=None, end_id=None, properties=None):
        self.id = id
        self.label = label
        self.start_id = start_id
        self.end_id = end_id
        self.properties = properties or {}

    def __repr__(self):
        return f"Edge(id={self.id}, label={self.label!r}, start={self.start_id}, end={self.end_id})"

    def __getitem__(self, name):
        return self.properties.get(name)

    def __setitem__(self, name, value):
        self.properties[name] = value


class Path:
    """A graph path parsed from agtype ``::path`` data -- a list of alternating vertices and edges."""

    def __init__(self, entities=None):
        self.entities = entities or []

    def __repr__(self):
        return f"Path(len={len(self.entities)})"

    def __len__(self):
        return len(self.entities)

    def __iter__(self):
        return iter(self.entities)

    def __getitem__(self, index):
        return self.entities[index]

    def append(self, entity):
        self.entities.append(entity)


# Regex to strip ::type annotation from the end of an agtype value.
# Matches ::vertex, ::edge, ::path, ::numeric (AGE's known type annotations).
_TYPE_SUFFIX_RE = re.compile(r"::(?:vertex|edge|path|numeric)$")

# Regex to strip ::vertex and ::edge annotations inside a path array.
# Path elements are: [{...}::vertex, {...}::edge, {...}::vertex]::path
_ELEMENT_ANNOTATION_RE = re.compile(r"}::(?:vertex|edge)")


def parse_agtype(raw: str) -> Any:
    """Parse a raw agtype text value into Python objects.

    Returns:
        - ``str``, ``int``, ``float``, ``bool``, ``None`` for scalars
        - ``list`` for arrays
        - ``dict`` for plain maps
        - ``Vertex`` for ``::vertex`` annotated maps
        - ``Edge`` for ``::edge`` annotated maps
        - ``Path`` for ``::path`` annotated arrays

    Raises:
        json.JSONDecodeError: If the agtype text is not valid JSON after
            stripping type annotations.
    """
    if not raw:
        return None

    # Detect and strip outer type annotation
    suffix = _detect_suffix(raw)

    if suffix == "path":
        return _parse_path(raw)
    if suffix == "vertex":
        return _parse_vertex(raw[: -len("::vertex")])
    if suffix == "edge":
        return _parse_edge(raw[: -len("::edge")])

    # Strip ::numeric if present (AGE uses this for high-precision numbers)
    clean = _TYPE_SUFFIX_RE.sub("", raw)
    return json.loads(clean)


def _detect_suffix(raw: str) -> str | None:
    """Detect the ::type suffix without modifying the string."""
    if raw.endswith("::vertex"):
        return "vertex"
    if raw.endswith("::edge"):
        return "edge"
    if raw.endswith("::path"):
        return "path"
    if raw.endswith("::numeric"):
        return "numeric"
    return None


def _vertex_from_dict(d: dict) -> Vertex:
    """Construct a Vertex from a parsed agtype object dict."""
    v = Vertex()
    v.id = d.get("id")
    v.label = d.get("label", "")
    v.properties = d.get("properties", {})
    return v


def _edge_from_dict(d: dict) -> Edge:
    """Construct an Edge from a parsed agtype object dict."""
    e = Edge()
    e.id = d.get("id")
    e.label = d.get("label", "")
    e.start_id = d.get("start_id")
    e.end_id = d.get("end_id")
    e.properties = d.get("properties", {})
    return e


def _parse_vertex(json_str: str) -> Vertex:
    """Parse a vertex JSON string into a Vertex."""
    return _vertex_from_dict(json.loads(json_str))


def _parse_edge(json_str: str) -> Edge:
    """Parse an edge JSON string into an Edge."""
    return _edge_from_dict(json.loads(json_str))


def _parse_path(raw: str) -> Path:
    """Parse a path agtype string into a Path.

    Path format: [{...}::vertex, {...}::edge, {...}::vertex]::path
    Alternating even-index vertices and odd-index edges.
    """
    # Strip outer ::path and the surrounding brackets
    inner = raw[: -len("::path")]

    # Strip ::vertex and ::edge annotations from inside the array so
    # json.loads can parse it as a plain JSON array of objects.
    clean = _ELEMENT_ANNOTATION_RE.sub("}", inner)
    elements_raw = json.loads(clean)

    entities = []
    for i, elem in enumerate(elements_raw):
        if i % 2 == 0:
            entities.append(_vertex_from_dict(elem))
        else:
            entities.append(_edge_from_dict(elem))

    return Path(entities)
