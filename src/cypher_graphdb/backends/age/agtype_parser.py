"""JSON-based agtype parser -- replaces the age package's ANTLR parser.

AGE's agtype text format is JSON with ``::vertex``, ``::edge``, and ``::path``
type annotations. The official AGE Python driver uses an ANTLR grammar to parse
this, but its ``visitStringValue`` implementation has a bug: it strips surrounding
quotes with ``strip('"')`` without unescaping ``\\"`` inside the string, causing
double-quote characters in property values to be returned with literal backslash
escapes.

This module replaces the ANTLR parser with Python's ``json.loads``, which
correctly handles all JSON escape sequences. The type annotations are stripped
before parsing and used to construct the appropriate ``age.models`` objects
(``Vertex``, ``Edge``, ``Path``).

Performance: ``json.loads`` is implemented in C and is significantly faster than
the ANTLR parser for large result sets.
"""

from __future__ import annotations

import json
import re
from typing import Any

from age.models import Edge, Path, Vertex

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


def _parse_vertex(json_str: str) -> Vertex:
    """Parse a vertex JSON object into an age.models.Vertex."""
    d = json.loads(json_str)
    v = Vertex()
    v.id = d.get("id")
    v.label = d.get("label", "")
    v.properties = d.get("properties", {})
    return v


def _parse_edge(json_str: str) -> Edge:
    """Parse an edge JSON object into an age.models.Edge."""
    d = json.loads(json_str)
    e = Edge()
    e.id = d.get("id")
    e.label = d.get("label", "")
    e.start_id = d.get("start_id")
    e.end_id = d.get("end_id")
    e.properties = d.get("properties", {})
    return e


def _parse_path(raw: str) -> Path:
    """Parse a path: [{...}::vertex, {...}::edge, ...]::path into an age.models.Path."""
    # Strip outer ::path and the surrounding brackets
    inner = raw[: -len("::path")]

    # Strip ::vertex and ::edge annotations from inside the array so
    # json.loads can parse it as a plain JSON array of objects.
    clean = _ELEMENT_ANNOTATION_RE.sub("}", inner)
    elements_raw = json.loads(clean)

    entities = []
    for i, elem in enumerate(elements_raw):
        if i % 2 == 0:
            # Even positions are vertices
            v = Vertex()
            v.id = elem.get("id")
            v.label = elem.get("label", "")
            v.properties = elem.get("properties", {})
            entities.append(v)
        else:
            # Odd positions are edges
            e = Edge()
            e.id = elem.get("id")
            e.label = elem.get("label", "")
            e.start_id = elem.get("start_id")
            e.end_id = elem.get("end_id")
            e.properties = elem.get("properties", {})
            entities.append(e)

    return Path(entities)
