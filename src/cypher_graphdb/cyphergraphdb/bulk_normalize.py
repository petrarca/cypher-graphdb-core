"""Helpers for normalizing typed input to the bulk write API.

The facade accepts either raw ``list[dict]`` (traditional shape) or
``list[GraphNode]`` / ``list[GraphEdge]`` (typed shape). These helpers convert
typed instances into the ``list[dict]`` shape the backends expect, deriving
the label automatically from the decorator registration.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..models import GraphEdge, GraphNode


def normalize_nodes_input(
    rows: Sequence[dict[str, Any]] | Sequence[GraphNode],
    label: str | None,
) -> tuple[str, list[dict[str, Any]]]:
    """Normalize bulk_create_nodes input to (label, list_of_dicts).

    Args:
        rows: Either plain dicts or decorated ``GraphNode`` instances. When
            mixed, a ``TypeError`` is raised.
        label: Node label. Required when ``rows`` are dicts; ignored (derived)
            when ``rows`` are typed instances. If both an explicit ``label`` and
            typed instances are given, the explicit label wins but must match
            the instances' labels.

    Returns:
        A tuple ``(derived_label, dict_rows)`` ready for backend invocation.

    Raises:
        TypeError: If ``rows`` mixes dicts and typed instances, or contains
            something else entirely.
        ValueError: If ``rows`` are typed instances with inconsistent labels,
            or if ``rows`` are dicts without an explicit ``label``.
    """
    if not rows:
        if label is None:
            raise ValueError("bulk_create_nodes: 'label' is required when 'rows' is empty")
        return label, []

    first = rows[0]
    if isinstance(first, GraphNode):
        return _normalize_typed_nodes(rows, label)
    if isinstance(first, dict):
        return _normalize_dict_nodes(rows, label)

    raise TypeError(
        f"bulk_create_nodes: 'rows' must be list[dict] or list[GraphNode], got element of type {type(first).__name__}"
    )


def _normalize_typed_nodes(rows: Sequence[GraphNode], label: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Convert a list of GraphNode instances to (label, dict_rows)."""
    derived = rows[0].graph_info_.label_
    for i, node in enumerate(rows):
        if not isinstance(node, GraphNode):
            raise TypeError(f"bulk_create_nodes: rows[{i}] is {type(node).__name__}, expected GraphNode (cannot mix types)")
        if node.graph_info_.label_ != derived:
            raise ValueError(
                f"bulk_create_nodes: all GraphNode instances must share the same label "
                f"(rows[0]={derived!r}, rows[{i}]={node.graph_info_.label_!r})"
            )
    if label is not None and label != derived:
        raise ValueError(f"bulk_create_nodes: explicit label {label!r} does not match instances' label {derived!r}")
    return derived, [node.flatten_properties() for node in rows]


def _normalize_dict_nodes(rows: Sequence[dict[str, Any]], label: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Pass through a list of dicts after validating label and homogeneity."""
    if label is None:
        raise ValueError("bulk_create_nodes: 'label' is required when 'rows' are dicts")
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"bulk_create_nodes: rows[{i}] is {type(row).__name__}, expected dict (cannot mix types)")
    return label, list(rows)


def normalize_edges_input(
    edges: Sequence[dict[str, Any]] | Sequence[GraphEdge],
    src_refs: Sequence[Any] | None,
    dst_refs: Sequence[Any] | None,
    label: str | None,
) -> tuple[str, list[dict[str, Any]]]:
    """Normalize bulk_create_edges input to (label, list_of_edge_dicts).

    For typed edges, ``src_refs`` and ``dst_refs`` are required parallel lists
    that carry the match values against ``src_ref_prop`` / ``dst_ref_prop``.
    Edge properties are taken from ``flatten_properties()``.

    For dict edges, ``src_refs`` / ``dst_refs`` must be None; each dict carries
    ``"src"`` / ``"dst"`` plus arbitrary edge properties (traditional shape).

    Args:
        edges: Either plain dicts or decorated ``GraphEdge`` instances.
        src_refs: Parallel list of source match values (typed path only).
        dst_refs: Parallel list of destination match values (typed path only).
        label: Edge label. Required for dicts; ignored for typed instances
            (derived). If both are given and non-matching, raises.

    Returns:
        A tuple ``(derived_label, edge_dicts)`` where each dict has ``"src"``,
        ``"dst"`` and edge properties.

    Raises:
        TypeError: If ``edges`` mixes dicts and typed instances.
        ValueError: If typed edges are missing ``src_refs`` / ``dst_refs``,
            if parallel-list lengths disagree, or if labels are inconsistent.
    """
    if not edges:
        if label is None:
            raise ValueError("bulk_create_edges: 'label' is required when 'edges' is empty")
        return label, []

    first = edges[0]
    if isinstance(first, GraphEdge):
        return _normalize_typed_edges(edges, src_refs, dst_refs, label)
    if isinstance(first, dict):
        if src_refs is not None or dst_refs is not None:
            raise ValueError(
                "bulk_create_edges: 'src_refs' / 'dst_refs' must be None when 'edges' are dicts "
                "(dicts carry 'src'/'dst' directly)"
            )
        return _normalize_dict_edges(edges, label)

    raise TypeError(
        f"bulk_create_edges: 'edges' must be list[dict] or list[GraphEdge], got element of type {type(first).__name__}"
    )


def _normalize_typed_edges(
    edges: Sequence[GraphEdge],
    src_refs: Sequence[Any] | None,
    dst_refs: Sequence[Any] | None,
    label: str | None,
) -> tuple[str, list[dict[str, Any]]]:
    """Convert a list of GraphEdge instances + parallel refs to (label, edge_dicts)."""
    if src_refs is None or dst_refs is None:
        raise ValueError("bulk_create_edges: 'src_refs' and 'dst_refs' are required when 'edges' are GraphEdge instances")
    if len(src_refs) != len(edges) or len(dst_refs) != len(edges):
        raise ValueError(
            f"bulk_create_edges: parallel lists must have equal length "
            f"(edges={len(edges)}, src_refs={len(src_refs)}, dst_refs={len(dst_refs)})"
        )

    derived = edges[0].graph_info_.label_
    result: list[dict[str, Any]] = []
    for i, edge_obj in enumerate(edges):
        if not isinstance(edge_obj, GraphEdge):
            raise TypeError(f"bulk_create_edges: edges[{i}] is {type(edge_obj).__name__}, expected GraphEdge (cannot mix types)")
        if edge_obj.graph_info_.label_ != derived:
            raise ValueError(
                f"bulk_create_edges: all GraphEdge instances must share the same label "
                f"(edges[0]={derived!r}, edges[{i}]={edge_obj.graph_info_.label_!r})"
            )
        edge_dict: dict[str, Any] = {"src": src_refs[i], "dst": dst_refs[i]}
        edge_dict.update(edge_obj.flatten_properties())
        result.append(edge_dict)

    if label is not None and label != derived:
        raise ValueError(f"bulk_create_edges: explicit label {label!r} does not match instances' label {derived!r}")
    return derived, result


def _normalize_dict_edges(edges: Sequence[dict[str, Any]], label: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Pass through a list of edge dicts after validating label and homogeneity."""
    if label is None:
        raise ValueError("bulk_create_edges: 'label' is required when 'edges' are dicts")
    for i, e in enumerate(edges):
        if not isinstance(e, dict):
            raise TypeError(f"bulk_create_edges: edges[{i}] is {type(e).__name__}, expected dict (cannot mix types)")
        if "src" not in e or "dst" not in e:
            raise ValueError(f"bulk_create_edges: edges[{i}] missing required 'src'/'dst' keys")
    return label, list(edges)
