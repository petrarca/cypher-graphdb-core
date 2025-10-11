"""Utilities for handling column names and resolving wildcards in query results."""

from typing import Any


def resolve_column_names(return_arguments: dict[str, str], result_data: Any, col_count: int) -> dict[str, str]:
    """Resolve wildcard (*) in return_arguments to actual column names.

    When a Cypher query contains RETURN *, the parser captures it as {"*": "*"}.
    This function resolves the wildcard by inspecting the actual result data
    to determine what columns were returned by the database.

    Args:
        return_arguments: Dict from parsed query, e.g. {"*": "*", "name": "p.name"}
        result_data: The query result data (list of rows or graph structure)
        col_count: Number of columns in the result

    Returns:
        Resolved column names dict with wildcard expanded.
        For graph results, uses label names (e.g. "Person", "KNOWS").
        For tabular results with objects, uses keys from first row.
        For tabular results with arrays, uses generic names.

    Examples:
        # RETURN * with graph result
        >>> resolve_column_names({"*": "*"}, graph_result, 3)
        {"0": "Person", "1": "KNOWS", "2": "Company"}

        # RETURN *, p.name with tabular result
        >>> resolve_column_names({"*": "*", "name": "p.name"}, tabular_result, 2)
        {"0": "Person", "name": "p.name"}

        # RETURN p.name (no wildcard)
        >>> resolve_column_names({"p.name": "p.name"}, result, 1)
        {"p.name": "p.name"}
    """
    # No wildcard - return as-is
    if "*" not in return_arguments:
        return return_arguments

    # Extract explicit columns after wildcard
    explicit_cols = {k: v for k, v in return_arguments.items() if k != "*"}
    num_explicit = len(explicit_cols)
    num_wildcard = col_count - num_explicit

    if num_wildcard <= 0:
        # No wildcard columns (shouldn't happen but handle gracefully)
        return explicit_cols

    # Detect column names from result data
    wildcard_names = _detect_column_names(result_data, num_wildcard)

    # Build resolved column names: wildcard columns + explicit columns
    resolved = {}

    # Add wildcard columns with numeric keys
    for i, name in enumerate(wildcard_names):
        resolved[str(i)] = name

    # Add explicit columns with their original keys
    resolved.update(explicit_cols)

    return resolved


def _detect_column_names(result_data: Any, expected_count: int) -> list[str]:
    """Detect column names from result data.

    Args:
        result_data: Query result (list of rows, graph, etc.)
        expected_count: Expected number of columns to detect

    Returns:
        List of column names detected from data
    """
    # Handle graph results (has 'nodes' and 'edges' attributes)
    if hasattr(result_data, "nodes") and hasattr(result_data, "edges"):
        return _detect_from_graph(result_data, expected_count)

    # Handle list of rows (tabular data)
    if isinstance(result_data, list) and len(result_data) > 0:
        return _detect_from_rows(result_data[0], expected_count)

    # Fallback: generic column names
    return [f"col_{i}" for i in range(expected_count)]


def _detect_from_graph(_graph: Any, expected_count: int) -> list[str]:
    """Detect column names from graph structure.

    For graph results, we inspect the first row of data to see what
    types of objects were returned (nodes, edges, etc.) and use
    their label names.

    Args:
        _graph: Graph object (currently unused, intentionally prefixed
                with underscore)
        expected_count: Number of columns expected

    Returns:
        List of generic column names
    """
    # This requires access to the raw result rows, which Graph doesn't
    # expose. For now, return generic names.
    # Future: Enhance Graph class to preserve row structure
    return [f"col_{i}" for i in range(expected_count)]


def _detect_from_rows(first_row: Any, expected_count: int) -> list[str]:
    """Detect column names from the first row of tabular data.

    Args:
        first_row: First row of result data
        expected_count: Expected number of columns

    Returns:
        List of detected column names
    """
    names = []

    # If row is a tuple/list, inspect each element
    if isinstance(first_row, (list, tuple)):
        for i, item in enumerate(first_row[:expected_count]):
            names.append(_get_item_label(item, i))
    # If row is a dict, use its keys
    elif isinstance(first_row, dict):
        names = list(first_row.keys())[:expected_count]
    # Single value
    else:
        names.append(_get_item_label(first_row, 0))

    # Pad with generic names if needed
    while len(names) < expected_count:
        names.append(f"col_{len(names)}")

    return names[:expected_count]


def _get_item_label(item: Any, index: int) -> str:
    """Get a descriptive label for a result item.

    Args:
        item: Result item (node, edge, value, etc.)
        index: Index of item in row (for fallback naming)

    Returns:
        Descriptive label for the item
    """
    # Handle dict-like objects with 'node' or 'edge' keys
    # (format used by backend when returning graph objects)
    if isinstance(item, dict):
        if "node" in item and isinstance(item["node"], dict):
            return item["node"].get("label_", f"col_{index}")
        if "edge" in item and isinstance(item["edge"], dict):
            return item["edge"].get("label_", f"col_{index}")
        # Generic dict - might have a 'label_' key
        if "label_" in item:
            return item["label_"]

    # Handle objects with label_ attribute (domain model objects)
    if hasattr(item, "label_"):
        return item.label_

    # Fallback to generic name
    return f"col_{index}"
