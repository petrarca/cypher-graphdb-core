"""AGE inline Cypher literal serializer.

Apache AGE does not support $params in UNWIND statements. Standard Cypher
uses UNWIND $rows AS props, but AGE requires the list to be an inline
literal. This module provides serialization from Python dicts to inline
Cypher map literals suitable for AGE UNWIND.

Note: This is intentionally separate from utils/string_utils.py's
convert_to_str / dict_to_non_quoted_json, which are used in the Cypher
builder for parameterized queries (where the driver handles escaping).
Here we must handle escaping ourselves because values are inlined
directly into the Cypher text.

Example output:
    [{name: "Foo", start_line: 42}, {name: "Bar", start_line: 99}]
"""


def escape_value(v: object) -> str:
    """Escape a Python value for inline AGE Cypher literals.

    Handles None, bool, int, float, and str-coercible values. Strings are
    escaped for double-quoted Cypher literals: backslashes, double quotes,
    newlines, carriage returns, and tabs are all escaped.

    Args:
        v: Python value (None, bool, int, float, or str-coercible).

    Returns:
        Cypher literal string representation.
    """
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int | float):
        return str(v)
    # Order matters: escape backslashes first, then other characters
    escaped = (
        str(v)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\0", "")
    )
    return f'"{escaped}"'


def to_cypher_list(rows: list[dict]) -> str:
    """Serialize a list of dicts as an inline Cypher list literal for AGE UNWIND.

    Args:
        rows: List of property dicts to serialize.

    Returns:
        Cypher list literal string, e.g. [{name: "Foo"}, {name: "Bar"}].

    Raises:
        ValueError: If rows is empty.
    """
    if not rows:
        raise ValueError("Cannot serialize empty list to Cypher literal")
    parts = []
    for row in rows:
        props = ", ".join(f"{k}: {escape_value(v)}" for k, v in row.items())
        parts.append(f"{{{props}}}")
    return "[" + ", ".join(parts) + "]"
