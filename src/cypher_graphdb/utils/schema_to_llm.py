"""Schema formatting utilities for LLM consumption.

This module provides utilities to convert JSON Schema definitions with x-graph
extensions into compact, LLM-friendly text formats optimized for AI agents.
"""


def format_schemas_for_llm(enriched_schema: dict) -> str:
    """Convert combined enriched JSON schema to compact LLM-friendly format.

    Takes a combined enriched schema with $defs structure and converts it to
    a concise text format that includes essential information for generating
    Cypher queries:
    - Schema title and description
    - Node labels with properties and types
    - Edge types with properties
    - Relationships between nodes

    Args:
        enriched_schema: Combined enriched schema dict with $defs structure

    Returns:
        Formatted string optimized for LLM consumption

    Example:
        >>> enriched_schema = {
        ...     "title": "Product Graph",
        ...     "description": "Complete product management schema",
        ...     "$defs": {...}
        ... }
        >>> print(format_schemas_for_llm(enriched_schema))
        # Product Graph
        # Complete product management schema
        ...
    """
    # Extract schemas from $defs and separate nodes/edges
    individual_schemas = list(enriched_schema["$defs"].values())
    nodes, edges = _separate_schemas(individual_schemas)
    schema_title = enriched_schema.get("title", "Graph Schema")
    schema_description = enriched_schema.get("description", "")

    # Return early if no schemas available
    if not nodes and not edges:
        return "(No schema information available)"

    lines = [f"# {schema_title}"]

    if schema_description:
        lines.append(f"# {schema_description}")

    lines.extend(
        [
            "## Format:",
            "- NodeLabel (description if available)",
            "  Properties: property(type; description), ...",
            "  -> REL_TYPE -> TargetNode [cardinality]",
            "- Arrows show relationship FROM source TO target",
            "- Use <- in Cypher to traverse relationships in reverse",
            "- Cardinality: [1] = ONE_TO_ONE, [*] = ONE_TO_MANY",
            "",
        ]
    )

    if nodes:
        lines.append("## Nodes")
        for schema in nodes:
            lines.extend(_format_node_schema(schema))
        lines.append("")

    if edges:
        lines.append("## Edges")
        for schema in edges:
            lines.extend(_format_edge_schema(schema))
        lines.append("")

    return "\n".join(lines)


def _separate_schemas(schemas: list[dict]) -> tuple[list[dict], list[dict]]:
    """Separate schemas into nodes and edges.

    Args:
        schemas: List of JSON Schema objects

    Returns:
        Tuple of (nodes, edges)
    """
    nodes = []
    edges = []

    for schema in schemas:
        x_graph = schema.get("x-graph", {})
        if x_graph.get("type") == "NODE":
            nodes.append(schema)
        elif x_graph.get("type") == "EDGE":
            edges.append(schema)

    return nodes, edges


def _format_node_schema(schema: dict) -> list[str]:
    """Format a single node schema.

    Args:
        schema: Node schema dictionary

    Returns:
        List of formatted lines
    """
    lines = []
    x_graph = schema.get("x-graph", {})
    label = x_graph.get("label", schema.get("title", "Unknown"))
    description = schema.get("description")
    properties = schema.get("properties", {})

    # Format label with optional description
    if description:
        lines.append(f"{label} ({description})")
    else:
        lines.append(f"{label}")

    # Format properties
    prop_strs = _format_properties(properties)
    if prop_strs:
        lines.append(f"  Properties: {', '.join(prop_strs)}")

    # Format relationships
    relations = x_graph.get("relations", [])
    for rel in relations:
        lines.append(_format_relationship(rel))

    return lines


def _format_edge_schema(schema: dict) -> list[str]:
    """Format a single edge schema.

    Args:
        schema: Edge schema dictionary

    Returns:
        List of formatted lines
    """
    lines = []
    x_graph = schema.get("x-graph", {})
    label = x_graph.get("label", schema.get("title", "Unknown"))
    description = schema.get("description")
    properties = schema.get("properties", {})

    # Format label with optional description
    if description:
        lines.append(f"{label} ({description})")
    else:
        lines.append(f"{label}")

    # Format properties
    prop_strs = _format_properties(properties)
    if prop_strs:
        lines.append(f"  Properties: {', '.join(prop_strs)}")

    return lines


def _format_properties(properties: dict) -> list[str]:
    """Format properties dictionary to compact string representations.

    Args:
        properties: Properties dictionary from JSON Schema

    Returns:
        List of formatted property strings
    """
    prop_strs = []
    for prop_name, prop_def in properties.items():
        prop_type = _abbreviate_type(prop_def.get("type", "any"))
        description = prop_def.get("description")
        if description:
            prop_strs.append(f"{prop_name}({prop_type}; {description})")
        else:
            prop_strs.append(f"{prop_name}({prop_type})")
    return prop_strs


def _format_relationship(rel: dict) -> str:
    """Format a relationship definition.

    Args:
        rel: Relationship dictionary from x-graph.relations

    Returns:
        Formatted relationship string
    """
    rel_type = rel.get("rel_type_name")
    to_type = rel.get("to_type_name")
    cardinality = rel.get("cardinality", "ONE_TO_MANY")
    description = rel.get("description")
    card_symbol = "1" if cardinality == "ONE_TO_ONE" else "*"
    base = f"  -> {rel_type} -> {to_type} [{card_symbol}]"
    if description:
        return f"{base} ({description})"
    return base


def _abbreviate_type(json_type: str | list | None) -> str:
    """Abbreviate JSON Schema types for compact display.

    Args:
        json_type: JSON Schema type (string, array, or None)

    Returns:
        Abbreviated type string

    Example:
        >>> _abbreviate_type("string")
        'str'
        >>> _abbreviate_type("integer")
        'int'
        >>> _abbreviate_type(["string", "null"])
        'str?'
    """
    if json_type is None:
        return "any"

    # Handle array of types (e.g., ["string", "null"])
    if isinstance(json_type, list):
        # Filter out null and get primary type
        non_null_types = [t for t in json_type if t != "null"]
        if not non_null_types:
            return "null"
        primary_type = non_null_types[0]
        nullable = "null" in json_type
        abbrev = _abbreviate_single_type(primary_type)
        return f"{abbrev}?" if nullable else abbrev

    return _abbreviate_single_type(json_type)


def _abbreviate_single_type(json_type: str) -> str:
    """Abbreviate a single JSON Schema type.

    Args:
        json_type: JSON Schema type string

    Returns:
        Abbreviated type string
    """
    type_map = {
        "string": "str",
        "integer": "int",
        "number": "num",
        "boolean": "bool",
        "array": "arr",
        "object": "obj",
        "null": "null",
    }
    return type_map.get(json_type, json_type)
