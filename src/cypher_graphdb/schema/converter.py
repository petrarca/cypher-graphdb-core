"""Schema to Graph Converter - Convert JSON Schemas to Graph representation."""

import uuid
from typing import Any

from ..models import Graph, GraphEdge, GraphNode


def _to_camel_case(key: str) -> str:
    """Convert hyphenated key to camelCase.

    Args:
        key: Hyphenated key (e.g., "x-graph", "x-ui-widget")

    Returns:
        camelCase key (e.g., "xGraph", "xUiWidget")
    """
    parts = key.split("-")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


def _generate_node_id(schema_name: str) -> int:
    """Generate stable numeric ID for a schema node.

    Uses UUID5 hash-based generation for deterministic IDs.

    Args:
        schema_name: Name of the schema type (e.g., "Person")

    Returns:
        Positive int64 ID
    """
    node_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"schema.node.{schema_name}")
    return hash(node_uuid) & 0x7FFFFFFFFFFFFFFF


def _generate_edge_id(source_name: str, target_name: str, rel_name: str) -> int:
    """Generate stable numeric ID for a schema edge.

    Uses UUID5 hash-based generation for deterministic IDs.

    Args:
        source_name: Source schema type name
        target_name: Target schema type name
        rel_name: Relation type name

    Returns:
        Positive int64 ID
    """
    edge_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"schema.edge.{source_name}.{target_name}.{rel_name}")
    return hash(edge_uuid) & 0x7FFFFFFFFFFFFFFF


def _generate_node_gid(schema_name: str) -> str:
    """Generate string-based gid_ for a schema node.

    Args:
        schema_name: Name of the schema type (e.g., "Person")

    Returns:
        String gid_ with "schema:" prefix (e.g., "schema:Person")
    """
    return f"schema:{schema_name}"


def _generate_edge_gid(source_name: str, target_name: str) -> str:
    """Generate string-based gid_ for a schema edge.

    Args:
        source_name: Source schema type name
        target_name: Target schema type name

    Returns:
        String gid_ with "schema:rel:" prefix (e.g., "schema:rel:Person:Organization")
    """
    return f"schema:rel:{source_name}:{target_name}"


def _create_schema_node(schema: dict[str, Any]) -> tuple[str, int, GraphNode]:
    """Create a GraphNode from a JSON Schema definition.

    Args:
        schema: JSON Schema dictionary with x-graph extension

    Returns:
        Tuple of (schema_name, node_id, GraphNode instance)
    """
    schema_name = schema.get("title", "Unknown")

    # Generate stable IDs
    node_id = _generate_node_id(schema_name)

    # Create GraphNode instance
    node = GraphNode(id_=node_id, label_="GraphNode", properties_={})

    # Set string-based gid_
    node.properties_["gid_"] = _generate_node_gid(schema_name)

    # Include all schema details in properties
    node.properties_["name"] = schema_name
    node.properties_["type"] = schema.get("type", "object")
    node.properties_["propertyDefinitions"] = schema.get("properties", {})
    node.properties_["required"] = schema.get("required", [])

    # Copy all x-* extensions to preserve custom metadata
    for key, value in schema.items():
        if key.startswith("x-"):
            camel_key = _to_camel_case(key)
            node.properties_[camel_key] = value

    return schema_name, node_id, node


def _create_schema_edges(
    schema_name: str,
    source_id: int,
    schema: dict[str, Any],
    schema_lookup: dict[str, tuple[int, dict[str, Any]]],
) -> list[GraphEdge]:
    """Create GraphEdge instances from a schema's relations.

    Args:
        schema_name: Name of the source schema type
        source_id: Numeric ID of the source node
        schema: JSON Schema dictionary
        schema_lookup: Mapping of schema names to (node_id, schema) tuples

    Returns:
        List of GraphEdge instances
    """
    edges = []
    xgraph = schema.get("x-graph", {})
    relations = xgraph.get("relations", [])

    for relation in relations:
        rel_name = relation.get("rel_type_name", "RELATED_TO")
        target_name = relation.get("to_type_name")

        if not target_name or target_name not in schema_lookup:
            continue

        target_id = schema_lookup[target_name][0]

        # Generate stable IDs
        edge_id = _generate_edge_id(schema_name, target_name, rel_name)

        # Create GraphEdge instance - use relation name as label
        edge = GraphEdge(id_=edge_id, label_=rel_name, start_id_=source_id, end_id_=target_id, properties_={})

        # Set string-based gid_
        edge.properties_["gid_"] = _generate_edge_gid(schema_name, target_name)

        # Include relation details in properties
        edge.properties_["name"] = rel_name
        edge.properties_["cardinality"] = relation.get("cardinality", "many-to-many")

        # Copy any additional relation metadata
        for key, value in relation.items():
            if key not in ("rel_type_name", "to_type_name", "cardinality"):
                edge.properties_[key] = value

        edges.append(edge)

    return edges


def json_schemas_to_graph(schemas: list[dict[str, Any]]) -> Graph:
    """Convert JSON Schemas to schema graph representation.

    Transforms JSON Schema definitions into a graph structure where:
    - Each NODE schema type becomes a GraphNode with label "GraphNode"
    - EDGE schema types are excluded (relations are shown as GraphEdge from NODE schemas)
    - Relations between types become GraphEdge with the relation name as label
    - IDs are deterministic (hash-based from UUID5) for stable caching
    - gid_ uses string prefix to distinguish from real data

    Args:
        schemas: List of JSON Schema dictionaries with x-graph extensions

    Returns:
        Graph object containing GraphNode and GraphEdge instances.
        Server can serialize with graph.model_dump(context={"with_type": True})
        to return {"nodes": [...], "edges": [...]} format.

    Example:
        >>> schemas = [
        ...     {
        ...         "title": "Person",
        ...         "type": "object",
        ...         "properties": {"name": {"type": "string"}},
        ...         "required": ["name"],
        ...         "x-graph": {
        ...             "type": "NODE",
        ...             "label": "Person",
        ...             "relations": [
        ...                 {
        ...                     "rel_type_name": "KNOWS",
        ...                     "to_type_name": "Person",
        ...                     "cardinality": "many-to-many"
        ...                 }
        ...             ]
        ...         }
        ...     }
        ... ]
        >>> graph = json_schemas_to_graph(schemas)
        >>> len(graph.nodes)
        1
        >>> node = list(graph.nodes.values())[0]
        >>> node.label_
        'GraphNode'
        >>> node.properties_["name"]
        'Person'
    """
    g = Graph()

    # Filter out EDGE schemas - they don't have source/target info
    # Relations are defined in NODE schemas' x-graph.relations array
    node_schemas = [s for s in schemas if s.get("x-graph", {}).get("type") != "EDGE"]

    # Create node lookup by schema name for edge generation
    schema_lookup: dict[str, tuple[int, dict[str, Any]]] = {}

    # First pass: Create all nodes (only NODE types)
    for schema in node_schemas:
        schema_name, node_id, node = _create_schema_node(schema)
        g.nodes[node_id] = node
        schema_lookup[schema_name] = (node_id, schema)

    # Second pass: Create edges from relations
    for schema_name, (source_id, schema) in schema_lookup.items():
        edges = _create_schema_edges(schema_name, source_id, schema, schema_lookup)
        for edge in edges:
            g.edges[edge.id_] = edge

    return g
