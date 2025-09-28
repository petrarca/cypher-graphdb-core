"""Memgraph row factories for converting Memgraph results to CypherGraphDB types.

This module provides functions to convert Memgraph query results into CypherGraphDB
compatible data structures, handling the conversion of nodes, relationships, and paths.
"""

from collections.abc import Callable
from typing import Any

import mgclient
from loguru import logger

from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge as Edge
from cypher_graphdb.models import GraphNode as Node
from cypher_graphdb.models import GraphObject
from cypher_graphdb.models import GraphPath as Path


def _convert_property_value(value: Any) -> Any:
    """Convert a Memgraph property value to CypherGraphDB compatible format.

    Args:
        value: Value from Memgraph to convert.

    Returns:
        Converted value compatible with CypherGraphDB.
    """
    if value is None:
        return None

    if isinstance(value, int | float | str | bool):
        return value
    elif isinstance(value, list):
        return [_convert_property_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: _convert_property_value(v) for k, v in value.items()}
    else:
        # For any other types, convert to string
        logger.warning(f"Unknown property type: {type(value)}, converting to string")
        return str(value)


def _convert_properties(props: dict[str, Any]) -> dict[str, Any]:
    """Convert Memgraph property dictionary to CypherGraphDB compatible format.

    Args:
        props: Dictionary of properties from Memgraph.

    Returns:
        Dictionary of converted properties.
    """
    if not props:
        return {}

    return {key: _convert_property_value(value) for key, value in props.items()}


def _convert_node(node: Any, model_provider: ModelProvider | None = None) -> Node:
    """Convert a Memgraph Node to CypherGraphDB Node.

    Args:
        node: Memgraph Node object.
        model_provider: Optional model provider for node conversion.

    Returns:
        CypherGraphDB Node object.
    """
    if node is None:
        return None

    # Extract node data
    node_id = node.id
    labels = list(node.labels)
    properties = _convert_properties(node.properties)

    # Create the node using model provider if available
    if model_provider:
        return model_provider.create_node(
            id=node_id,
            label=labels[0] if labels else None,
            props=properties,
        )

    # Create a standard Node
    return Node(id_=node_id, label_=labels[0] if labels else None, properties_=properties)


def _convert_relationship(rel: Any, model_provider: ModelProvider | None = None) -> Edge:
    """Convert a Memgraph Relationship to CypherGraphDB Edge.

    Args:
        rel: Memgraph Relationship object.
        model_provider: Optional model provider for edge conversion.

    Returns:
        CypherGraphDB Edge object.
    """
    if rel is None:
        return None

    # Extract relationship data
    rel_id = rel.id
    rel_type = rel.type
    start_id = rel.start_id
    end_id = rel.end_id
    properties = _convert_properties(rel.properties)

    # Create the edge using model provider if available
    if model_provider:
        return model_provider.create_edge(
            id=rel_id,
            label=rel_type,
            start_id=start_id,
            end_id=end_id,
            props=properties,
        )

    # Create a standard Edge
    return Edge(id_=rel_id, label_=rel_type, start_id_=start_id, end_id_=end_id, properties_=properties)


def _convert_path(path: Any, model_provider: ModelProvider | None = None) -> Path:
    """Convert a Memgraph Path to CypherGraphDB Path.

    Args:
        path: Memgraph Path object.
        model_provider: Optional model provider for path conversion.

    Returns:
        CypherGraphDB Path object.
    """
    if path is None:
        return None

    # Validate path structure: nodes must equal relationships + 1
    assert len(path.nodes) == len(path.relationships) + 1, (
        f"Invalid path structure: {len(path.nodes)} nodes != {len(path.relationships)} relationships + 1"
    )

    # Build the entity sequence directly: node -> rel -> node -> rel -> node
    # In a path, entities alternate between nodes and relationships
    entities = []
    for i, node in enumerate(path.nodes):
        entities.append(_convert_node(node, model_provider))
        if i < len(path.relationships):  # Add relationship if it exists
            rel = path.relationships[i]
            entities.append(_convert_relationship(rel, model_provider))

    # Create the path
    return Path(entities=entities)


def _convert_memgraph_value(value: Any, exec_stats: ExecStatistics, model_provider: Any) -> Any:
    """Convert any Memgraph value to CypherGraphDB compatible format.

    This function handles all possible Memgraph types and converts them to
    appropriate CypherGraphDB types. Also updates execution statistics if
    provided.

    Args:
        value: Value from Memgraph to convert.
        model_provider: Optional model provider for object conversion.
        exec_stats: Execution statistics to update during conversion.
        update_stats: Whether to update execution statistics for nodes/edges.

    Returns:
        Converted value compatible with CypherGraphDB.
    """

    # Handle different Memgraph types
    if isinstance(value, mgclient.Node):
        exec_stats.nodes += 1
        return _convert_node(value, model_provider)
    elif isinstance(value, mgclient.Relationship):
        exec_stats.edges += 1
        return _convert_relationship(value, model_provider)
    elif isinstance(value, mgclient.Path):
        exec_stats.paths += 1
        return _convert_path(value, model_provider)
    elif isinstance(value, list | tuple):
        return [_convert_memgraph_value(v, exec_stats, model_provider) for v in value]
    elif isinstance(value, dict):
        return {k: _convert_memgraph_value(v, exec_stats, model_provider) for k, v in value.items()}
    else:
        # Return primitive values as is
        exec_stats.values += 1
        return value


def memgraph_row_factory(
    exec_stats: ExecStatistics, model_provider: Any = None
) -> Callable[[tuple[Any, ...]], tuple[GraphObject, ...]]:
    """Create a row factory function for Memgraph query results.

    Args:
        exec_stats: Execution statistics object to update.
        model_provider: Optional model provider for object conversion.

    Returns:
        Function that converts a row of Memgraph results to CypherGraphDB objects.
    """

    def row_factory(row: tuple[Any, ...]) -> tuple[GraphObject, ...]:
        """Convert a row of Memgraph results to CypherGraphDB objects.

        Args:
            row: Row of results from Memgraph.

        Returns:
            Tuple of converted CypherGraphDB objects.
        """
        if not row:
            return ()

        # Convert each value in the row (statistics are updated within _convert_memgraph_value)
        result = tuple(_convert_memgraph_value(value, exec_stats, model_provider) for value in row)

        # Update row count statistics
        exec_stats.row_count += 1
        return result

    return row_factory
