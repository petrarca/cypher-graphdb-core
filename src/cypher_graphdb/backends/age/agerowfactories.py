"""AGE row factories module: Custom row factories for Apache AGE graph data.

This module provides custom row factory classes for converting raw Apache AGE
result rows into graph objects (nodes, edges, paths) using registered model
providers.
"""

from collections.abc import Sequence
from typing import Any

import age
from psycopg.cursor import BaseCursor
from psycopg.rows import BaseRowFactory, RowMaker, T

from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge, GraphNode, GraphPath


def _create_graph_node(value: age.models.Vertex, stats: ExecStatistics, provider: ModelProvider) -> GraphNode:
    """Create a GraphNode from an AGE Vertex."""
    node = provider.create_node(value.label, value.properties, id=value.id)
    stats.track_node(node)
    return node


def _create_graph_edge(value: age.models.Edge, stats: ExecStatistics, provider: ModelProvider) -> GraphEdge:
    """Create a GraphEdge from an AGE Edge."""
    edge = provider.create_edge(value.label, value.start_id, value.end_id, value.properties, id=value.id)
    stats.track_edge(edge)
    return edge


def _create_graph_path(value, stats: ExecStatistics, provider: ModelProvider) -> GraphPath:
    """Create a GraphPath from an AGE Path."""
    path = GraphPath()

    for entity in value.entities:
        if isinstance(entity, age.models.Vertex):
            # Don't update stats here, will be updated when path stats are updated
            node = provider.create_node(entity.label, entity.properties, id=entity.id)
            path.append(node)
        elif isinstance(entity, age.models.Edge):
            # Don't update stats here, will be updated when path stats are updated
            edge = provider.create_edge(entity.label, entity.start_id, entity.end_id, entity.properties, id=entity.id)
            path.append(edge)
        else:
            path.append(entity)

    stats.paths += 1
    return path


def _map_age_value(value, stats: ExecStatistics, provider: ModelProvider):
    """Map an AGE value to the appropriate graph object type."""
    if isinstance(value, age.models.Vertex):
        return _create_graph_node(value, stats, provider)
    elif isinstance(value, age.models.Edge):
        return _create_graph_edge(value, stats, provider)
    elif isinstance(value, age.models.Path):
        return _create_graph_path(value, stats, provider)
    else:
        # Only count non-null values (matching Memgraph behavior)
        if value is not None:
            stats.values += 1
        return value


def _process_row_values(values: Sequence[Any], stats: ExecStatistics, provider: ModelProvider) -> tuple:
    """Process a row of values from the database result."""
    result = []

    for value in values:
        if isinstance(value, list):
            # values can contain list of edges, like match ()-[e:*]->() return e
            val = [_map_age_value(v, stats, provider) for v in value]
            stats.hops += 1
        else:
            val = _map_age_value(value, stats, provider)

        result.append(val)

    stats.row_count += 1
    return tuple(result)


def age_row_factory(stats: ExecStatistics, provider: ModelProvider) -> BaseRowFactory[T]:
    """Create a row factory for AGE query results.

    This factory converts AGE-specific types to CypherGraphDB model objects.

    Args:
        stats: Statistics object to track query execution metrics
        provider: Model provider for creating graph objects

    Returns:
        A row factory compatible with psycopg cursors
    """

    def age_row_maker_(cursor: BaseCursor[Any, Any]) -> RowMaker[T]:
        # Update column statistics
        stats.col_count = len(cursor.description)

        def age_row_maker__(values: Sequence[Any]) -> T:
            return _process_row_values(values, stats, provider)

        return age_row_maker__

    return age_row_maker_
