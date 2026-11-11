"""statistics module: Compute and serialize graph and label statistics.

This module provides:
    - GraphStatistics: Summarize nodes, edges, paths, and values in a Graph or query result.
    - LabelStatistics: Serialize per-label counts for a specific graph.
    - IndexType: Enumeration of index types (property, unique, fulltext, vector).
    - IndexInfo: Backend-agnostic index metadata model.
"""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_serializer

from .models import Graph, GraphEdge, GraphNode, GraphObjectType, GraphPath


class GraphStatistics(BaseModel):
    """Compute summary statistics for a Graph or query result."""

    nodes: int = 0
    edges: int = 0
    paths: int = 0
    values: int = 0
    hops: int = 0
    row_count: int = 0
    col_count: int = 0
    node_labels: dict[str, int] = {}
    edge_labels: dict[str, int] = {}

    def is_empty(self) -> bool:
        """Check if the statistics represent an empty result.

        Returns:
            True if there are no nodes, edges, paths, or values in the result.
        """
        return self.nodes == 0 and self.edges == 0 and self.paths == 0 and self.values == 0

    def has_graph_data(self) -> bool:
        """Check if the statistics represent graph data.

        Returns:
            True if there are nodes or edges in the result.
        """
        return self.nodes > 0 or self.edges > 0

    def has_tabular_data(self) -> bool:
        """Check if the statistics represent tabular data.

        Returns:
            True if there are values or paths in the result or if the edges exist without nodes.
        """
        return self.values > 0 or self.paths > 0 or (self.edges > 0 and self.nodes == 0)

    def has_data(self) -> bool:
        """Check if the statistics represent any data.

        Returns:
            True if there is any data in the result.
        """
        return not self.is_empty()

    @classmethod
    def create_from_graph(cls: type[GraphStatistics], graph: Graph) -> GraphStatistics:
        """Build statistics from an existing Graph object.

        Args:
            cls: The GraphStatistics class.
            graph: Graph model instance to summarize.

        Returns:
            An instance of GraphStatistics populated from the graph.

        """
        stats = GraphStatistics()

        for node in graph.nodes.values():
            stats.track_node(node)

        for edge in graph.edges.values():
            stats.track_edge(edge)

        return stats

    @classmethod
    def create_from_result(cls: type[GraphStatistics], result: Any) -> GraphStatistics:
        """Build statistics by walking through a query result structure.

        Args:
            cls: The GraphStatistics class.
            result: Query result, potentially nested values, GraphNode, GraphEdge, or collections.

        Returns:
            An instance of GraphStatistics populated from the result.

        """
        stats = GraphStatistics()

        def walk_result(value):
            nonlocal stats

            if isinstance(value, GraphNode):
                stats.track_node(value)
            elif isinstance(value, GraphEdge):
                stats.track_edge(value)
            elif isinstance(value, GraphPath | dict | tuple | list):
                for item in value:
                    walk_result(item)
            else:
                stats.values += 1

        if not result:
            walk_result(result)

        return stats

    def track_node(self, node: GraphNode) -> None:
        """Increment node count and record the node's label frequency."""
        self.nodes += 1
        if node.label_ is not None:
            self.node_labels[node.label_] = self.node_labels.get(node.label_, 0) + 1

    def track_edge(self, edge: GraphEdge) -> None:
        """Increment edge count and record the edge's label frequency."""
        self.edges += 1
        if edge.label_ is not None:
            self.edge_labels[edge.label_] = self.edge_labels.get(edge.label_, 0) + 1

    def __str__(self) -> str:
        """Return a formatted summary of statistics."""
        return (
            f"nodes={self.nodes}, ({len(self.node_labels)} label(s)), "
            f"edges={self.edges} ({len(self.edge_labels)} label(s)), "
            f"values={self.values}, "
            f"paths={self.paths}, "
            f"rows={self.row_count}, "
            f"cols={self.col_count}"
        )


class LabelStatistics(BaseModel):
    """Serialize count statistics for a single label in a graph."""

    graph_name: str
    label_: str
    type_: GraphObjectType
    count: int = -1

    @model_serializer
    def serialize_model(self, _) -> dict[str, Any]:
        """Convert LabelStatistics to a serializable dictionary."""
        return {
            "graph": self.graph_name,
            "label_": self.label_,
            "type_": self.type_.name,
            "count": self.count,
        }


class IndexType(StrEnum):
    """Enumeration of index types supported across graph backends."""

    PROPERTY = "property"  # Range/GIN/BTREE property lookup index
    UNIQUE = "unique"  # Unique constraint index
    FULLTEXT = "fulltext"  # Full-text search index
    VECTOR = "vector"  # Vector similarity index
    OTHER = "other"  # A kind not normalized (spatial, text, ...): the backend's own type is in IndexInfo.details


class IndexInfo(BaseModel):
    """Backend-agnostic index metadata.

    The fields mean the same on every backend (AGE, Memgraph, Neo4j, FalkorDB);
    what a backend reports beyond them is kept, as reported, in ``details``.

    Attributes:
        label: Node label or edge type the index is on (e.g. "Method").
        object_type: Whether the index is on nodes or on edges.
        property_names: The properties the index is keyed on, in order. None
            for an index keyed on no single property: a label index, or an
            AGE GIN index over the whole ``properties`` column -- which serves
            containment, not ``n.prop = value``.
        index_type: The kind of index (property, unique, fulltext, vector).
        index_name: Backend-specific index name, used for drop_index.
        unique: Whether the index enforces uniqueness.
        details: Backend-specific information as the backend reports it (its
            own index type, definition, state, entry count, ...), so the
            normalization loses nothing.
    """

    label: str
    object_type: GraphObjectType = GraphObjectType.NODE
    property_names: list[str] | None = None
    index_type: IndexType = IndexType.PROPERTY
    index_name: str | None = None
    unique: bool = False
    details: dict[str, Any] = Field(default_factory=dict)

    @property
    def lookup_property(self) -> str | None:
        """The property an equality lookup on it (``MATCH (n:Label {prop: $v})``) uses this index for, else None.

        Only a property or unique index keyed on exactly one property: a
        composite index is not used for a lookup on one of its properties on
        every backend (Neo4j plans a label scan), and a GIN or label index
        serves no property equality at all.
        """
        if (
            self.index_type not in (IndexType.PROPERTY, IndexType.UNIQUE)
            or not self.property_names
            or len(self.property_names) != 1
        ):
            return None
        return self.property_names[0]

    @model_serializer
    def serialize_model(self, _) -> dict[str, Any]:
        """Convert IndexInfo to a serializable dictionary."""
        return {
            "label": self.label,
            "object_type": self.object_type.name.lower(),
            "property_names": self.property_names,
            "index_type": self.index_type.value,
            "index_name": self.index_name,
            "unique": self.unique,
            "details": self.details,
        }
