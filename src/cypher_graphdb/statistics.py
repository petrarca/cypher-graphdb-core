"""statistics module: Compute and serialize graph and label statistics.

This module provides:
    - GraphStatistics: Summarize nodes, edges, paths, and values in a Graph or query result.
    - LabelStatistics: Serialize per-label counts for a specific graph.
"""

from typing import Any

from pydantic import BaseModel, model_serializer

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

    @classmethod
    def create_from_graph(cls: type["GraphStatistics"], graph: Graph) -> "GraphStatistics":
        """Build statistics from an existing Graph object.

        Args:
            cls: The GraphStatistics class.
            graph: Graph model instance to summarize.

        Returns:
            An instance of GraphStatistics populated from the graph.

        """
        stats = GraphStatistics()

        for node in graph.nodes:
            stats.track_node(node)

        for edge in graph.edges:
            stats.track_edge(edge)

        return stats

    @classmethod
    def create_from_result(cls: type["GraphStatistics"], result: Any) -> "GraphStatistics":
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
        if node.label_ in self.node_labels:
            self.node_labels[node.label_] += 1
        else:
            self.node_labels.update({node.label_: 1})

    def track_edge(self, edge: GraphEdge) -> None:
        """Increment edge count and record the edge's label frequency."""
        self.edges += 1
        if edge.label_ in self.edge_labels:
            self.edge_labels[edge.label_] += 1
        else:
            self.edge_labels.update({edge.label_: 1})

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
