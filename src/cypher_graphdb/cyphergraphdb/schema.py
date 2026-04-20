"""Schema and introspection mixin for CypherGraphDB."""

from typing import Any

from .. import graphops as gops
from .. import utils
from ..cypherbuilder import CypherBuilder
from ..models import Graph
from ..statistics import LabelStatistics


class SchemaMixin:
    """Mixin providing schema introspection methods for CypherGraphDB."""

    def graphs(self) -> list[str]:
        """Get list of available graphs in the database backend.

        Returns:
            Sorted list of graph names.
        """
        assert self._backend
        return sorted(self._backend.graphs())

    def graph_exists(self, graph_name: str) -> bool:
        """Check if a graph exists in the database backend.

        Args:
            graph_name: Name of the graph to check.

        Returns:
            True if the graph exists, False otherwise.
        """
        assert self._backend
        return self._backend.graph_exists(graph_name)

    def create_graph(self, graph_name: str) -> None:
        """Create a new graph in the database backend.

        Args:
            graph_name: Name of the graph to create.
        """
        assert self._backend
        self._backend.create_graph(graph_name)

    def drop_graph(self, graph_name: str) -> None:
        """Drop a graph from the database backend.

        Args:
            graph_name: Name of the graph to drop.
        """
        assert self._backend
        self._backend.drop_graph(graph_name)

    def labels(self) -> list[LabelStatistics]:
        """Get statistics for all labels (node types and relationship types)
        in the current graph.

        Provides comprehensive metadata about the graph schema including node
        and edge label usage statistics. Useful for schema discovery and data
        profiling.

        Returns:
            List of LabelStatistics objects sorted by type (nodes first) then
            by name

        Examples:
            ```python
            # Using context manager (recommended)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Get all label statistics
                label_stats = cdb.labels()

                for label_stat in label_stats:
                    print(f"Label: {label_stat.label_}")
                    print(f"Type: {label_stat.type_}")  # node or relationship
                    print(f"Count: {label_stat.count_}")
                    print(f"Properties: {label_stat.property_names}")
                    print("---")

                # Filter for just node labels
                node_labels = [
                    stat for stat in label_stats
                    if stat.type_.value == "node"
                ]
                print(f"Node types: {[stat.label_ for stat in node_labels]}")

                # Filter for specific node types
                product_tech_labels = [
                    stat for stat in label_stats
                    if stat.label_ in ["Product", "Technology"]
                ]
                for stat in product_tech_labels:
                    print(f"{stat.label_}: {stat.count_} instances")

            # Find the most common relationship type
            rel_labels = [
                stat for stat in label_stats
                if stat.type_.value == "relationship"
            ]
            if rel_labels:
                most_common_rel = max(rel_labels, key=lambda x: x.count_)
                print(
                    "Most common relationship: "
                    f"{most_common_rel.label_} with {most_common_rel.count_} instances"
                )

            # Get schema overview for Product and Technology
            schema = {}
            for stat in label_stats:
                if stat.label_ in ["Product", "Technology", "USES_TECHNOLOGY"]:
                    schema[stat.label_] = {
                        'type': stat.type_.value,
                        'count': stat.count_,
                        'properties': stat.property_names or []
                    }
            print(f"Schema subset: {schema}")
            ```
        """
        assert self._backend

        result = self._backend.labels()

        # sort by type and then by label name
        return sorted(result, key=lambda x: (x.type_.value, x.label_))

    def nest_result(self, result: Any) -> Any:
        """Convert query results to consistent nested tuple format for
        internal processing.

        Normalizes different result formats into a consistent list-of-tuples
        structure used internally by the library. Primarily used by result
        processing utilities.

        Args:
            result: Query result in any format (single value, tuple, list,
                etc.)

        Returns:
            Normalized result as list of tuples, or None if input is None

        Examples:
            ```python
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Single value -> [(value,)]
                nested = cdb.nest_result(42)
                # Returns: [(42,)]

                # Tuple -> [tuple]
                nested = cdb.nest_result(("Alice", 30))
                # Returns: [("Alice", 30)]

                # List is returned as-is
                nested = cdb.nest_result([("Alice", 30), ("Bob", 25)])
                # Returns: [("Alice", 30), ("Bob", 25)]

                # None -> None
                nested = cdb.nest_result(None)
                # Returns: None
            ```

        Note:
            This method is primarily for internal use by result processing
            utilities. Most users should use the unnest_result parameter in
            execute/fetch methods instead.
        """
        if result is None:
            return None

        if isinstance(result, list):
            return result
        if isinstance(result, tuple):
            return [result]

        return [(result,)]

    def resolve_edges(self, graph: Graph) -> set[int] | None:
        if graph is None:
            return None

        if not (missing_nodes := gops.missing_nodes(graph)):
            return missing_nodes

        for batch in utils.chunk_list(list(missing_nodes), 50):
            graph.merge(self._parse_and_execute(CypherBuilder.fetch_nodes_by_ids(batch)))

        return missing_nodes
