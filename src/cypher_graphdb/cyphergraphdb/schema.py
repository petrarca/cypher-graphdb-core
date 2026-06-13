"""Schema and introspection mixin for CypherGraphDB."""

import re
from typing import Any

from loguru import logger

from .. import graphops as gops
from .. import utils
from ..cypherbuilder import CypherBuilder
from ..models import Graph
from ..statistics import GraphObjectType, LabelStatistics

# Safe label name pattern: Cypher node labels must be valid identifiers.
# This guards against injection when label names are interpolated into Cypher.
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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

    def node_labels(self) -> list[str]:
        """Return all node labels present in the current graph.

        Resolved from the backend label catalog (no per-node scan), so it is
        cheap even on large graphs. Sorted, de-duplicated.
        """
        return sorted({s.label_ for s in self.labels() if s.type_ == GraphObjectType.NODE})

    def edge_types(self) -> list[str]:
        """Return all edge (relationship) types present in the current graph.

        Resolved from the backend label catalog (no per-edge scan). Sorted,
        de-duplicated.
        """
        return sorted({s.label_ for s in self.labels() if s.type_ == GraphObjectType.EDGE})

    def mass_delete(
        self,
        *,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> int:
        """Bulk-delete nodes (and their relationships) by label filter.

        A backend-portable mass delete keyed on node labels resolved from the
        catalog (``node_labels()``), deleting each selected label with
        ``MATCH (n:Label) DETACH DELETE n``. Per-label deletion is used because
        not all backends (e.g. Apache AGE) support label predicates or list
        functions in a single ``WHERE`` clause.

        Args:
            include: If given, only these node labels are deleted. When ``None``,
                all node labels are considered.
            exclude: Labels to preserve (e.g. infrastructure nodes like
                ``_GraphModel`` / ``_NamedQuery``). Applied after ``include``.

        Returns:
            Total number of nodes deleted across all matched labels.

        Examples:
            ```python
            # Wipe all domain nodes but keep graph metadata nodes
            cdb.mass_delete(exclude=["_GraphModel", "_NamedQuery"])

            # Delete only specific labels
            cdb.mass_delete(include=["Product", "Component"])
            ```

        Raises:
            ReadOnlyModeError: If the connection is in read-only mode.
        """
        targets = set(self.node_labels())
        if include is not None:
            targets &= set(include)
        if exclude is not None:
            targets -= set(exclude)

        deleted = 0
        for label in sorted(targets):
            if not _SAFE_LABEL_RE.match(label):
                logger.warning("mass_delete: skipping label with unsafe name {!r}", label)
                continue
            result = self.execute(f"MATCH (n:{label}) DETACH DELETE n RETURN count(n)", unnest_result=True)
            if isinstance(result, int):
                deleted += result
            else:
                logger.warning("mass_delete: unexpected non-int result for label {!r}: {!r}", label, result)
        return deleted

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
