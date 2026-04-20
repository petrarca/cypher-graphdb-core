"""Indexing and bulk write mixin for CypherGraphDB."""

from ..backend import BackendCapability
from ..statistics import IndexInfo


class IndexingMixin:
    """Mixin providing index management and bulk write operations for CypherGraphDB."""

    def has_capability(self, capability: BackendCapability) -> bool:
        """Check if the backend supports a specific capability.

        Args:
            capability: The capability to check.

        Returns:
            True if the capability is supported, False otherwise.
        """
        assert self._backend
        return self._backend.has_capability(capability)

    def create_property_index(self, label: str, *property_names: str) -> None:
        """Create a property index on the given label.

        For backends like AGE where a single GIN index covers all properties,
        the property_names parameter may be ignored. For backends like Neo4j,
        Memgraph, and FalkorDB, each property gets its own index.

        Consumers should always pass property_names for portability across
        backends.

        Args:
            label: Node label to index (e.g. "Method").
            *property_names: Property names to index.

        Examples:
            ```python
            with CypherGraphDB("age") as cdb:
                cdb.connect()

                # Create indexes on common lookup properties
                for label in ("Module", "File", "Class", "Method"):
                    cdb.create_property_index(label, "id", "name")

                # Check capability before calling
                if cdb.has_capability(BackendCapability.PROPERTY_INDEX):
                    cdb.create_property_index("Function", "id")
            ```
        """
        assert self._backend
        self._backend.create_property_index(label, *property_names)

    def drop_index(self, label: str, *property_names: str) -> None:
        """Drop a property index on the given label.

        Args:
            label: Node label whose index to drop.
            *property_names: Property names of the index.
        """
        assert self._backend
        self._backend.drop_index(label, *property_names)

    def list_indexes(self) -> list[IndexInfo]:
        """List all indexes on the current graph.

        Returns:
            List of IndexInfo objects describing each index.

        Examples:
            ```python
            with CypherGraphDB("age") as cdb:
                cdb.connect()

                indexes = cdb.list_indexes()
                for idx in indexes:
                    print(f"{idx.label}: {idx.index_type.value} "
                          f"({idx.property_names or 'all properties'})")
            ```
        """
        assert self._backend
        return self._backend.list_indexes()

    def bulk_create_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        """Create nodes in batches.

        Uses UNWIND for efficient bulk insertion. The exact mechanism is
        backend-specific (inline literals for AGE, parameterized for others).

        Args:
            label: Node label for all created nodes.
            rows: List of property dicts, one per node.
            batch_size: Number of nodes per batch.

        Returns:
            Total number of nodes created.

        Examples:
            ```python
            with CypherGraphDB("age") as cdb:
                cdb.connect()

                nodes = [
                    {"id": "mod_1", "name": "main", "filepath": "main.py"},
                    {"id": "mod_2", "name": "utils", "filepath": "utils.py"},
                ]
                count = cdb.bulk_create_nodes("Module", nodes)
                cdb.commit()
                print(f"Created {count} nodes")
            ```
        """
        assert self._backend
        return self._backend.bulk_create_nodes(label, rows, batch_size)

    def bulk_create_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str = "",
        dst_label: str = "",
        src_key: str = "id",
        dst_key: str = "id",
        batch_size: int = 500,
    ) -> int:
        """Create edges in batches by matching src/dst nodes on a key property.

        Each dict in edges must have "src" and "dst" keys whose values match
        the src_key/dst_key properties on source/destination nodes.

        Args:
            label: Edge label for all created edges.
            edges: List of dicts with at least "src" and "dst" keys.
            src_label: Label of source nodes (empty string for any label).
            dst_label: Label of destination nodes (empty string for any label).
            src_key: Property name on source nodes to match against "src".
            dst_key: Property name on destination nodes to match against "dst".
            batch_size: Number of edges per batch.

        Returns:
            Total number of edges created.

        Examples:
            ```python
            with CypherGraphDB("age") as cdb:
                cdb.connect()

                edges = [
                    {"src": "class_Foo", "dst": "method_bar"},
                    {"src": "class_Foo", "dst": "method_baz"},
                ]
                count = cdb.bulk_create_edges(
                    "HAS_METHOD", edges,
                    src_label="Class", dst_label="Method",
                )
                cdb.commit()
                print(f"Created {count} edges")
            ```
        """
        assert self._backend
        return self._backend.bulk_create_edges(
            label,
            edges,
            src_label=src_label,
            dst_label=dst_label,
            src_key=src_key,
            dst_key=dst_key,
            batch_size=batch_size,
        )
