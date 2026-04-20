"""Indexing and bulk write mixin for CypherGraphDB."""

from collections.abc import Sequence
from typing import Any

from ..backend import BackendCapability
from ..models import GraphEdge, GraphNode
from ..statistics import IndexInfo
from .bulk_normalize import normalize_edges_input, normalize_nodes_input


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

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:
        """List all indexes on the current graph.

        Args:
            include_internal: If True, also return backend-internal indexes
                (e.g. AGE's _pkey, _start_id_idx, _end_id_idx). Default False.

        Returns:
            List of IndexInfo objects describing each index.

        Examples:
            ```python
            with CypherGraphDB("age") as cdb:
                cdb.connect()

                # User-created indexes only (default)
                indexes = cdb.list_indexes()
                for idx in indexes:
                    print(f"{idx.label}: {idx.index_type.value} "
                          f"({idx.property_names or 'all properties'})")

                # All indexes including AGE internals (diagnostics)
                all_indexes = cdb.list_indexes(include_internal=True)
            ```
        """
        assert self._backend
        return self._backend.list_indexes(include_internal=include_internal)

    def bulk_create_nodes(
        self,
        rows: Sequence[dict] | Sequence[GraphNode],
        label: str | None = None,
        batch_size: int = 200,
    ) -> int:
        """Create nodes in batches.

        Uses UNWIND for efficient bulk insertion. Accepts either plain
        property dicts (traditional shape) or decorated ``GraphNode``
        instances (typed shape). For typed input, the label is derived from
        the instances' ``graph_info_`` and ``label`` becomes optional.

        Args:
            rows: Either a list of property dicts or a list of ``GraphNode``
                instances (all sharing the same label). An empty list is
                accepted and returns 0.
            label: Node label. Required when ``rows`` are dicts; optional for
                typed input (if given, must match the instances' label).
            batch_size: Number of nodes per batch.

        Returns:
            Total number of nodes created.

        Examples:
            Dict shape (backend-agnostic, no model required)::

                nodes = [
                    {"id": "mod_1", "name": "main", "filepath": "main.py"},
                    {"id": "mod_2", "name": "utils", "filepath": "utils.py"},
                ]
                cdb.bulk_create_nodes(nodes, label="Module")

            Typed shape (requires ``@node()`` decorated class)::

                modules = [Module(id="mod_1", name="main"), Module(id="mod_2", name="utils")]
                cdb.bulk_create_nodes(modules)
        """
        assert self._backend
        derived_label, dict_rows = normalize_nodes_input(rows, label)
        return self._backend.bulk_create_nodes(derived_label, dict_rows, batch_size)

    def bulk_create_edges(
        self,
        edges: Sequence[dict] | Sequence[GraphEdge],
        src_refs: Sequence[Any] | None = None,
        dst_refs: Sequence[Any] | None = None,
        label: str | None = None,
        src_label: str = "",
        dst_label: str = "",
        src_ref_prop: str = "id",
        dst_ref_prop: str = "id",
        batch_size: int = 500,
    ) -> int:
        """Create edges in batches by matching src/dst nodes on a reference property.

        Accepts either plain dicts (traditional shape) or decorated
        ``GraphEdge`` instances (typed shape).

        For dict shape, each entry must have ``"src"`` and ``"dst"`` keys
        whose values match the ``src_ref_prop`` / ``dst_ref_prop`` properties
        on source/destination nodes. Extra keys become edge properties.

        For typed shape, the match values are supplied via the parallel
        ``src_refs`` / ``dst_refs`` lists; the edge properties come from
        ``flatten_properties()`` on each instance.

        Args:
            edges: Either a list of dicts (each with ``src``/``dst`` + props)
                or a list of ``GraphEdge`` instances (all sharing the same
                label). An empty list is accepted and returns 0.
            src_refs: Parallel list of source match values. Required for
                typed input; must be None for dict input.
            dst_refs: Parallel list of destination match values. Required for
                typed input; must be None for dict input.
            label: Edge label. Required for dicts; optional for typed input.
            src_label: Label of source nodes (empty string for any label).
            dst_label: Label of destination nodes (empty string for any label).
            src_ref_prop: Property name on source nodes to match against src refs.
            dst_ref_prop: Property name on destination nodes to match against dst refs.
            batch_size: Number of edges per batch.

        Returns:
            Total number of edges created.

        Examples:
            Dict shape::

                edges = [
                    {"src": "class_Foo", "dst": "method_bar"},
                    {"src": "class_Foo", "dst": "method_baz"},
                ]
                cdb.bulk_create_edges(edges, label="HAS_METHOD",
                                      src_label="Class", dst_label="Method")

            Typed shape::

                calls = [Calls(confidence="high"), Calls(confidence="medium")]
                cdb.bulk_create_edges(
                    calls,
                    src_refs=["mod.foo.bar", "mod.foo.baz"],
                    dst_refs=["mod.target.a", "mod.target.b"],
                    src_label="Method", dst_label="Method",
                    src_ref_prop="qualified_name",
                    dst_ref_prop="qualified_name",
                )
        """
        assert self._backend
        derived_label, edge_dicts = normalize_edges_input(edges, src_refs, dst_refs, label)
        return self._backend.bulk_create_edges(
            derived_label,
            edge_dicts,
            src_label=src_label,
            dst_label=dst_label,
            src_ref_prop=src_ref_prop,
            dst_ref_prop=dst_ref_prop,
            batch_size=batch_size,
        )
