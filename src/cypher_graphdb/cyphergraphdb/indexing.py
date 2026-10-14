"""Indexing and bulk write mixin for CypherGraphDB."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ..backend import BackendCapability
from ..models import GraphEdge, GraphNode
from ..statistics import IndexInfo
from .bulk_normalize import normalize_edges_input, normalize_nodes_input

if TYPE_CHECKING:
    from ._host import CypherGraphDBHost as _Host
else:
    _Host = object


class IndexingMixin(_Host):
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

        Always pass ``property_names`` -- no backend treats them as optional
        decoration:

        - **AGE** creates a GIN index on the whole ``properties`` column *and*
          a btree expression index per named property. The GIN index does not
          serve a Cypher ``WHERE n.prop = ...`` lookup, which compiles to
          ``agtype_access_operator``; without the per-property btree indexes
          every such lookup is a sequential scan.
        - **Neo4j, Memgraph, FalkorDB** create one index per property.

        Args:
            label: Node label to index (e.g. "Method").
            *property_names: Property names to index. Omitting them yields at
                best a partial index and at worst none.

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

    def bulk_delete_nodes(self, label: str, filters: dict[str, str]) -> int:
        """Delete all nodes of a label matching property filters, cascading to edges.

        Delegates to the backend's ``bulk_delete_nodes``. Backends that declare
        ``BULK_DELETE`` capability provide an optimized implementation; others
        fall back to Cypher DETACH DELETE.

        Args:
            label: Node label to delete from (e.g. "Method", "Class").
            filters: Property key=value filters (AND semantics).
                     Example: {"source_key": "nais-platform", "lang": "ts"}

        Returns:
            Number of nodes deleted.
        """
        assert self._backend
        return self._backend.bulk_delete_nodes(label, filters)

    def bulk_delete_orphans(
        self, label: str, edge_label: str, *, incoming: bool = True, filters: dict[str, str] | None = None
    ) -> int:
        """Delete nodes of a label that have no edge of ``edge_label`` attached.

        Garbage-collects shared nodes (e.g. Dependency, License) after the
        nodes referencing them have been removed, or removes dangling
        source-scoped nodes after a re-index. Delegates to the backend's
        ``bulk_delete_orphans``. Backends that declare ``BULK_DELETE_ORPHANS``
        provide an optimized implementation (direct SQL anti-join on AGE);
        others fall back to a Cypher ``OPTIONAL MATCH ... WHERE x IS NULL``
        pattern (used by Memgraph).

        Args:
            label: Node label to scan for orphans (e.g. "Dependency").
            edge_label: Edge label whose absence marks the node as orphaned
                (e.g. "HAS_DEPENDENCY").
            incoming: If True (default), check for incoming edges
                ``(n)<-[:edge_label]-()``. If False, outgoing.
            filters: Optional property key=value filters (AND semantics) to
                restrict the orphan scan (e.g. ``{"source_key": "my-source"}``).

        Returns:
            Number of orphan nodes deleted.
        """
        assert self._backend
        return self._backend.bulk_delete_orphans(label, edge_label, incoming=incoming, filters=filters)

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
        src_label: str,
        dst_label: str,
        src_refs: Sequence[Any] | None = None,
        dst_refs: Sequence[Any] | None = None,
        label: str | None = None,
        src_ref_prop: str | Sequence[str] = "id",
        dst_ref_prop: str | Sequence[str] = "id",
        batch_size: int = 500,
    ) -> int:
        """Create edges in batches by matching src/dst nodes on a reference key.

        Accepts either plain dicts (traditional shape) or decorated
        ``GraphEdge`` instances (typed shape).

        For dict shape, each entry must have ``"src"`` and ``"dst"`` keys
        whose values match the ``src_ref_prop`` / ``dst_ref_prop`` properties
        on source/destination nodes. Extra keys become edge properties.

        For typed shape, the match values are supplied via the parallel
        ``src_refs`` / ``dst_refs`` lists; the edge properties come from
        ``flatten_properties()`` on each instance.

        A ref prop may name a **single** property (the common case) or an ordered
        **sequence** of properties forming a composite key. A composite key is the
        correct choice when no single property is unique within the label (e.g. the
        same ``symbol`` exists in several sources of a shared graph): matching on
        ``("symbol", "source_key")`` binds each edge to the intended node instead of
        an arbitrary one that happens to share the symbol. For a composite key each
        ``src`` / ``dst`` (or ``src_refs`` / ``dst_refs`` element) is a parallel
        sequence of values, in the same order as the props.

        Args:
            edges: Either a list of dicts (each with ``src``/``dst`` + props)
                or a list of ``GraphEdge`` instances (all sharing the same
                label). An empty list is accepted and returns 0.
            src_refs: Parallel list of source match values. Required for
                typed input; must be None for dict input.
            dst_refs: Parallel list of destination match values. Required for
                typed input; must be None for dict input.
            label: Edge label. Required for dicts; optional for typed input.
            src_label: Label of source nodes. Required for optimal performance on AGE.
            dst_label: Label of destination nodes. Required for optimal performance on AGE.
            src_ref_prop: Property, or ordered sequence of properties (composite
                key), on source nodes to match against src refs.
            dst_ref_prop: As ``src_ref_prop``, for destination nodes.
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

            Composite key (disambiguate a non-unique property)::

                cdb.bulk_create_edges(
                    calls,
                    src_refs=[("mod.foo.bar", "svc-a"), ("mod.foo.baz", "svc-a")],
                    dst_refs=[("mod.target.a", "svc-a"), ("mod.target.b", "svc-a")],
                    src_label="Method", dst_label="Method",
                    src_ref_prop=("qualified_name", "source_key"),
                    dst_ref_prop=("qualified_name", "source_key"),
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
