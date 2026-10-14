"""Integration tests for index management and bulk write operations.

Tests create_property_index, drop_index, list_indexes, bulk_create_nodes,
and bulk_create_edges for both Memgraph and AGE backends.
"""

import pytest

from cypher_graphdb import BackendCapability, GraphEdge, GraphNode, ModelProvider

mp = ModelProvider()

pytestmark = pytest.mark.integration


# ── Typed model fixtures (shared across typed-path tests) ────────────────────


@mp.node()
class _ITPerson(GraphNode):
    """Integration-test typed person node."""

    id: str
    name: str
    age: int = 0


@mp.edge(label="FRIENDS_WITH")
class _ITFriendsWith(GraphEdge):
    """Integration-test typed edge with a property."""

    since: int = 0


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections.

    Binds this module's provider so query results hydrate into the typed
    classes declared above.
    """
    db = request.getfixturevalue(request.param)
    db.model_provider = mp
    return db


@pytest.fixture
def clean_db(test_db):
    """Provide a clean database connection with teardown."""
    yield test_db
    test_db.execute("MATCH (n) DETACH DELETE n")
    test_db.commit()


# ── Capability detection ──────────────────────────────────────────────────────


class TestCapabilities:
    """Test BackendCapability detection for index-related features."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_property_index_capability(self, test_db):
        """Both backends should support PROPERTY_INDEX."""
        assert test_db.has_capability(BackendCapability.PROPERTY_INDEX) is True

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_unique_constraint_not_supported(self, test_db):
        """Neither backend currently declares UNIQUE_CONSTRAINT support."""
        assert test_db.has_capability(BackendCapability.UNIQUE_CONSTRAINT) is False


# ── Index management ──────────────────────────────────────────────────────────


class TestIndexManagement:
    """Test create_property_index, drop_index, list_indexes."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_create_property_index(self, clean_db):
        """Creating an index should succeed without error."""
        # Create nodes so label table exists (needed for AGE)
        clean_db.execute("CREATE (n:TestLabel {id: '1', name: 'foo'})")
        clean_db.commit()

        # Should not raise
        clean_db.create_property_index("TestLabel", "id", "name")

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_create_index_idempotent(self, clean_db):
        """Creating the same index twice should not raise."""
        clean_db.execute("CREATE (n:IdxLabel {id: '1'})")
        clean_db.commit()

        clean_db.create_property_index("IdxLabel", "id")
        clean_db.create_property_index("IdxLabel", "id")  # second call -- no error

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_list_indexes_empty(self, clean_db):
        """list_indexes on a fresh graph should return only internal/no indexes."""
        indexes = clean_db.list_indexes()
        assert isinstance(indexes, list)

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_list_indexes_after_create(self, clean_db):
        """list_indexes should return created indexes."""
        clean_db.execute("CREATE (n:ListLabel {id: '1', name: 'foo'})")
        clean_db.commit()

        clean_db.create_property_index("ListLabel", "id")
        indexes = clean_db.list_indexes()

        labels = [idx.label for idx in indexes]
        assert "ListLabel" in labels

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_drop_index(self, clean_db):
        """Dropping an index should remove it from list_indexes."""
        clean_db.execute("CREATE (n:DropLabel {id: '1'})")
        clean_db.commit()

        clean_db.create_property_index("DropLabel", "id")
        before = [idx.label for idx in clean_db.list_indexes()]
        assert "DropLabel" in before

        clean_db.drop_index("DropLabel", "id")
        drop_label_indexes = [idx for idx in clean_db.list_indexes() if idx.label == "DropLabel"]
        assert not drop_label_indexes


# ── Bulk create nodes ─────────────────────────────────────────────────────────


class TestBulkCreateNodes:
    """Test bulk_create_nodes for both backends."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_basic(self, clean_db):
        """bulk_create_nodes should create all nodes."""
        rows = [{"id": f"node_{i}", "name": f"Node {i}", "value": i} for i in range(10)]
        count = clean_db.bulk_create_nodes(rows, label="BulkNode")
        clean_db.commit()

        assert count == 10

        result = clean_db.execute("MATCH (n:BulkNode) RETURN count(n)", unnest_result=True)
        assert result == 10

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_empty(self, clean_db):
        """bulk_create_nodes with empty list should return 0."""
        count = clean_db.bulk_create_nodes([], label="BulkNode")
        assert count == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_batching(self, clean_db):
        """bulk_create_nodes should correctly batch large inputs."""
        rows = [{"id": f"n{i}", "name": f"Name {i}"} for i in range(250)]
        count = clean_db.bulk_create_nodes(rows, label="BatchNode", batch_size=100)
        clean_db.commit()

        assert count == 250
        result = clean_db.execute("MATCH (n:BatchNode) RETURN count(n)", unnest_result=True)
        assert result == 250

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_special_chars(self, clean_db):
        """bulk_create_nodes should handle special characters in string values."""
        rows = [
            {"id": "sc_1", "name": 'value with "quotes"'},
            {"id": "sc_2", "name": "value with 'apostrophe'"},
            {"id": "sc_3", "name": "value with\nnewline"},
            {"id": "sc_4", "name": "value with\\backslash"},
        ]
        count = clean_db.bulk_create_nodes(rows, label="SpecialNode")
        clean_db.commit()

        assert count == 4
        result = clean_db.execute("MATCH (n:SpecialNode) RETURN count(n)", unnest_result=True)
        assert result == 4


# ── Bulk create edges ─────────────────────────────────────────────────────────


class TestBulkCreateEdges:
    """Test bulk_create_edges for both backends."""

    @pytest.fixture(autouse=True)
    def setup_nodes(self, clean_db):
        """Create source and destination nodes for edge tests."""
        clean_db.bulk_create_nodes(
            [
                {"id": "p1", "name": "Alice"},
                {"id": "p2", "name": "Bob"},
                {"id": "p3", "name": "Charlie"},
            ],
            label="Person",
        )
        clean_db.bulk_create_nodes(
            [
                {"id": "c1", "name": "Acme"},
                {"id": "c2", "name": "Globex"},
            ],
            label="Company",
        )
        # Create indexes for fast MATCH lookups
        clean_db.create_property_index("Person", "id")
        clean_db.create_property_index("Company", "id")
        clean_db.commit()

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_basic(self, clean_db):
        """bulk_create_edges should create all edges."""
        edges = [
            {"src": "p1", "dst": "c1"},
            {"src": "p2", "dst": "c1"},
            {"src": "p3", "dst": "c2"},
        ]
        count = clean_db.bulk_create_edges(edges, label="WORKS_AT", src_label="Person", dst_label="Company")
        clean_db.commit()

        assert count == 3
        result = clean_db.execute("MATCH ()-[r:WORKS_AT]->() RETURN count(r)", unnest_result=True)
        assert result == 3

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_with_properties(self, clean_db):
        """bulk_create_edges should set extra keys as edge properties."""
        edges = [
            {"src": "p1", "dst": "p2", "since": 2020, "strength": 0.9},
            {"src": "p2", "dst": "p3", "since": 2021, "strength": 0.7},
        ]
        count = clean_db.bulk_create_edges(edges, label="KNOWS", src_label="Person", dst_label="Person")
        clean_db.commit()

        assert count == 2
        result = clean_db.execute(
            "MATCH ()-[r:KNOWS]->() RETURN r.since, r.strength ORDER BY r.since",
            unnest_result=False,
        )
        assert len(result) == 2

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_empty(self, clean_db):
        """bulk_create_edges with empty list should return 0."""
        count = clean_db.bulk_create_edges([], label="KNOWS", src_label="Person", dst_label="Person")
        assert count == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_custom_ref_prop(self, clean_db):
        """bulk_create_edges should support custom src_ref_prop/dst_ref_prop."""
        edges = [{"src": "Alice", "dst": "Acme"}]
        count = clean_db.bulk_create_edges(
            edges,
            label="WORKS_AT",
            src_label="Person",
            dst_label="Company",
            src_ref_prop="name",
            dst_ref_prop="name",
        )
        clean_db.commit()

        assert count == 1
        result = clean_db.execute("MATCH ()-[r:WORKS_AT]->() RETURN count(r)", unnest_result=True)
        assert result == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_no_match(self, clean_db):
        """bulk_create_edges with no matching src/dst should create 0 edges in the graph."""
        edges = [{"src": "nonexistent_1", "dst": "nonexistent_2"}]
        clean_db.bulk_create_edges(edges, label="GHOST", src_label="Person", dst_label="Person")
        clean_db.commit()

        # UNWIND ran but MATCH found 0 nodes -> 0 edges actually created in the graph
        result = clean_db.execute("MATCH ()-[r:GHOST]->() RETURN count(r)", unnest_result=True)
        assert result == 0


# ── Composite endpoint keys ───────────────────────────────────────────────────


class TestBulkCreateEdgesCompositeKey:
    """Edge endpoint matching on a composite (multi-property) key.

    A single ref prop is only correct when it is unique within the label. When
    it is not -- the same ``qualified_name`` exists in several ``source_key``s of
    a shared graph -- a single-prop match binds an edge to an arbitrary node that
    happens to share the value, cross-linking (and, on re-index, corrupting)
    sources. Matching on ``(qualified_name, source_key)`` must bind each edge to
    the intended node within its own source. Runs on both the AGE direct-SQL path
    and the Memgraph Cypher UNWIND path.
    """

    @pytest.fixture(autouse=True)
    def setup_shared_symbol_nodes(self, clean_db):
        """Two sources with an identical qualified_name layout (the leak scenario)."""
        clean_db.bulk_create_nodes(
            [
                {"qualified_name": "pkg.foo", "source_key": "srcA", "name": "foo"},
                {"qualified_name": "pkg.bar", "source_key": "srcA", "name": "bar"},
                {"qualified_name": "pkg.foo", "source_key": "srcB", "name": "foo"},
                {"qualified_name": "pkg.bar", "source_key": "srcB", "name": "bar"},
            ],
            label="CKFn",
        )
        clean_db.create_property_index("CKFn", "qualified_name")
        clean_db.create_property_index("CKFn", "source_key")
        clean_db.commit()

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_binds_within_source(self, clean_db):
        """Each source's edge stays within that source -- no cross-source leak."""
        edges = [
            {"src": ("pkg.foo", "srcA"), "dst": ("pkg.bar", "srcA")},
            {"src": ("pkg.foo", "srcB"), "dst": ("pkg.bar", "srcB")},
        ]
        count = clean_db.bulk_create_edges(
            edges,
            label="CALLS",
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        assert count == 2
        pairs = clean_db.execute("MATCH (a:CKFn)-[:CALLS]->(b:CKFn) RETURN a.source_key, b.source_key ORDER BY a.source_key")
        assert [tuple(r) for r in pairs] == [("srcA", "srcA"), ("srcB", "srcB")]
        leaked = clean_db.execute(
            "MATCH (a:CKFn)-[:CALLS]->(b:CKFn) WHERE a.source_key <> b.source_key RETURN count(*)",
            unnest_result=True,
        )
        assert leaked == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_crossing_sources_is_explicit(self, clean_db):
        """A deliberately cross-source edge is still expressible via the composite key."""
        edges = [{"src": ("pkg.foo", "srcA"), "dst": ("pkg.bar", "srcB")}]
        count = clean_db.bulk_create_edges(
            edges,
            label="CALLS",
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        assert count == 1
        pairs = clean_db.execute("MATCH (a:CKFn)-[:CALLS]->(b:CKFn) RETURN a.source_key, b.source_key")
        assert [tuple(r) for r in pairs] == [("srcA", "srcB")]

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_with_edge_properties(self, clean_db):
        """Edge properties are preserved alongside a composite endpoint key."""
        edges = [{"src": ("pkg.foo", "srcA"), "dst": ("pkg.bar", "srcA"), "confidence": "EXTRACTED", "line": 7}]
        clean_db.bulk_create_edges(
            edges,
            label="CALLS",
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        result = clean_db.execute("MATCH (:CKFn)-[r:CALLS]->(:CKFn) RETURN r.confidence, r.line")
        assert len(result) == 1
        assert tuple(result[0]) == ("EXTRACTED", 7)

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_single_prop_key_still_matches_scalar(self, clean_db):
        """The scalar (single-property) form is accepted and matches as before.

        ``qualified_name`` alone is deliberately ambiguous here (two sources share
        it), which is exactly why a composite key is needed. The backends resolve
        the ambiguity differently -- AGE's direct-SQL graphid map keeps one node per
        value (1 edge), while Memgraph's Cypher MATCH yields the Cartesian product
        (2 foo x 2 bar = 4 edges). Both are pre-existing single-prop behaviour; this
        test only asserts the scalar form still works and produces at least one edge.
        """
        edges = [{"src": "pkg.foo", "dst": "pkg.bar"}]
        clean_db.bulk_create_edges(
            edges,
            label="LINKS",
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop="qualified_name",
            dst_ref_prop="qualified_name",
        )
        clean_db.commit()

        total = clean_db.execute("MATCH ()-[r:LINKS]->() RETURN count(r)", unnest_result=True)
        assert total >= 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_three_property_composite_key(self, clean_db):
        """A key of three properties matches only the node agreeing on all three."""
        # Same qualified_name+source_key in two langs: only the 3-key disambiguates.
        clean_db.bulk_create_nodes(
            [
                {"qualified_name": "pkg.x", "source_key": "srcA", "lang": "py", "name": "x"},
                {"qualified_name": "pkg.y", "source_key": "srcA", "lang": "py", "name": "y"},
                {"qualified_name": "pkg.x", "source_key": "srcA", "lang": "ts", "name": "x"},
                {"qualified_name": "pkg.y", "source_key": "srcA", "lang": "ts", "name": "y"},
            ],
            label="CK3",
        )
        clean_db.commit()

        key = ("qualified_name", "source_key", "lang")
        count = clean_db.bulk_create_edges(
            [{"src": ("pkg.x", "srcA", "py"), "dst": ("pkg.y", "srcA", "py")}],
            label="CALLS",
            src_label="CK3",
            dst_label="CK3",
            src_ref_prop=key,
            dst_ref_prop=key,
        )
        clean_db.commit()

        assert count == 1
        pairs = clean_db.execute("MATCH (a:CK3)-[:CALLS]->(b:CK3) RETURN a.lang, b.lang")
        assert [tuple(r) for r in pairs] == [("py", "py")]

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_typed_edges_parallel_refs(self, clean_db):
        """The typed GraphEdge path carries composite values via src_refs/dst_refs."""
        edges = [_ITFriendsWith(since=2020), _ITFriendsWith(since=2021)]
        count = clean_db.bulk_create_edges(
            edges,
            src_refs=[("pkg.foo", "srcA"), ("pkg.foo", "srcB")],
            dst_refs=[("pkg.bar", "srcA"), ("pkg.bar", "srcB")],
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        assert count == 2
        pairs = clean_db.execute(
            "MATCH (a:CKFn)-[:FRIENDS_WITH]->(b:CKFn) RETURN a.source_key, b.source_key ORDER BY a.source_key"
        )
        assert [tuple(r) for r in pairs] == [("srcA", "srcA"), ("srcB", "srcB")]

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_no_match_creates_nothing(self, clean_db):
        """A composite key resolving to no node creates 0 edges (no stray scalar match)."""
        # qualified_name exists, but source_key 'ghost' does not -- must not match.
        clean_db.bulk_create_edges(
            [{"src": ("pkg.foo", "ghost"), "dst": ("pkg.bar", "ghost")}],
            label="CALLS",
            src_label="CKFn",
            dst_label="CKFn",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        total = clean_db.execute("MATCH ()-[r:CALLS]->() RETURN count(r)", unnest_result=True)
        assert total == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_batching(self, clean_db):
        """Composite matching holds across multiple batches (graphid cache extend path)."""
        nodes = [{"qualified_name": f"n{i}", "source_key": "srcA", "name": f"n{i}"} for i in range(250)]
        clean_db.bulk_create_nodes(nodes, label="CKBatch")
        clean_db.commit()

        # Chain n0->n1->...->n249, 249 edges, forced through small batches.
        edges = [{"src": (f"n{i}", "srcA"), "dst": (f"n{i + 1}", "srcA")} for i in range(249)]
        count = clean_db.bulk_create_edges(
            edges,
            label="NEXT",
            src_label="CKBatch",
            dst_label="CKBatch",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
            batch_size=100,
        )
        clean_db.commit()

        assert count == 249
        total = clean_db.execute("MATCH (:CKBatch)-[r:NEXT]->(:CKBatch) RETURN count(r)", unnest_result=True)
        assert total == 249

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_composite_key_special_characters(self, clean_db):
        """Composite values with quotes/backslashes are escaped on both paths."""
        clean_db.bulk_create_nodes(
            [
                {"qualified_name": 'q"x', "source_key": "src\\a", "name": "a"},
                {"qualified_name": "q'y", "source_key": "src\\a", "name": "b"},
            ],
            label="CKSpec",
        )
        clean_db.commit()

        count = clean_db.bulk_create_edges(
            [{"src": ('q"x', "src\\a"), "dst": ("q'y", "src\\a")}],
            label="CALLS",
            src_label="CKSpec",
            dst_label="CKSpec",
            src_ref_prop=("qualified_name", "source_key"),
            dst_ref_prop=("qualified_name", "source_key"),
        )
        clean_db.commit()

        assert count == 1
        total = clean_db.execute("MATCH (:CKSpec)-[r:CALLS]->(:CKSpec) RETURN count(r)", unnest_result=True)
        assert total == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    @pytest.mark.parametrize("composite_side", ["src", "dst"])
    def test_mixed_arity_composite_and_scalar_endpoints(self, clean_db, composite_side):
        """One endpoint on a composite key, the other on a single property.

        Regression: Memgraph coerced both endpoints to lists as soon as either was
        composite, splitting the scalar endpoint's string into characters so the
        edge silently matched nothing. Each endpoint must be handled by its own arity.
        """
        clean_db.bulk_create_nodes([{"symbol": "ext:srcA:os.path", "name": "ext"}], label="CKExt")
        clean_db.create_property_index("CKExt", "symbol")
        clean_db.commit()

        composite_key = ("qualified_name", "source_key")
        if composite_side == "src":
            edge = {"src": ("pkg.foo", "srcA"), "dst": "ext:srcA:os.path"}
            kwargs = {"src_label": "CKFn", "dst_label": "CKExt", "src_ref_prop": composite_key, "dst_ref_prop": "symbol"}
            query = "MATCH (a:CKFn)-[r:CALLS]->(b:CKExt) RETURN a.source_key, b.symbol"
        else:
            edge = {"src": "ext:srcA:os.path", "dst": ("pkg.foo", "srcA")}
            kwargs = {"src_label": "CKExt", "dst_label": "CKFn", "src_ref_prop": "symbol", "dst_ref_prop": composite_key}
            query = "MATCH (a:CKExt)-[r:CALLS]->(b:CKFn) RETURN a.symbol, b.source_key"

        count = clean_db.bulk_create_edges([edge], label="CALLS", **kwargs)
        clean_db.commit()

        assert count == 1
        rows = clean_db.execute(query)
        assert len(rows) == 1, "mixed-arity edge was silently dropped"
        assert "srcA" in tuple(rows[0])

    @pytest.mark.parametrize("test_db", ["age_db"], indirect=True)
    def test_age_cypher_fallback_composite_and_mixed_arity(self, clean_db):
        """The AGE Cypher UNWIND fallback (direct_bulk_insert off) honours composite keys.

        The default AGE path is direct SQL; the fallback builds a different statement
        and was previously not exercised for composite or mixed-arity keys.
        """
        backend = clean_db._backend
        previous = backend.direct_bulk_insert
        backend.direct_bulk_insert = False
        try:
            clean_db.execute("MATCH (n) DETACH DELETE n")
            clean_db.bulk_create_nodes(
                [
                    {"qualified_name": "pkg.foo", "source_key": "srcA"},
                    {"qualified_name": "pkg.bar", "source_key": "srcA"},
                    {"qualified_name": "pkg.foo", "source_key": "srcB"},
                    {"qualified_name": "pkg.bar", "source_key": "srcB"},
                ],
                label="CKFb",
            )
            clean_db.bulk_create_nodes([{"symbol": "ext:srcA:os.path"}], label="CKFbExt")
            clean_db.commit()
            key = ("qualified_name", "source_key")

            clean_db.bulk_create_edges(
                [
                    {"src": ("pkg.foo", "srcA"), "dst": ("pkg.bar", "srcA")},
                    {"src": ("pkg.foo", "srcB"), "dst": ("pkg.bar", "srcB")},
                ],
                label="CALLS",
                src_label="CKFb",
                dst_label="CKFb",
                src_ref_prop=key,
                dst_ref_prop=key,
            )
            clean_db.bulk_create_edges(
                [{"src": ("pkg.foo", "srcA"), "dst": "ext:srcA:os.path"}],
                label="EXT",
                src_label="CKFb",
                dst_label="CKFbExt",
                src_ref_prop=key,
                dst_ref_prop="symbol",
            )
            clean_db.commit()

            pairs = clean_db.execute("MATCH (a:CKFb)-[:CALLS]->(b:CKFb) RETURN a.source_key, b.source_key ORDER BY a.source_key")
            assert [tuple(r) for r in pairs] == [("srcA", "srcA"), ("srcB", "srcB")]
            ext = clean_db.execute("MATCH (a:CKFb)-[:EXT]->(:CKFbExt) RETURN a.source_key", unnest_result=True)
            assert ext == "srcA"
        finally:
            backend.direct_bulk_insert = previous
            clean_db.execute("MATCH (n) DETACH DELETE n")
            clean_db.commit()


# ── Bulk + rollback interaction ─────────────────────────────────────────────


class TestBulkTransactional:
    """Tests covering bulk writes' interaction with transactions.

    Note: The default connection mode is ``autocommit=True``, so bulk writes
    are persisted immediately and cannot be rolled back. Tests here verify
    that rollback is still a safe no-op on an autocommit connection.
    """

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_rollback_does_not_raise(self, clean_db):
        """Rollback after bulk_create_nodes should not raise on an autocommit connection."""
        clean_db.bulk_create_nodes([{"id": f"t{i}"} for i in range(10)], label="TempNode")
        clean_db.rollback()  # should not raise


# ── Typed-input bulk writes (GraphNode / GraphEdge instances) ────────────────


class TestBulkCreateTyped:
    """Test bulk writes with typed GraphNode / GraphEdge instances for both backends."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_typed(self, clean_db):
        """bulk_create_nodes with GraphNode instances should derive label and persist properties."""
        people = [_ITPerson(id=f"t{i}", name=f"Typed {i}", age=20 + i) for i in range(5)]
        count = clean_db.bulk_create_nodes(people)
        clean_db.commit()

        assert count == 5
        fetched = clean_db.fetch_nodes({"label_": "_ITPerson"}, unnest_result=True)
        assert len(fetched) == 5
        assert all(isinstance(p, _ITPerson) for p in fetched)
        ages = sorted(p.age for p in fetched)
        assert ages == [20, 21, 22, 23, 24]

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_typed(self, clean_db):
        """bulk_create_edges with GraphEdge instances + parallel refs should persist edge properties."""
        # Seed nodes via typed path
        people = [_ITPerson(id=f"t{i}", name=f"T{i}") for i in range(3)]
        clean_db.bulk_create_nodes(people)
        clean_db.create_property_index("_ITPerson", "id")
        clean_db.commit()

        # Typed edges: connect t0->t1, t1->t2
        edges = [_ITFriendsWith(since=2020), _ITFriendsWith(since=2021)]
        count = clean_db.bulk_create_edges(
            edges,
            src_refs=["t0", "t1"],
            dst_refs=["t1", "t2"],
            src_label="_ITPerson",
            dst_label="_ITPerson",
            src_ref_prop="id",
            dst_ref_prop="id",
        )
        clean_db.commit()

        assert count == 2
        fetched = clean_db.fetch_edges({"label_": "FRIENDS_WITH"}, unnest_result=True)
        assert len(fetched) == 2
        assert all(isinstance(e, _ITFriendsWith) for e in fetched)
        assert sorted(e.since for e in fetched) == [2020, 2021]

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_typed_nodes_roundtrip_preserves_all_fields(self, clean_db):
        """Typed node write -> read should preserve all model fields including defaults."""
        p = _ITPerson(id="alice", name="Alice", age=30)
        clean_db.bulk_create_nodes([p])
        clean_db.commit()

        fetched = clean_db.fetch_nodes({"label_": "_ITPerson", "id": "alice"}, unnest_result=True, fetch_one=True)
        assert fetched is not None
        assert fetched.id == "alice"
        assert fetched.name == "Alice"
        assert fetched.age == 30


# ── Data correctness tests for bulk write ─────────────────────────────────────


class TestBulkDataCorrectness:
    """Verify that bulk-written data round-trips correctly through the graph.

    These tests go beyond count assertions: they read back specific property
    values, verify types (int, float, bool, str), and check edge properties.
    Runs on both backends to ensure the direct SQL path (AGE) and Cypher
    UNWIND path (Memgraph) produce identical results.
    """

    @pytest.fixture
    def test_db(self, request):
        return request.getfixturevalue(request.param)

    @pytest.fixture
    def clean_db(self, test_db):
        yield test_db
        test_db.execute("MATCH (n) DETACH DELETE n")
        test_db.commit()

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_node_string_properties_preserved(self, clean_db):
        """String values are preserved exactly, including empty strings."""
        rows = [
            {"id": "s1", "name": "Alice", "bio": ""},
            {"id": "s2", "name": "Bob", "bio": "A developer"},
        ]
        clean_db.bulk_create_nodes(rows, label="StrNode")
        clean_db.commit()

        result = clean_db.execute("MATCH (n:StrNode) RETURN n.id, n.name, n.bio ORDER BY n.id")
        assert len(result) == 2
        assert result[0] == ("s1", "Alice", "")
        assert result[1] == ("s2", "Bob", "A developer")

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_node_numeric_types_preserved(self, clean_db):
        """Integer and float property values are preserved with correct types."""
        rows = [{"id": "n1", "qty": 42, "score": 3.14, "zero": 0, "neg": -7}]
        clean_db.bulk_create_nodes(rows, label="NumNode")
        clean_db.commit()

        result = clean_db.execute("MATCH (n:NumNode {id: 'n1'}) RETURN n.qty, n.score, n.zero, n.neg", unnest_result=True)
        assert result[0] == 42
        assert abs(result[1] - 3.14) < 0.001
        assert result[2] == 0
        assert result[3] == -7

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_node_boolean_properties_preserved(self, clean_db):
        """Boolean values are stored and retrieved as booleans, not ints."""
        rows = [{"id": "b1", "active": True, "deleted": False}]
        clean_db.bulk_create_nodes(rows, label="BoolNode")
        clean_db.commit()

        result = clean_db.execute("MATCH (n:BoolNode {id: 'b1'}) RETURN n.active, n.deleted", unnest_result=True)
        assert result[0] is True
        assert result[1] is False

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_node_special_characters_in_strings(self, clean_db):
        """Special characters (quotes, backslash, newline, tab) survive round-trip."""
        rows = [
            {"id": "x1", "val": 'say "hello"'},
            {"id": "x2", "val": "path\\to\\file"},
            {"id": "x3", "val": "line1\nline2"},
            {"id": "x4", "val": "tab\there"},
            {"id": "x5", "val": "single 'quote' here"},
        ]
        clean_db.bulk_create_nodes(rows, label="SpecNode")
        clean_db.commit()

        result = clean_db.execute("MATCH (n:SpecNode) RETURN n.id, n.val ORDER BY n.id")
        assert len(result) == 5
        assert result[0][1] == 'say "hello"'
        assert result[1][1] == "path\\to\\file"
        assert result[2][1] == "line1\nline2"
        assert result[3][1] == "tab\there"
        assert result[4][1] == "single 'quote' here"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_edge_properties_preserved(self, clean_db):
        """Edge properties (confidence, line number) survive round-trip."""
        clean_db.bulk_create_nodes([{"id": "m1"}, {"id": "m2"}], label="Meth")
        clean_db.create_property_index("Meth", "id")
        clean_db.commit()

        edges = [{"src": "m1", "dst": "m2", "confidence": "EXTRACTED", "line": 42, "weight": 0.95}]
        clean_db.bulk_create_edges(edges, label="CALLS", src_label="Meth", dst_label="Meth")
        clean_db.commit()

        result = clean_db.execute(
            "MATCH ()-[r:CALLS]->() RETURN r.confidence, r.line, r.weight",
            unnest_result=True,
        )
        assert result[0] == "EXTRACTED"
        assert result[1] == 42
        assert abs(result[2] - 0.95) < 0.001

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_edge_without_properties(self, clean_db):
        """Edges with no properties (structural edges) work correctly."""
        clean_db.bulk_create_nodes([{"id": "a"}, {"id": "b"}], label="Node")
        clean_db.create_property_index("Node", "id")
        clean_db.commit()

        edges = [{"src": "a", "dst": "b"}]
        count = clean_db.bulk_create_edges(edges, label="LINKS", src_label="Node", dst_label="Node")
        clean_db.commit()

        assert count == 1
        result = clean_db.execute("MATCH (a:Node {id: 'a'})-[:LINKS]->(b:Node {id: 'b'}) RETURN b.id", unnest_result=True)
        assert result == "b"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_many_nodes_all_queryable(self, clean_db):
        """Bulk insert of 500 nodes should all be individually queryable."""
        rows = [{"id": f"bulk_{i}", "value": i * 10} for i in range(500)]
        clean_db.bulk_create_nodes(rows, label="BulkQ", batch_size=100)
        clean_db.commit()

        # Spot-check individual nodes
        for i in [0, 99, 250, 499]:
            result = clean_db.execute(f"MATCH (n:BulkQ {{id: 'bulk_{i}'}}) RETURN n.value", unnest_result=True)
            assert result == i * 10, f"Node bulk_{i} should have value {i * 10}"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_edge_direction_correct(self, clean_db):
        """Edges go from src to dst, not the other way around."""
        clean_db.bulk_create_nodes([{"id": "src_node"}, {"id": "dst_node"}], label="DirNode")
        clean_db.create_property_index("DirNode", "id")
        clean_db.commit()

        edges = [{"src": "src_node", "dst": "dst_node"}]
        clean_db.bulk_create_edges(edges, label="POINTS_TO", src_label="DirNode", dst_label="DirNode")
        clean_db.commit()

        # Forward direction should match
        fwd = clean_db.execute("MATCH (a:DirNode {id: 'src_node'})-[:POINTS_TO]->(b) RETURN b.id", unnest_result=True)
        assert fwd == "dst_node"

        # Reverse direction should not match
        rev = clean_db.execute("MATCH (a:DirNode {id: 'dst_node'})-[:POINTS_TO]->(b) RETURN b.id")
        assert rev == [] or rev is None


# ── Bulk delete nodes ─────────────────────────────────────────────────────


class TestBulkDeleteNodes:
    """Test bulk_delete_nodes for both backends."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_by_single_filter(self, clean_db):
        """bulk_delete_nodes should remove only matching nodes."""
        clean_db.bulk_create_nodes(
            [{"id": f"a{i}", "source_key": "alpha", "lang": "py"} for i in range(5)],
            label="DelNode",
        )
        clean_db.bulk_create_nodes(
            [{"id": f"b{i}", "source_key": "beta", "lang": "py"} for i in range(3)],
            label="DelNode",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_nodes("DelNode", {"source_key": "alpha"})
        clean_db.commit()

        assert deleted == 5
        remaining = clean_db.execute("MATCH (n:DelNode) RETURN count(n)", unnest_result=True)
        assert remaining == 3

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_by_multiple_filters(self, clean_db):
        """bulk_delete_nodes with multiple filters should AND them."""
        clean_db.bulk_create_nodes(
            [{"id": f"a{i}", "source_key": "src", "lang": "py"} for i in range(4)],
            label="MFNode",
        )
        clean_db.bulk_create_nodes(
            [{"id": f"b{i}", "source_key": "src", "lang": "ts"} for i in range(3)],
            label="MFNode",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_nodes("MFNode", {"source_key": "src", "lang": "py"})
        clean_db.commit()

        assert deleted == 4
        remaining = clean_db.execute("MATCH (n:MFNode) RETURN count(n)", unnest_result=True)
        assert remaining == 3

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_cascades_to_edges(self, clean_db):
        """bulk_delete_nodes should also remove edges referencing deleted nodes."""
        clean_db.bulk_create_nodes(
            [{"id": "x1", "source_key": "s"}, {"id": "x2", "source_key": "s"}],
            label="CascNode",
        )
        clean_db.bulk_create_nodes(
            [{"id": "y1", "source_key": "other"}],
            label="CascNode",
        )
        clean_db.create_property_index("CascNode", "id")
        clean_db.commit()

        clean_db.bulk_create_edges(
            [{"src": "x1", "dst": "x2"}, {"src": "x1", "dst": "y1"}],
            label="LINKS",
            src_label="CascNode",
            dst_label="CascNode",
        )
        clean_db.commit()

        # Verify edges exist
        edge_count = clean_db.execute("MATCH ()-[r:LINKS]->() RETURN count(r)", unnest_result=True)
        assert edge_count == 2

        # Delete only source_key='s' nodes
        deleted = clean_db.bulk_delete_nodes("CascNode", {"source_key": "s"})
        clean_db.commit()

        assert deleted == 2
        # Both edges should be gone (x1 was start of both)
        edge_count = clean_db.execute("MATCH ()-[r:LINKS]->() RETURN count(r)", unnest_result=True)
        assert edge_count == 0
        # y1 should still exist
        remaining = clean_db.execute("MATCH (n:CascNode) RETURN count(n)", unnest_result=True)
        assert remaining == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_no_match_returns_zero(self, clean_db):
        """bulk_delete_nodes with no matching nodes should return 0."""
        clean_db.bulk_create_nodes([{"id": "z1", "source_key": "keep"}], label="NoMatch")
        clean_db.commit()

        deleted = clean_db.bulk_delete_nodes("NoMatch", {"source_key": "nonexistent"})
        assert deleted == 0

        remaining = clean_db.execute("MATCH (n:NoMatch) RETURN count(n)", unnest_result=True)
        assert remaining == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_empty_filters_raises(self, clean_db):
        """bulk_delete_nodes with empty filters should raise ValueError."""
        with pytest.raises(ValueError, match="filters must not be empty"):
            clean_db.bulk_delete_nodes("SomeLabel", {})


# ── Bulk delete orphans ─────────────────────────────────────────────────────


class TestBulkDeleteOrphans:
    """Test bulk_delete_orphans for both backends.

    An orphan is a node with no edge of the given type in the given
    direction. AGE uses a direct SQL anti-join; Memgraph uses the Cypher
    OPTIONAL MATCH fallback. Both must produce identical results.
    """

    def test_age_declares_capability(self, age_db):
        """AGE declares BULK_DELETE_ORPHANS (optimized direct-SQL path)."""
        assert age_db.has_capability(BackendCapability.BULK_DELETE_ORPHANS) is True

    def _seed_referenced_and_orphans(self, clean_db):
        """Create Dependency nodes: some referenced by a Component, some orphaned."""
        clean_db.bulk_create_nodes(
            [{"id": "c1", "name": "comp1"}],
            label="OrphComponent",
        )
        clean_db.bulk_create_nodes(
            [{"id": f"dep{i}", "name": f"Dep {i}"} for i in range(5)],
            label="OrphDependency",
        )
        clean_db.create_property_index("OrphComponent", "id")
        clean_db.create_property_index("OrphDependency", "id")
        clean_db.commit()

        # Reference only dep0 and dep1 via HAS_DEP; dep2..dep4 are orphans.
        clean_db.bulk_create_edges(
            [{"src": "c1", "dst": "dep0"}, {"src": "c1", "dst": "dep1"}],
            label="HAS_DEP",
            src_label="OrphComponent",
            dst_label="OrphDependency",
        )
        clean_db.commit()

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_deletes_only_orphans(self, clean_db):
        """Only nodes with no incoming HAS_DEP edge should be deleted."""
        self._seed_referenced_and_orphans(clean_db)

        deleted = clean_db.bulk_delete_orphans("OrphDependency", "HAS_DEP", incoming=True)
        clean_db.commit()

        assert deleted == 3  # dep2, dep3, dep4
        remaining = clean_db.execute("MATCH (n:OrphDependency) RETURN count(n)", unnest_result=True)
        assert remaining == 2  # dep0, dep1 still referenced

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_preserves_referenced_nodes(self, clean_db):
        """Referenced nodes must survive and keep their edges."""
        self._seed_referenced_and_orphans(clean_db)

        clean_db.bulk_delete_orphans("OrphDependency", "HAS_DEP", incoming=True)
        clean_db.commit()

        edge_count = clean_db.execute("MATCH ()-[r:HAS_DEP]->() RETURN count(r)", unnest_result=True)
        assert edge_count == 2

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_all_orphans_deleted_when_no_edges(self, clean_db):
        """When no edges exist, every node of the label is an orphan."""
        clean_db.bulk_create_nodes(
            [{"id": f"x{i}"} for i in range(4)],
            label="AllOrphan",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_orphans("AllOrphan", "SOME_REL", incoming=True)
        clean_db.commit()

        assert deleted == 4
        remaining = clean_db.execute("MATCH (n:AllOrphan) RETURN count(n)", unnest_result=True)
        assert remaining == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_no_orphans_returns_zero(self, clean_db):
        """When every node is referenced, nothing is deleted."""
        clean_db.bulk_create_nodes([{"id": "c1"}], label="RefComp")
        clean_db.bulk_create_nodes([{"id": "d1"}, {"id": "d2"}], label="RefDep")
        clean_db.create_property_index("RefComp", "id")
        clean_db.create_property_index("RefDep", "id")
        clean_db.commit()

        clean_db.bulk_create_edges(
            [{"src": "c1", "dst": "d1"}, {"src": "c1", "dst": "d2"}],
            label="REFS",
            src_label="RefComp",
            dst_label="RefDep",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_orphans("RefDep", "REFS", incoming=True)
        clean_db.commit()

        assert deleted == 0
        remaining = clean_db.execute("MATCH (n:RefDep) RETURN count(n)", unnest_result=True)
        assert remaining == 2

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_outgoing_direction(self, clean_db):
        """incoming=False should check outgoing edges instead."""
        clean_db.bulk_create_nodes([{"id": "s1"}, {"id": "s2"}], label="OutSrc")
        clean_db.bulk_create_nodes([{"id": "t1"}], label="OutDst")
        clean_db.create_property_index("OutSrc", "id")
        clean_db.create_property_index("OutDst", "id")
        clean_db.commit()

        # s1 has an outgoing OUT_REL; s2 does not (orphan).
        clean_db.bulk_create_edges(
            [{"src": "s1", "dst": "t1"}],
            label="OUT_REL",
            src_label="OutSrc",
            dst_label="OutDst",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_orphans("OutSrc", "OUT_REL", incoming=False)
        clean_db.commit()

        assert deleted == 1  # s2
        remaining = clean_db.execute("MATCH (n:OutSrc) RETURN count(n)", unnest_result=True)
        assert remaining == 1  # s1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_missing_label_returns_zero(self, clean_db):
        """Orphan delete on a never-created label returns 0 without error."""
        deleted = clean_db.bulk_delete_orphans("NeverCreatedLabel", "NEVER_REL", incoming=True)
        assert deleted == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_filters_scope_orphan_deletion(self, clean_db):
        """filters should restrict orphan deletion to matching nodes only."""
        # Two sources of orphaned decorators; only 'src_a' should be cleaned.
        clean_db.bulk_create_nodes(
            [{"id": f"a{i}", "source_key": "src_a"} for i in range(3)],
            label="ScopedDec",
        )
        clean_db.bulk_create_nodes(
            [{"id": f"b{i}", "source_key": "src_b"} for i in range(2)],
            label="ScopedDec",
        )
        clean_db.commit()

        # All are orphans (no DECORATES edge), but scope to src_a only.
        deleted = clean_db.bulk_delete_orphans("ScopedDec", "DECORATES", incoming=False, filters={"source_key": "src_a"})
        clean_db.commit()

        assert deleted == 3
        remaining = clean_db.execute("MATCH (n:ScopedDec) RETURN count(n)", unnest_result=True)
        assert remaining == 2  # src_b untouched

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_filters_with_referenced_nodes(self, clean_db):
        """filters + edge presence: only filtered orphans deleted, referenced kept."""
        clean_db.bulk_create_nodes([{"id": "tgt1"}], label="DecTarget")
        clean_db.bulk_create_nodes(
            [
                {"id": "d1", "source_key": "s1"},  # will have edge -> kept
                {"id": "d2", "source_key": "s1"},  # orphan, in scope -> deleted
                {"id": "d3", "source_key": "s2"},  # orphan, out of scope -> kept
            ],
            label="ScopedDec2",
        )
        clean_db.create_property_index("ScopedDec2", "id")
        clean_db.create_property_index("DecTarget", "id")
        clean_db.commit()

        clean_db.bulk_create_edges(
            [{"src": "d1", "dst": "tgt1"}],
            label="DECORATES",
            src_label="ScopedDec2",
            dst_label="DecTarget",
        )
        clean_db.commit()

        deleted = clean_db.bulk_delete_orphans("ScopedDec2", "DECORATES", incoming=False, filters={"source_key": "s1"})
        clean_db.commit()

        assert deleted == 1  # only d2
        remaining = clean_db.execute("MATCH (n:ScopedDec2) RETURN n.id ORDER BY n.id")
        ids = sorted(r[0] for r in remaining)
        assert ids == ["d1", "d3"]
