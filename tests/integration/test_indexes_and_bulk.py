"""Integration tests for index management and bulk write operations.

Tests create_property_index, drop_index, list_indexes, bulk_create_nodes,
and bulk_create_edges for both Memgraph and AGE backends.
"""

import pytest

from cypher_graphdb import BackendCapability

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


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
        count = clean_db.bulk_create_nodes("BulkNode", rows)
        clean_db.commit()

        assert count == 10

        result = clean_db.execute("MATCH (n:BulkNode) RETURN count(n)", unnest_result=True)
        assert result == 10

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_empty(self, clean_db):
        """bulk_create_nodes with empty list should return 0."""
        count = clean_db.bulk_create_nodes("BulkNode", [])
        assert count == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_nodes_batching(self, clean_db):
        """bulk_create_nodes should correctly batch large inputs."""
        rows = [{"id": f"n{i}", "name": f"Name {i}"} for i in range(250)]
        count = clean_db.bulk_create_nodes("BatchNode", rows, batch_size=100)
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
        count = clean_db.bulk_create_nodes("SpecialNode", rows)
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
            "Person",
            [
                {"id": "p1", "name": "Alice"},
                {"id": "p2", "name": "Bob"},
                {"id": "p3", "name": "Charlie"},
            ],
        )
        clean_db.bulk_create_nodes(
            "Company",
            [
                {"id": "c1", "name": "Acme"},
                {"id": "c2", "name": "Globex"},
            ],
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
        count = clean_db.bulk_create_edges("WORKS_AT", edges, src_label="Person", dst_label="Company")
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
        count = clean_db.bulk_create_edges("KNOWS", edges, src_label="Person", dst_label="Person")
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
        count = clean_db.bulk_create_edges("KNOWS", [])
        assert count == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_create_edges_custom_key(self, clean_db):
        """bulk_create_edges should support custom src_key/dst_key."""
        edges = [{"src": "Alice", "dst": "Acme"}]
        count = clean_db.bulk_create_edges(
            "WORKS_AT",
            edges,
            src_label="Person",
            dst_label="Company",
            src_key="name",
            dst_key="name",
        )
        clean_db.commit()

        assert count == 1
        result = clean_db.execute("MATCH ()-[r:WORKS_AT]->() RETURN count(r)", unnest_result=True)
        assert result == 1
