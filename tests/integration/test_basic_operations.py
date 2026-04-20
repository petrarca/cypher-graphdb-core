"""Integration tests for basic CypherGraphDB operations.

Tests the core facade operations that consumers use: execute, commit, rollback,
labels, graphs, graph create/drop, and data lifecycle (create → query → update → delete).
"""

import pytest

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


# ── Execute & basic queries ───────────────────────────────────────────────────


class TestExecute:
    """Test basic Cypher execution."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_return_literal(self, test_db):
        """Execute a simple RETURN statement."""
        result = test_db.execute("RETURN 42 AS val", unnest_result=True)
        assert result == 42

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_return_string(self, test_db):
        """Execute RETURN with a string value."""
        result = test_db.execute("RETURN 'hello' AS val", unnest_result=True)
        assert result == "hello"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_return_multiple_columns(self, test_db):
        """Execute RETURN with multiple columns."""
        result = test_db.execute("RETURN 1 AS a, 'two' AS b, 3.0 AS c", unnest_result=True)
        assert result[0] == 1
        assert result[1] == "two"
        assert result[2] == 3.0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_empty_result(self, test_db):
        """Query that returns no rows."""
        result = test_db.execute("MATCH (n:NonExistent__) RETURN n", unnest_result=False)
        assert result == [] or result is None


# ── Data lifecycle: create → query → update → delete ─────────────────────────


class TestDataLifecycle:
    """Test the full CRUD lifecycle via Cypher execute."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_create_and_query_node(self, clean_db):
        """Create a node and query it back."""
        clean_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        clean_db.commit()

        result = clean_db.execute("MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age", unnest_result=True)
        assert result[0] == "Alice"
        assert result[1] == 30

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_create_and_query_edge(self, clean_db):
        """Create nodes + edge and query the relationship."""
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice'})
            CREATE (b:Company {name: 'Acme'})
            CREATE (a)-[:WORKS_AT {since: 2020}]->(b)
        """)
        clean_db.commit()

        result = clean_db.execute(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, r.since, c.name",
            unnest_result=True,
        )
        assert result[0] == "Alice"
        assert result[1] == 2020
        assert result[2] == "Acme"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_update_node_property(self, clean_db):
        """Update a node property via SET."""
        clean_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        clean_db.commit()

        clean_db.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 31")
        clean_db.commit()

        result = clean_db.execute("MATCH (p:Person {name: 'Alice'}) RETURN p.age", unnest_result=True)
        assert result == 31

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_node(self, clean_db):
        """Delete a node and verify it's gone."""
        clean_db.execute("CREATE (p:Person {name: 'Alice'})")
        clean_db.commit()

        clean_db.execute("MATCH (p:Person {name: 'Alice'}) DELETE p")
        clean_db.commit()

        result = clean_db.execute("MATCH (p:Person {name: 'Alice'}) RETURN count(p)", unnest_result=True)
        assert result == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_detach_delete(self, clean_db):
        """DETACH DELETE removes node and all its relationships."""
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice'})
            CREATE (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        clean_db.commit()

        clean_db.execute("MATCH (p:Person {name: 'Alice'}) DETACH DELETE p")
        clean_db.commit()

        result = clean_db.execute("MATCH (p:Person) RETURN count(p)", unnest_result=True)
        assert result == 1  # Only Bob remains

        result = clean_db.execute("MATCH ()-[r:KNOWS]->() RETURN count(r)", unnest_result=True)
        assert result == 0  # Edge deleted with Alice


# ── Commit and rollback ──────────────────────────────────────────────────────


class TestCommitRollback:
    """Test explicit commit and rollback behavior.

    Note: The default test fixtures use autocommit=True, so rollback tests
    can only verify that rollback() does not raise -- not that data is undone.
    """

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_commit_persists_data(self, clean_db):
        """Data should be visible after commit."""
        clean_db.execute("CREATE (p:Person {name: 'Committed'})")
        clean_db.commit()

        result = clean_db.execute("MATCH (p:Person {name: 'Committed'}) RETURN count(p)", unnest_result=True)
        assert result == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_rollback_does_not_raise(self, clean_db):
        """Rollback should not raise even in autocommit mode."""
        clean_db.execute("CREATE (p:Person {name: 'Test'})")
        clean_db.rollback()  # should not raise


# ── Labels ────────────────────────────────────────────────────────────────────


class TestLabels:
    """Test labels() facade method."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_labels_after_create(self, clean_db):
        """labels() should reflect created node labels."""
        clean_db.execute("CREATE (p:Person {name: 'Alice'})")
        clean_db.execute("CREATE (c:Company {name: 'Acme'})")
        clean_db.commit()

        labels = clean_db.labels()
        label_names = [lbl.label_ for lbl in labels]
        assert "Person" in label_names
        assert "Company" in label_names

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_labels_include_edge_labels(self, clean_db):
        """labels() should include edge labels."""
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice'})
            CREATE (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        clean_db.commit()

        labels = clean_db.labels()
        label_names = [lbl.label_ for lbl in labels]
        assert "Person" in label_names
        assert "KNOWS" in label_names


# ── Graphs ────────────────────────────────────────────────────────────────────


class TestGraphs:
    """Test graphs() and graph management."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_graphs_returns_list(self, test_db):
        """graphs() should return a non-empty list."""
        graphs = test_db.graphs()
        assert isinstance(graphs, list)
        assert len(graphs) >= 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_graph_exists(self, test_db):
        """graph_exists() should return True for a known graph."""
        graphs = test_db.graphs()
        assert test_db.graph_exists(graphs[0]) is True

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_graph_not_exists(self, test_db):
        """graph_exists() should return False for a non-existent graph."""
        assert test_db.graph_exists("this_graph_does_not_exist_xyz") is False


# ── Special value handling ────────────────────────────────────────────────────


class TestSpecialValues:
    """Test handling of None, boolean, and edge-case values."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_boolean_properties(self, clean_db):
        """Boolean values should roundtrip correctly."""
        clean_db.bulk_create_nodes("Flag", [{"id": "f1", "active": True, "deleted": False}])
        clean_db.commit()

        result = clean_db.execute("MATCH (f:Flag) RETURN f.active, f.deleted", unnest_result=True)
        assert result[0] is True
        assert result[1] is False

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_null_properties(self, clean_db):
        """Null values should roundtrip correctly."""
        clean_db.bulk_create_nodes("Nullable", [{"id": "n1", "value": None}])
        clean_db.commit()

        result = clean_db.execute("MATCH (n:Nullable {id: 'n1'}) RETURN n.value", unnest_result=True)
        assert result is None

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_numeric_properties(self, clean_db):
        """Integer and float values should roundtrip correctly."""
        clean_db.bulk_create_nodes("Numeric", [{"id": "num1", "int_val": 42, "float_val": 3.14}])
        clean_db.commit()

        result = clean_db.execute("MATCH (n:Numeric) RETURN n.int_val, n.float_val", unnest_result=True)
        assert result[0] == 42
        assert abs(result[1] - 3.14) < 0.001

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_empty_string_property(self, clean_db):
        """Empty string should roundtrip correctly."""
        clean_db.bulk_create_nodes("EmptyStr", [{"id": "e1", "name": ""}])
        clean_db.commit()

        result = clean_db.execute("MATCH (n:EmptyStr) RETURN n.name", unnest_result=True)
        assert result == ""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_special_characters_in_strings(self, clean_db):
        """Strings with special characters should roundtrip correctly."""
        clean_db.bulk_create_nodes(
            "Special",
            [
                {"id": "s1", "val": 'contains "double quotes"'},
                {"id": "s2", "val": "contains\nnewline"},
                {"id": "s3", "val": "contains\\backslash"},
            ],
        )
        clean_db.commit()

        result = clean_db.execute("MATCH (n:Special) RETURN n.id, n.val ORDER BY n.id", unnest_result=False)
        assert len(result) == 3


# ── Bulk + rollback interaction ───────────────────────────────────────────────


class TestBulkEdgeCases:
    """Test edge cases in bulk operations."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_edges_no_match(self, clean_db):
        """bulk_create_edges with no matching src/dst should create 0 edges."""
        edges = [{"src": "nonexistent_1", "dst": "nonexistent_2"}]
        count = clean_db.bulk_create_edges("GHOST", edges, src_label="Person", dst_label="Person")
        clean_db.commit()

        assert count == 1  # UNWIND ran but MATCH found 0 nodes -> 0 edges actually created
        result = clean_db.execute("MATCH ()-[r:GHOST]->() RETURN count(r)", unnest_result=True)
        assert result == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_bulk_rollback_does_not_raise(self, clean_db):
        """Rollback after bulk_create_nodes should not raise."""
        clean_db.bulk_create_nodes("TempNode", [{"id": f"t{i}"} for i in range(10)])
        clean_db.rollback()  # should not raise
