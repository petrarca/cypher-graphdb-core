"""Integration tests for read-only mode functionality."""

import pytest

from cypher_graphdb import CypherGraphDB
from cypher_graphdb.exceptions import ReadOnlyModeError

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db_readonly(request):
    """Parametrized fixture for read-only database connections."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def test_db(request):
    """Parametrized fixture for regular database connections."""
    return request.getfixturevalue(request.param)


class TestReadOnlyMode:
    """Test suite for read-only mode functionality."""

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_read_only_property(self, test_db_readonly):
        """Test that read_only property is accessible and correct."""
        assert test_db_readonly.read_only is True

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_regular_connection_not_readonly(self, test_db):
        """Test that regular connections are not read-only by default."""
        assert test_db.read_only is False

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_read_operations_work(self, test_db_readonly):
        """Test that read operations work in read-only mode."""
        # Simple MATCH query (no data needed, just checking it doesn't error)
        result = test_db_readonly.execute("MATCH (n) RETURN n")
        # Should return empty list or execute without error
        assert isinstance(result, list)

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_create_blocked(self, test_db_readonly):
        """Test that CREATE is blocked in read-only mode."""
        with pytest.raises(ReadOnlyModeError) as exc_info:
            test_db_readonly.execute("CREATE (p:Person {name: 'Test'})")
        assert "Write operation not allowed" in str(exc_info.value)

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_delete_blocked(self, test_db_readonly):
        """Test that DELETE is blocked in read-only mode."""
        with pytest.raises(ReadOnlyModeError):
            test_db_readonly.execute("MATCH (p) DELETE p")

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_set_blocked(self, test_db_readonly):
        """Test that SET is blocked in read-only mode."""
        with pytest.raises(ReadOnlyModeError):
            test_db_readonly.execute("MATCH (p) SET p.x = 1")

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_merge_blocked(self, test_db_readonly):
        """Test that MERGE is blocked in read-only mode."""
        with pytest.raises(ReadOnlyModeError):
            test_db_readonly.execute("MERGE (p:Person {name: 'Test'})")

    @pytest.mark.parametrize("test_db_readonly", ["memgraph_db_readonly", "age_db_readonly"], indirect=True)
    def test_remove_blocked(self, test_db_readonly):
        """Test that REMOVE is blocked in read-only mode."""
        with pytest.raises(ReadOnlyModeError):
            test_db_readonly.execute("MATCH (p) REMOVE p.x")

    @pytest.mark.parametrize(
        "container_name,backend",
        [
            ("memgraph_container", "memgraph"),
            ("age_container", "age"),
        ],
    )
    def test_readonly_via_connect_param(self, request, container_name, backend):
        """Test that read_only can be set via connect parameter."""
        container_info = request.getfixturevalue(container_name)

        if backend == "memgraph":
            connect_params = {
                "host": container_info["host"],
                "port": container_info["port"],
                "read_only": True,
            }
        else:  # age
            connect_params = {
                "host": container_info["host"],
                "port": container_info["port"],
                "dbname": container_info["dbname"],
                "user": container_info["user"],
                "password": container_info["password"],
                "graph_name": "test_graph",
                "read_only": True,
            }

        with CypherGraphDB(backend=backend, connect_params=connect_params) as db:
            assert db.read_only is True
            with pytest.raises(ReadOnlyModeError):
                db.execute("CREATE (p:Person {name: 'Test'})")

    @pytest.mark.parametrize(
        "container_name,backend",
        [
            ("memgraph_container", "memgraph"),
            ("age_container", "age"),
        ],
    )
    def test_toggle_readonly_mode(self, request, container_name, backend):
        """Test that read-only mode can be enabled/disabled dynamically."""
        container_info = request.getfixturevalue(container_name)

        if backend == "memgraph":
            connect_params = {
                "host": container_info["host"],
                "port": container_info["port"],
                "read_only": False,
            }
        else:  # age
            connect_params = {
                "host": container_info["host"],
                "port": container_info["port"],
                "dbname": container_info["dbname"],
                "user": container_info["user"],
                "password": container_info["password"],
                "graph_name": "test_graph",
                "read_only": False,
            }

        db = CypherGraphDB(backend=backend, connect_params=connect_params)
        db.connect()

        try:
            # Ensure we start with read-only disabled
            assert db.read_only is False

            # Enable read-only mode
            db.read_only = True
            assert db.read_only is True

            # Write operations should now be blocked
            with pytest.raises(ReadOnlyModeError):
                db.execute("CREATE (p:Person {name: 'Test'})")

            # Disable read-only mode
            db.read_only = False
            assert db.read_only is False

            # Write operations should now work
            db.execute("CREATE (p:Person {name: 'Test'})")

            # Verify the node was created
            result = db.execute(
                "MATCH (p:Person {name: 'Test'}) RETURN count(p)",
                unnest_result="rc",
            )
            assert result == 1

            # Cleanup
            db.execute("MATCH (p:Person {name: 'Test'}) DELETE p")
        finally:
            db.disconnect()
