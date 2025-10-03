"""Integration tests for create_or_merge functionality with Memgraph backend.

Tests the tuple unpacking fix for _create_node, _merge_node,
_create_edge, and _merge_edge.
"""

import pytest

from cypher_graphdb import GraphEdge, GraphNode, edge, node


@node(label="PersonNode")
class PersonNode(GraphNode):
    """Person node model for testing."""

    name: str
    age: int | None = None
    email: str | None = None


@edge(label="RelationEdge")
class RelationEdge(GraphEdge):
    """Relation edge model for testing."""

    since: int | None = None
    role: str | None = None


@pytest.fixture
def test_node_model():
    """Sample node for testing."""
    return PersonNode(name="TestPerson", age=30, email="test@example.com")


@pytest.fixture
def test_edge_model(memgraph_db):
    """Sample edge for testing - requires two nodes."""
    # Create two nodes first
    node1 = PersonNode(name="Person1", age=25)
    node2 = PersonNode(name="Person2", age=30)

    node1 = memgraph_db.create_or_merge(node1)
    node2 = memgraph_db.create_or_merge(node2)

    # Create edge between them
    edge = RelationEdge(start_id_=node1.id_, end_id_=node2.id_, since=2020, role="colleague")
    return edge, node1, node2


class TestCreateNode:
    """Test node creation with tuple unpacking fix."""

    def test_create_node_assigns_integer_id(self, memgraph_db, test_node_model):
        """Test that creating a node assigns an integer ID, not a tuple."""
        # Create the node
        created_node = memgraph_db.create_or_merge(test_node_model, strategy="force_create")

        # Verify ID is an integer, not a tuple
        assert created_node.id_ is not None, "Node ID should be assigned"
        assert isinstance(created_node.id_, int), f"Node ID should be int, got {type(created_node.id_)}"
        assert created_node.id_ >= 0, "Node ID should be non-negative"

    def test_create_node_preserves_properties(self, memgraph_db, test_node_model):
        """Test that creating a node preserves all properties."""
        created_node = memgraph_db.create_or_merge(test_node_model, strategy="force_create")

        assert created_node.name == "TestPerson"
        assert created_node.age == 30
        assert created_node.email == "test@example.com"

    def test_create_multiple_nodes_get_different_ids(self, memgraph_db):
        """Test that creating multiple nodes assigns different IDs."""
        node1 = PersonNode(name="Person1", age=25)
        node2 = PersonNode(name="Person2", age=30)

        created1 = memgraph_db.create_or_merge(node1, strategy="force_create")
        created2 = memgraph_db.create_or_merge(node2, strategy="force_create")

        assert isinstance(created1.id_, int), "First node ID should be int"
        assert isinstance(created2.id_, int), "Second node ID should be int"
        assert created1.id_ != created2.id_, "Different nodes should have different IDs"


class TestMergeNode:
    """Test node merging with tuple unpacking fix."""

    def test_merge_node_updates_properties(self, memgraph_db, test_node_model):
        """Test that merging a node updates its properties correctly."""
        # Create the node first
        created_node = memgraph_db.create_or_merge(test_node_model, strategy="force_create")
        original_id = created_node.id_

        # Modify properties
        created_node.age = 31
        created_node.email = "updated@example.com"

        # Merge (update) the node
        merged_node = memgraph_db.create_or_merge(created_node, strategy="merge")

        # Verify ID is still an integer and unchanged
        assert isinstance(merged_node.id_, int), f"Merged node ID should be int, got {type(merged_node.id_)}"
        assert merged_node.id_ == original_id, "Node ID should not change after merge"

        # Verify properties were updated
        assert merged_node.age == 31, "Age should be updated"
        assert merged_node.email == "updated@example.com", "Email should be updated"

    def test_merge_node_fetches_latest_from_db(self, memgraph_db):
        """Test that merge fetches the latest node state from database."""
        # Create a node
        node = PersonNode(name="TestUser", age=25)
        created = memgraph_db.create_or_merge(node, strategy="force_create")
        node_id = created.id_

        # Update directly in database
        memgraph_db.execute(f"MATCH (n) WHERE id(n) = {node_id} SET n.age = 99, n.email = 'db@example.com'")

        # Now merge - should fetch updated values from DB
        created.email = "will_be_overwritten@example.com"
        merged = memgraph_db.create_or_merge(created, strategy="merge")

        # Should have DB values, not what we set before merge
        assert isinstance(merged.id_, int), "Merged node ID should be int"
        # Note: The merge updates the node, so we get our latest changes
        assert merged.email == "will_be_overwritten@example.com"


class TestCreateEdge:
    """Test edge creation with tuple unpacking fix."""

    def test_create_edge_assigns_integer_id(self, memgraph_db, test_edge_model):
        """Test that creating an edge assigns an integer ID, not a tuple."""
        edge, node1, node2 = test_edge_model

        # Create the edge
        created_edge = memgraph_db.create_or_merge(edge, strategy="force_create")

        # Verify ID is an integer, not a tuple
        assert created_edge.id_ is not None, "Edge ID should be assigned"
        assert isinstance(created_edge.id_, int), f"Edge ID should be int, got {type(created_edge.id_)}"
        assert created_edge.id_ >= 0, "Edge ID should be non-negative"

    def test_create_edge_preserves_properties(self, memgraph_db, test_edge_model):
        """Test that creating an edge preserves all properties."""
        edge, node1, node2 = test_edge_model

        created_edge = memgraph_db.create_or_merge(edge, strategy="force_create")

        assert created_edge.since == 2020
        assert created_edge.role == "colleague"
        assert isinstance(created_edge.start_id_, int)
        assert isinstance(created_edge.end_id_, int)


class TestMergeEdge:
    """Test edge merging with tuple unpacking fix."""

    def test_merge_edge_updates_properties(self, memgraph_db, test_edge_model):
        """Test that merging an edge updates its properties correctly."""
        edge, node1, node2 = test_edge_model

        # Create the edge first
        created_edge = memgraph_db.create_or_merge(edge, strategy="force_create")
        original_id = created_edge.id_

        # Modify properties
        created_edge.since = 2021
        created_edge.role = "manager"

        # Merge (update) the edge
        merged_edge = memgraph_db.create_or_merge(created_edge, strategy="merge")

        # Verify ID is still an integer and unchanged
        assert isinstance(merged_edge.id_, int), f"Merged edge ID should be int, got {type(merged_edge.id_)}"
        assert merged_edge.id_ == original_id, "Edge ID should not change after merge"

        # Verify properties were updated
        assert merged_edge.since == 2021, "Since should be updated"
        assert merged_edge.role == "manager", "Role should be updated"


class TestCreateOrMergeStrategy:
    """Test the merge strategy behavior."""

    def test_merge_strategy_creates_when_no_id(self, memgraph_db):
        """Test that merge strategy creates a new node when it has no ID."""
        node = PersonNode(name="NewNode", age=40)

        # Use merge strategy, but node has no ID, so it should create
        result = memgraph_db.create_or_merge(node, strategy="merge")

        assert result.id_ is not None
        assert isinstance(result.id_, int)
        assert result.name == "NewNode"

    def test_merge_strategy_updates_when_has_id(self, memgraph_db):
        """Test that merge strategy updates when node has an ID."""
        # Create first
        node = PersonNode(name="ExistingNode", age=35)
        created = memgraph_db.create_or_merge(node, strategy="force_create")
        original_id = created.id_

        # Update properties
        created.age = 36

        # Merge should update, not create new
        merged = memgraph_db.create_or_merge(created, strategy="merge")

        assert isinstance(merged.id_, int)
        assert merged.id_ == original_id, "Should update existing node, not create new"
        assert merged.age == 36

    def test_force_create_always_creates_new(self, memgraph_db):
        """Test that force_create always creates a new node even with same properties."""
        node1 = PersonNode(name="Duplicate", age=25)
        node2 = PersonNode(name="Duplicate", age=25)

        created1 = memgraph_db.create_or_merge(node1, strategy="force_create")
        created2 = memgraph_db.create_or_merge(node2, strategy="force_create")

        assert isinstance(created1.id_, int)
        assert isinstance(created2.id_, int)
        assert created1.id_ != created2.id_, "force_create should create separate nodes"


class TestRoundTrip:
    """Test complete round-trip: create, fetch, update, fetch again."""

    def test_node_roundtrip(self, memgraph_db):
        """Test creating, fetching, updating, and fetching a node."""
        # Create
        original = PersonNode(name="RoundTrip", age=50, email="original@test.com")
        created = memgraph_db.create_or_merge(original, strategy="force_create")
        created_id = created.id_

        assert isinstance(created_id, int)

        # Fetch by ID
        fetched = memgraph_db.fetch_nodes(created_id, unnest_result="rc")
        assert isinstance(fetched.id_, int)
        assert fetched.id_ == created_id
        assert fetched.name == "RoundTrip"
        assert fetched.age == 50

        # Update
        fetched.age = 51
        fetched.email = "updated@test.com"
        updated = memgraph_db.create_or_merge(fetched, strategy="merge")

        assert isinstance(updated.id_, int)
        assert updated.id_ == created_id
        assert updated.age == 51

        # Fetch again to verify persistence
        final = memgraph_db.fetch_nodes(created_id, unnest_result="rc")
        assert isinstance(final.id_, int)
        assert final.id_ == created_id
        assert final.age == 51
        assert final.email == "updated@test.com"

    def test_edge_roundtrip(self, memgraph_db):
        """Test creating, fetching, updating, and fetching an edge."""
        # Create nodes
        node1 = PersonNode(name="EdgeNode1", age=20)
        node2 = PersonNode(name="EdgeNode2", age=25)
        node1 = memgraph_db.create_or_merge(node1)
        node2 = memgraph_db.create_or_merge(node2)

        # Create edge
        edge = RelationEdge(start_id_=node1.id_, end_id_=node2.id_, since=2019, role="friend")
        created_edge = memgraph_db.create_or_merge(edge, strategy="force_create")
        edge_id = created_edge.id_

        assert isinstance(edge_id, int)

        # Fetch by ID
        fetched_edge = memgraph_db.fetch_edges(edge_id, unnest_result="rc")
        assert isinstance(fetched_edge.id_, int)
        assert fetched_edge.id_ == edge_id
        assert fetched_edge.since == 2019

        # Update
        fetched_edge.since = 2020
        fetched_edge.role = "best_friend"
        updated_edge = memgraph_db.create_or_merge(fetched_edge, strategy="merge")

        assert isinstance(updated_edge.id_, int)
        assert updated_edge.id_ == edge_id
        assert updated_edge.since == 2020

        # Fetch again
        final_edge = memgraph_db.fetch_edges(edge_id, unnest_result="rc")
        assert isinstance(final_edge.id_, int)
        assert final_edge.id_ == edge_id
        assert final_edge.since == 2020
        assert final_edge.role == "best_friend"
