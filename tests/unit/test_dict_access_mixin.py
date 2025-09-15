"""Unit tests for DictAccessMixin functionality in GraphNode and GraphEdge."""

import pytest

from cypher_graphdb.models import GraphEdge, GraphNode


class TestGraphNodeDictAccess:
    """Test dictionary-like access for GraphNode."""

    def test_property_access(self):
        """Test accessing properties via dictionary syntax."""
        node = GraphNode(properties_={"name": "Alice", "age": 30, "is_active": True, "gid_": "abc123"})

        assert node["name"] == "Alice"
        assert node["age"] == 30
        assert node["is_active"] is True
        assert node["gid_"] == "abc123"

    def test_special_field_access(self):
        """Test accessing special fields via dictionary syntax."""
        node = GraphNode(id_=123, label_="Person", properties_={"name": "Alice"})

        assert node["id_"] == 123
        assert node["label_"] == "Person"

    def test_property_setting(self):
        """Test setting properties via dictionary syntax."""
        node = GraphNode(properties_={"name": "Alice"})

        node["name"] = "Bob"
        assert node["name"] == "Bob"
        assert node.properties_["name"] == "Bob"

        node["age"] = 25
        assert node["age"] == 25
        assert node.properties_["age"] == 25

    def test_special_field_setting(self):
        """Test setting special fields via dictionary syntax."""
        node = GraphNode(properties_={})

        node["id_"] = 456
        assert node["id_"] == 456
        assert node.id_ == 456

        node["label_"] = "User"
        assert node["label_"] == "User"
        assert node.label_ == "User"

    def test_contains_operator(self):
        """Test the 'in' operator for checking key existence."""
        node = GraphNode(id_=123, label_="Person", properties_={"name": "Alice", "age": 30})

        # Properties should be found
        assert "name" in node
        assert "age" in node

        # Special fields should be found
        assert "id_" in node
        assert "label_" in node

        # Non-existent keys should not be found
        assert "nonexistent" not in node

        # Internal fields should not be found
        assert "properties_" not in node
        assert "type_" not in node

    def test_get_method(self):
        """Test the get method with default values."""
        node = GraphNode(properties_={"name": "Alice"})

        assert node.get("name") == "Alice"
        assert node.get("age", 25) == 25
        assert node.get("nonexistent") is None

    def test_keys_values_items(self):
        """Test the keys(), values(), and items() methods."""
        node = GraphNode(id_=123, label_="Person", properties_={"name": "Alice", "age": 30})

        keys = node.keys()
        assert "name" in keys
        assert "age" in keys
        assert "id_" in keys
        assert "label_" in keys
        assert "properties_" not in keys

        values = list(node.values())
        assert "Alice" in values
        assert 30 in values
        assert 123 in values
        assert "Person" in values

        items_dict = dict(node.items())
        assert items_dict["name"] == "Alice"
        assert items_dict["age"] == 30
        assert items_dict["id_"] == 123
        assert items_dict["label_"] == "Person"

    def test_internal_field_access_denied(self):
        """Test that internal fields cannot be accessed."""
        node = GraphNode(properties_={"name": "Alice"})

        with pytest.raises(KeyError, match="'properties_' is not accessible"):
            _ = node["properties_"]

        with pytest.raises(KeyError, match="'type_' is not accessible"):
            _ = node["type_"]

        with pytest.raises(KeyError, match="'properties_' cannot be set"):
            node["properties_"] = {}

        with pytest.raises(KeyError, match="'type_' cannot be set"):
            node["type_"] = "something"

    def test_missing_key_error(self):
        """Test that accessing non-existent keys raises KeyError."""
        node = GraphNode(properties_={"name": "Alice"})

        with pytest.raises(KeyError, match="'nonexistent' not found"):
            _ = node["nonexistent"]

    def test_none_special_fields(self):
        """Test behavior with None special fields."""
        node = GraphNode(properties_={"name": "Alice"})

        # Should return None for unset special fields
        assert node["id_"] is None
        assert node["label_"] is None

        # Should not be considered as 'in' the node
        assert "id_" not in node
        assert "label_" not in node


class TestGraphEdgeDictAccess:
    """Test dictionary-like access for GraphEdge."""

    def test_edge_special_fields(self):
        """Test accessing edge-specific special fields."""
        edge = GraphEdge(id_=456, label_="WORKS_FOR", start_id_=123, end_id_=789, properties_={"since": 2020, "role": "Engineer"})

        # Test property access
        assert edge["since"] == 2020
        assert edge["role"] == "Engineer"

        # Test special field access including edge-specific ones
        assert edge["id_"] == 456
        assert edge["label_"] == "WORKS_FOR"
        assert edge["start_id_"] == 123
        assert edge["end_id_"] == 789

    def test_edge_field_setting(self):
        """Test setting edge-specific fields."""
        edge = GraphEdge(properties_={})

        edge["start_id_"] = 100
        edge["end_id_"] = 200

        assert edge["start_id_"] == 100
        assert edge["end_id_"] == 200
        assert edge.start_id_ == 100
        assert edge.end_id_ == 200

    def test_edge_contains_operator(self):
        """Test 'in' operator for edge-specific fields."""
        edge = GraphEdge(start_id_=123, end_id_=789, properties_={"role": "Manager"})

        assert "start_id_" in edge
        assert "end_id_" in edge
        assert "role" in edge

    def test_edge_keys_includes_edge_fields(self):
        """Test that keys() includes edge-specific fields."""
        edge = GraphEdge(id_=456, start_id_=123, end_id_=789, properties_={"role": "Engineer"})

        keys = edge.keys()
        assert "id_" in keys
        assert "start_id_" in keys
        assert "end_id_" in keys
        assert "role" in keys
