"""Unit tests for DataFlattener used in hierarchical import.

Tests cover:
- Node data extraction
- Relationship detection and extraction
- Edge property handling
- Target node property extraction
- Various relationship patterns (camelCase, snake_case)
"""

from cypher_graphdb.tools.data_flattener import DataFlattener


def test_flatten_simple_node():
    """Test flattening a simple node without relationships."""
    item = {"gid_": "apple", "name": "Apple Inc.", "founded": 1976}

    flattened = DataFlattener.flatten_item(item, "Company")

    assert flattened.source_label == "Company"
    assert flattened.source_gid == "apple"
    # Note: label_ is added by the flattener
    expected_node_data = {"gid_": "apple", "name": "Apple Inc.", "founded": 1976, "label_": "Company"}
    assert flattened.node_data == expected_node_data
    assert len(flattened.relations) == 0


def test_flatten_node_with_single_relationship():
    """Test flattening node with a single relationship using edge: prefix."""
    item = {
        "gid_": "iphone15",
        "name": "iPhone 15",
        "price": 999,
        "edge:IP_OWNED_BY": {"target_gid": "apple", "target_label": "Company", "since": 2007},
    }

    flattened = DataFlattener.flatten_item(item, "Product")

    # Node data should exclude relationships but include label
    expected_node_data = {"gid_": "iphone15", "name": "iPhone 15", "price": 999, "label_": "Product"}
    assert flattened.node_data == expected_node_data

    # Should extract one relationship
    assert len(flattened.relations) == 1

    relation = flattened.relations[0]
    assert relation["relation_key"] == "IP_OWNED_BY"
    assert relation["data"]["target_gid"] == "apple"
    assert relation["data"]["target_label"] == "Company"
    assert relation["data"]["since"] == 2007


def test_flatten_node_with_multiple_relationships():
    """Test flattening node with multiple relationships using edge: prefix."""
    item = {
        "gid_": "iphone",
        "name": "iPhone",
        "edge:IP_OWNED_BY": {"target_gid": "apple", "target_label": "Company", "since": 2007},
        "edge:USES_TECHNOLOGY": {"target_gid": "ios", "target_label": "Technology", "version": "17"},
    }

    flattened = DataFlattener.flatten_item(item, "Product")

    # Should extract both relationships
    assert len(flattened.relations) == 2

    relation_keys = {rel["relation_key"] for rel in flattened.relations}
    assert relation_keys == {"IP_OWNED_BY", "USES_TECHNOLOGY"}

    # Check first relationship
    ip_owned_rel = next(rel for rel in flattened.relations if rel["relation_key"] == "IP_OWNED_BY")
    assert ip_owned_rel["data"]["target_gid"] == "apple"
    assert ip_owned_rel["data"]["target_label"] == "Company"

    # Check second relationship
    uses_tech_rel = next(rel for rel in flattened.relations if rel["relation_key"] == "USES_TECHNOLOGY")
    assert uses_tech_rel["data"]["target_gid"] == "ios"
    assert uses_tech_rel["data"]["target_label"] == "Technology"
    assert uses_tech_rel["data"]["version"] == "17"


def test_flatten_node_with_relationship_list():
    """Test flattening node with list of relationships using edge: prefix."""
    item = {
        "gid_": "company",
        "name": "TechCorp",
        "edge:EMPLOYS": [
            {"target_gid": "alice", "target_label": "Person", "role": "Engineer"},
            {"target_gid": "bob", "target_label": "Person", "role": "Manager"},
        ],
    }

    flattened = DataFlattener.flatten_item(item, "Company")

    # Should extract list as multiple relationships
    assert len(flattened.relations) == 2

    for rel in flattened.relations:
        assert rel["relation_key"] == "EMPLOYS"
        assert "target_gid" in rel["data"]
        assert "target_label" in rel["data"]


def test_relationship_detection_patterns():
    """Test various relationship detection patterns with explicit edge: prefix."""
    test_cases = [
        ("edge:ipownedby", True),  # edge: prefix = relation
        ("edge:usestechnology", True),  # edge: prefix = relation
        ("edge:IP_OWNED_BY", True),  # edge: prefix = relation
        ("edge:USES_TECHNOLOGY", True),  # edge: prefix = relation
        ("edge:owned_by", True),  # edge: prefix = relation
        ("name", False),  # primitive string = not a relation
        ("description", False),  # primitive string = not a relation
        ("metadata", False),  # primitive string = not a relation
    ]

    for field_name, should_be_relation in test_cases:
        if should_be_relation:
            # Relationship fields have edge: prefix and target_gid
            item = {"gid_": "test", field_name: {"target_gid": "target", "target_label": "Entity", "since": 2007}}
        else:
            # Non-relationship fields have primitive values
            item = {"gid_": "test", field_name: "Target"}

        flattened = DataFlattener.flatten_item(item, "Test")

        if should_be_relation:
            assert len(flattened.relations) == 1
            # relation_key should have edge: prefix removed
            expected_key = field_name.removeprefix("edge:")
            assert flattened.relations[0]["relation_key"] == expected_key
        else:
            assert len(flattened.relations) == 0
            assert field_name in flattened.node_data


def test_extract_edge_properties():
    """Test extraction of edge properties from relationship data.

    In explicit format, edge properties are everything except target_gid and target_label.
    """
    relation_data = {
        "target_gid": "apple",
        "target_label": "Company",
        "since": 2007,
        "type": "majority",
    }

    edge_props = DataFlattener.extract_edge_properties(relation_data)

    # Should include all properties except target_gid and target_label
    expected = {"since": 2007, "type": "majority"}
    assert edge_props == expected


def test_extract_target_node_properties():
    """Test extraction of target node properties.

    In explicit format, target node properties are minimal - just the gid_ derived from target_gid.
    """
    relation_data = {
        "target_gid": "apple",
        "target_label": "Company",
        "since": 2007,
        "type": "majority",
    }

    target_props = DataFlattener.extract_target_node_properties(relation_data)

    # Should only include gid_ derived from target_gid
    expected = {"gid_": "apple"}
    assert target_props == expected


def test_empty_and_null_handling():
    """Test handling of empty and null values."""
    # Empty relationship
    item1 = {"gid_": "test1", "name": "Test 1", "relationship": {}}

    flattened1 = DataFlattener.flatten_item(item1, "Test")
    assert len(flattened1.relations) == 0

    # Null relationship
    item2 = {"gid_": "test2", "name": "Test 2", "relationship": None}

    flattened2 = DataFlattener.flatten_item(item2, "Test")
    assert len(flattened2.relations) == 0

    # Empty list relationship
    item3 = {"gid_": "test3", "name": "Test 3", "relationship": []}

    flattened3 = DataFlattener.flatten_item(item3, "Test")
    assert len(flattened3.relations) == 0


def test_nested_relationship_structure():
    """Test complex nested relationship structures using edge: prefix."""
    item = {
        "gid_": "complex",
        "name": "Complex Node",
        "edge:HAS_PARTS": {
            "target_gid": "part1",
            "target_label": "Part",
            "subcomponents": {"target_gid": "sub1", "target_label": "SubComponent"},
        },
    }

    flattened = DataFlattener.flatten_item(item, "Complex")

    # Should extract top-level relationship only
    assert len(flattened.relations) == 1

    relation = flattened.relations[0]
    assert relation["relation_key"] == "HAS_PARTS"
    assert relation["data"]["target_gid"] == "part1"
    assert relation["data"]["target_label"] == "Part"
    assert "subcomponents" in relation["data"]
