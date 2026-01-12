"""Unit tests for schema to graph converter."""

import uuid

import pytest

from cypher_graphdb.schema.converter import json_schemas_to_graph


@pytest.fixture
def sample_schemas():
    """Sample JSON schemas for testing."""
    return [
        {
            "title": "Person",
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name"],
            "x-graph": {
                "type": "NODE",
                "label": "Person",
                "relations": [
                    {"rel_type_name": "KNOWS", "to_type_name": "Person", "cardinality": "many-to-many"},
                    {"rel_type_name": "WORKS_FOR", "to_type_name": "Organization", "cardinality": "many-to-one"},
                ],
            },
        },
        {
            "title": "Organization",
            "type": "object",
            "properties": {"name": {"type": "string"}, "founded": {"type": "integer"}},
            "required": ["name"],
            "x-graph": {"type": "NODE", "label": "Organization", "relations": []},
        },
    ]


def test_json_schemas_to_graph_returns_graph(sample_schemas):
    """Test that function returns Graph object."""
    graph = json_schemas_to_graph(sample_schemas)

    assert graph is not None
    assert hasattr(graph, "nodes")
    assert hasattr(graph, "edges")


def test_creates_node_for_each_schema(sample_schemas):
    """Test that each schema becomes a GraphNode."""
    graph = json_schemas_to_graph(sample_schemas)

    assert len(graph.nodes) == 2
    node_names = [node.properties_["name"] for node in graph.nodes.values()]
    assert "Person" in node_names
    assert "Organization" in node_names


def test_nodes_have_graphnode_label(sample_schemas):
    """Test that all nodes have label 'GraphNode'."""
    graph = json_schemas_to_graph(sample_schemas)

    for node in graph.nodes.values():
        assert node.label_ == "GraphNode"


def test_nodes_have_stable_numeric_ids(sample_schemas):
    """Test that node IDs are deterministic."""
    graph1 = json_schemas_to_graph(sample_schemas)
    graph2 = json_schemas_to_graph(sample_schemas)

    nodes1 = {node.properties_["name"]: node.id_ for node in graph1.nodes.values()}
    nodes2 = {node.properties_["name"]: node.id_ for node in graph2.nodes.values()}

    assert nodes1["Person"] == nodes2["Person"]
    assert nodes1["Organization"] == nodes2["Organization"]


def test_nodes_have_string_gid(sample_schemas):
    """Test that nodes have string-based gid_ with schema prefix."""
    graph = json_schemas_to_graph(sample_schemas)

    for node in graph.nodes.values():
        assert "gid_" in node.properties_
        assert node.properties_["gid_"].startswith("schema:")

    gids = [node.properties_["gid_"] for node in graph.nodes.values()]
    assert "schema:Person" in gids
    assert "schema:Organization" in gids


def test_node_properties_contain_schema_details(sample_schemas):
    """Test that node properties include all schema details."""
    graph = json_schemas_to_graph(sample_schemas)

    person_node = next(n for n in graph.nodes.values() if n.properties_["name"] == "Person")

    assert person_node.properties_["type"] == "object"
    assert "name" in person_node.properties_["propertyDefinitions"]
    assert "age" in person_node.properties_["propertyDefinitions"]
    assert person_node.properties_["required"] == ["name"]
    assert "xGraph" in person_node.properties_
    assert person_node.properties_["xGraph"]["type"] == "NODE"


def test_creates_edges_from_relations(sample_schemas):
    """Test that relations become GraphEdge objects."""
    graph = json_schemas_to_graph(sample_schemas)

    # Person has 2 relations: KNOWS (to Person) and WORKS_FOR (to Organization)
    assert len(graph.edges) == 2


def test_edges_have_relation_name_as_label(sample_schemas):
    """Test that edge labels use the actual relation name."""
    graph = json_schemas_to_graph(sample_schemas)

    edge_labels = {edge.label_ for edge in graph.edges.values()}
    assert "KNOWS" in edge_labels
    assert "WORKS_FOR" in edge_labels


def test_edges_have_stable_numeric_ids(sample_schemas):
    """Test that edge IDs are deterministic."""
    graph1 = json_schemas_to_graph(sample_schemas)
    graph2 = json_schemas_to_graph(sample_schemas)

    edges1 = sorted([edge.id_ for edge in graph1.edges.values()])
    edges2 = sorted([edge.id_ for edge in graph2.edges.values()])

    assert edges1 == edges2


def test_edges_have_string_gid(sample_schemas):
    """Test that edges have string-based gid_ with schema:rel prefix."""
    graph = json_schemas_to_graph(sample_schemas)

    for edge in graph.edges.values():
        assert "gid_" in edge.properties_
        assert edge.properties_["gid_"].startswith("schema:rel:")


def test_edge_properties_contain_relation_details(sample_schemas):
    """Test that edge properties include relation metadata."""
    graph = json_schemas_to_graph(sample_schemas)

    knows_edge = next(e for e in graph.edges.values() if e.properties_["name"] == "KNOWS")

    assert knows_edge.properties_["name"] == "KNOWS"
    assert knows_edge.properties_["cardinality"] == "many-to-many"


def test_edge_connects_correct_nodes(sample_schemas):
    """Test that edges connect source and target nodes correctly."""
    graph = json_schemas_to_graph(sample_schemas)

    # Find Person and Organization node IDs
    person_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "Person")
    org_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "Organization")

    # Find WORKS_FOR edge (Person -> Organization)
    works_for_edge = next(e for e in graph.edges.values() if e.properties_["name"] == "WORKS_FOR")

    assert works_for_edge.start_id_ == person_id
    assert works_for_edge.end_id_ == org_id


def test_self_referencing_relation(sample_schemas):
    """Test that self-referencing relations (Person KNOWS Person) work correctly."""
    graph = json_schemas_to_graph(sample_schemas)

    person_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "Person")
    knows_edge = next(e for e in graph.edges.values() if e.properties_["name"] == "KNOWS")

    assert knows_edge.start_id_ == person_id
    assert knows_edge.end_id_ == person_id


def test_empty_schemas_list():
    """Test that empty schemas list returns empty graph."""
    graph = json_schemas_to_graph([])

    assert len(graph.nodes) == 0
    assert len(graph.edges) == 0


def test_schema_without_relations():
    """Test that schema without relations creates node but no edges."""
    schemas = [{"title": "SimpleNode", "type": "object", "properties": {}, "x-graph": {"type": "NODE", "relations": []}}]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 1
    assert len(graph.edges) == 0


def test_schema_with_missing_target_relation():
    """Test that relation to non-existent target is skipped."""
    schemas = [
        {
            "title": "Person",
            "type": "object",
            "properties": {},
            "x-graph": {"type": "NODE", "relations": [{"rel_type_name": "KNOWS", "to_type_name": "NonExistent"}]},
        }
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 1
    assert len(graph.edges) == 0  # Edge to NonExistent should be skipped


def test_graph_serialization(sample_schemas):
    """Test that graph can be serialized to dict format."""
    graph = json_schemas_to_graph(sample_schemas)

    serialized = graph.model_dump(context={"with_type": True})

    assert "nodes" in serialized
    assert "edges" in serialized
    assert len(serialized["nodes"]) == 2
    assert len(serialized["edges"]) == 2


def test_id_generation_uses_uuid5():
    """Test that ID generation uses UUID5 for determinism."""
    schemas = [{"title": "TestNode", "type": "object", "properties": {}, "x-graph": {"type": "NODE", "relations": []}}]

    graph = json_schemas_to_graph(schemas)
    node = list(graph.nodes.values())[0]

    # Verify the ID matches expected UUID5 hash
    expected_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, "schema.node.TestNode")
    expected_id = hash(expected_uuid) & 0x7FFFFFFFFFFFFFFF

    assert node.id_ == expected_id


def test_preserves_custom_x_extensions():
    """Test that all x-* extensions are preserved in node properties."""
    schemas = [
        {
            "title": "CustomNode",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
            "x-ui-widget": "custom-widget",
            "x-ui-options": {"color": "blue", "size": "large"},
            "x-custom-metadata": {"author": "test", "version": "1.0"},
        }
    ]

    graph = json_schemas_to_graph(schemas)
    node = list(graph.nodes.values())[0]

    # Verify all x-* extensions are converted to camelCase
    assert node.properties_["xUiWidget"] == "custom-widget"
    assert node.properties_["xUiOptions"] == {"color": "blue", "size": "large"}
    assert node.properties_["xCustomMetadata"] == {"author": "test", "version": "1.0"}
    assert node.properties_["xGraph"] == {"type": "NODE", "relations": []}
