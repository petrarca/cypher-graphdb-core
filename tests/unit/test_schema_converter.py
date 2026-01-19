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


def test_node_schemas_have_graphnode_label(sample_schemas):
    """Test that NODE type schemas have label 'GraphNode'."""
    graph = json_schemas_to_graph(sample_schemas)

    for node in graph.nodes.values():
        assert node.label_ == "GraphNode"


def test_edge_schemas_have_graphedge_label():
    """Test that EDGE type schemas have label 'GraphEdge'."""
    schemas = [
        {
            "title": "KNOWS",
            "type": "object",
            "properties": {"since": {"type": "string", "format": "date"}, "strength": {"type": "integer"}},
            "x-graph": {"type": "EDGE"},
        }
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 1
    node = list(graph.nodes.values())[0]
    assert node.label_ == "GraphEdge"
    assert node.properties_["name"] == "KNOWS"


def test_mixed_node_and_edge_schemas():
    """Test that NODE and EDGE schemas get appropriate labels."""
    schemas = [
        {
            "title": "Person",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": [{"rel_type_name": "KNOWS", "to_type_name": "Person"}]},
        },
        {
            "title": "KNOWS",
            "type": "object",
            "properties": {"since": {"type": "string"}},
            "x-graph": {"type": "EDGE"},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    # Should have 2 nodes: Person (GraphNode) and KNOWS (GraphEdge)
    assert len(graph.nodes) == 2

    node_labels = {n.properties_["name"]: n.label_ for n in graph.nodes.values()}
    assert node_labels["Person"] == "GraphNode"
    assert node_labels["KNOWS"] == "GraphEdge"

    # Should have 1 edge from relations
    assert len(graph.edges) == 1


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


def test_single_inheritance_creates_inherits_from_edge():
    """Test that single inheritance creates INHERITS_FROM_ edge."""
    schemas = [
        {
            "title": "Product",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
        },
        {
            "title": "TechProduct",
            "type": "object",
            "properties": {"tech_stack": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": [], "inherits_from": ["Product"]},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1

    inheritance_edge = list(graph.edges.values())[0]
    assert inheritance_edge.label_ == "INHERITS_FROM_"

    # Verify edge connects child to parent
    product_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "Product")
    tech_product_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "TechProduct")

    assert inheritance_edge.start_id_ == tech_product_id
    assert inheritance_edge.end_id_ == product_id


def test_multiple_inheritance_creates_multiple_edges():
    """Test that multiple inheritance creates multiple INHERITS_FROM_ edges."""
    schemas = [
        {
            "title": "Product",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
        },
        {
            "title": "Auditable",
            "type": "object",
            "properties": {"created_at": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
        },
        {
            "title": "AuditedProduct",
            "type": "object",
            "properties": {"version": {"type": "integer"}},
            "x-graph": {"type": "NODE", "relations": [], "inherits_from": ["Product", "Auditable"]},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 3

    # Should have 2 inheritance edges
    inheritance_edges = [e for e in graph.edges.values() if e.label_ == "INHERITS_FROM_"]
    assert len(inheritance_edges) == 2

    # Verify both edges start from AuditedProduct
    audited_product_id = next(n.id_ for n in graph.nodes.values() if n.properties_["name"] == "AuditedProduct")
    assert all(e.start_id_ == audited_product_id for e in inheritance_edges)


def test_no_inheritance_creates_no_inherits_from_edges():
    """Test that schemas without inherits_from field don't create inheritance edges."""
    schemas = [
        {
            "title": "Product",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
        },
        {
            "title": "Company",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "relations": []},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 2
    assert len(graph.edges) == 0


def test_inheritance_edge_properties():
    """Test that inheritance edges have correct properties."""
    schemas = [
        {"title": "BaseClass", "type": "object", "properties": {}, "x-graph": {"type": "NODE", "relations": []}},
        {
            "title": "DerivedClass",
            "type": "object",
            "properties": {},
            "x-graph": {"type": "NODE", "relations": [], "inherits_from": ["BaseClass"]},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    edge = list(graph.edges.values())[0]

    assert edge.label_ == "INHERITS_FROM_"
    assert "gid_" in edge.properties_
    assert edge.properties_["gid_"].startswith("schema:rel:")
    assert edge.properties_["name"] == "INHERITS_FROM_"
    assert edge.properties_["parent"] == "BaseClass"


def test_inheritance_with_missing_parent():
    """Test that inheritance edge to non-existent parent is skipped."""
    schemas = [
        {
            "title": "DerivedClass",
            "type": "object",
            "properties": {},
            "x-graph": {"type": "NODE", "relations": [], "inherits_from": ["NonExistentParent"]},
        }
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 1
    assert len(graph.edges) == 0  # Edge to NonExistent parent should be skipped


def test_inheritance_and_relations_coexist():
    """Test that both inheritance and relation edges are created."""
    schemas = [
        {"title": "BaseProduct", "type": "object", "properties": {}, "x-graph": {"type": "NODE", "relations": []}},
        {"title": "Category", "type": "object", "properties": {}, "x-graph": {"type": "NODE", "relations": []}},
        {
            "title": "TechProduct",
            "type": "object",
            "properties": {},
            "x-graph": {
                "type": "NODE",
                "relations": [{"rel_type_name": "BELONGS_TO", "to_type_name": "Category"}],
                "inherits_from": ["BaseProduct"],
            },
        },
    ]

    graph = json_schemas_to_graph(schemas)

    assert len(graph.nodes) == 3
    assert len(graph.edges) == 2  # 1 inheritance + 1 relation

    inheritance_edges = [e for e in graph.edges.values() if e.label_ == "INHERITS_FROM_"]
    relation_edges = [e for e in graph.edges.values() if e.label_ == "BELONGS_TO"]

    assert len(inheritance_edges) == 1
    assert len(relation_edges) == 1


def test_edge_inheritance():
    """Test that EDGE schemas can also have inheritance."""
    schemas = [
        {"title": "Relationship", "type": "object", "properties": {"since": {"type": "string"}}, "x-graph": {"type": "EDGE"}},
        {
            "title": "Partnership",
            "type": "object",
            "properties": {"contract_type": {"type": "string"}},
            "x-graph": {"type": "EDGE", "inherits_from": ["Relationship"]},
        },
    ]

    graph = json_schemas_to_graph(schemas)

    # Both should become GraphEdge nodes (not connected edges)
    assert len(graph.nodes) == 2
    assert all(n.label_ == "GraphEdge" for n in graph.nodes.values())

    # Should have 1 inheritance edge between them
    assert len(graph.edges) == 1
    edge = list(graph.edges.values())[0]
    assert edge.label_ == "INHERITS_FROM_"
