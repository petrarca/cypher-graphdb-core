from cypher_graphdb.modelinfo import (
    GraphEdgeInfo,
    GraphModelInfo,
    GraphNodeInfo,
    GraphRelationInfo,
)
from cypher_graphdb.models import GraphEdge, GraphNode, GraphObjectType


def test_graph_model_info_basic_fields_filtering():
    info = GraphModelInfo(label_="Person", graph_model=GraphNode, metadata={})
    # node fields exclude id_, label_, properties_
    filtered = info.fields
    assert "id_" not in filtered and "label_" not in filtered
    # ensure dict like structure
    assert isinstance(filtered, dict)


def test_graph_model_info_serialization_compact_vs_detailed():
    info = GraphModelInfo(label_="Person", graph_model=GraphNode, metadata={"a": 1})
    compact = info.model_dump()
    assert compact["graph_model"].endswith("GraphNode")
    assert isinstance(compact["fields"], list)
    detailed = info.model_dump(context={"with_detailed_fields": True})
    assert isinstance(detailed["fields"], dict)
    assert detailed["has_schema"] is False  # no custom schema set


def test_graph_node_info_relations_serialization():
    rels = [GraphRelationInfo(rel_type_name="KNOWS", to_type_name="Person")]
    info = GraphNodeInfo(label_="Person", graph_model=GraphNode, metadata={}, relations=rels)
    dumped = info.model_dump()
    assert dumped["type_"] == GraphObjectType.NODE
    # to_collection converts objects to dicts
    assert dumped["relations"][0]["rel_type_name"] == "KNOWS"
    assert dumped["relations"][0]["to_type_name"] == "Person"


def test_graph_edge_info_type_and_serialization():
    info = GraphEdgeInfo(label_="KNOWS", graph_model=GraphEdge, metadata={})
    dumped = info.model_dump()
    assert dumped["type_"] == GraphObjectType.EDGE
    assert dumped["label_"] == "KNOWS"


def test_model_info_hash_and_equality():
    info1 = GraphModelInfo(label_="Book", graph_model=GraphNode, metadata={})
    info2 = GraphModelInfo(label_="Book", graph_model=GraphNode, metadata={})
    info3 = GraphModelInfo(label_="Author", graph_model=GraphNode, metadata={})
    assert hash(info1) == hash(info2)
    assert info1 == "Book"
    assert info1 != "Author"
    assert hash(info1) != hash(info3)


def test_schema_flag_custom_assignment():
    info = GraphModelInfo(label_="Doc", graph_model=GraphNode, metadata={})
    assert info.graph_schema.has_schema is False
    # assign custom schema
    custom = {"title": "Doc", "type": "object"}
    info.graph_schema.json_schema = custom
    assert info.graph_schema.has_schema is True
    dumped = info.model_dump()
    assert dumped["has_schema"] is True
