from cypher_graphdb.modelinfo import GraphEdgeInfo, GraphModelInfo, GraphNodeInfo, GraphRelationInfo
from cypher_graphdb.models import GraphEdge, GraphNode, GraphObjectType
from cypher_graphdb.schema import GraphSchemaContext, build_json_schema


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


def test_graph_model_info_detailed_fields_content():
    class SampleNode(GraphNode):
        required: str
        optional: str | None = None

    info = GraphModelInfo(label_="Sample", graph_model=SampleNode, metadata={})

    detailed = info.model_dump(context={"with_detailed_fields": True})
    fields = detailed["fields"]

    assert fields["required"]["annotation"] == "str"
    assert fields["required"]["required"] is True
    assert "default" not in fields["required"]

    optional_annotation = fields["optional"]["annotation"]
    assert optional_annotation in {"str | None", "typing.Optional[str]"}
    assert fields["optional"]["required"] is False
    assert fields["optional"]["default"] is None


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
    assert "has_schema" not in dumped


def test_graph_model_info_json_schema_without_extensions():
    info = GraphModelInfo(label_="Doc", graph_model=GraphNode, metadata={})

    schema = build_json_schema(info.graph_model)

    assert "x-cypher-graph" not in schema
    assert schema["title"] == "GraphNode"


def test_graph_node_info_json_schema_with_extensions():
    rels = [GraphRelationInfo(rel_type_name="KNOWS", to_type_name="Person")]
    info = GraphNodeInfo(label_="Person", graph_model=GraphNode, metadata={"team": "core"}, relations=rels)

    graph_model_ref = f"{info.graph_model.__module__}.{info.graph_model.__name__}"
    context = GraphSchemaContext(
        label=info.label_,
        metadata=info.metadata,
        graph_type=GraphObjectType.NODE,
        relations=info.relations,
        graph_model_ref=graph_model_ref,
    )
    schema = build_json_schema(info.graph_model, context=context)

    assert "x-graph" in schema
    extension = schema["x-graph"]
    assert extension["type"] == GraphObjectType.NODE.name
    assert extension["label"] == "Person"
    assert extension["metadata"] == {"team": "core"}
    assert extension["graph_model"].endswith("GraphNode")
    assert extension["relations"][0] == rels[0].model_dump()

    schema_from_property = info.graph_schema.json_schema
    assert schema_from_property["x-graph"] == extension
