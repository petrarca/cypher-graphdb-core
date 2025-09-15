import pytest

from cypher_graphdb import config
from cypher_graphdb.models import (
    Graph,
    GraphEdge,
    GraphLabelMixin,
    GraphNode,
    GraphObjectType,
    GraphPath,
)


def test_label_to_object_type():
    assert GraphLabelMixin.label_to_obj_type("BELONGS_TO") == GraphObjectType.EDGE
    assert GraphLabelMixin.label_to_obj_type("_BELONGS_TO") == GraphObjectType.EDGE
    assert GraphLabelMixin.label_to_obj_type(":BELONGS_TO") == GraphObjectType.UNDEFINED
    assert GraphLabelMixin.label_to_obj_type("Product") == GraphObjectType.NODE
    assert GraphLabelMixin.label_to_obj_type("_Product") == GraphObjectType.NODE
    assert GraphLabelMixin.label_to_obj_type("Product_Label") == GraphObjectType.NODE
    assert GraphLabelMixin.label_to_obj_type(":Product") == GraphObjectType.UNDEFINED
    assert GraphLabelMixin.label_to_obj_type("123") == GraphObjectType.UNDEFINED
    assert GraphLabelMixin.label_to_obj_type("'string'") == GraphObjectType.UNDEFINED


def test_flatten_properties_filters_internal_and_keeps_gid():
    node = GraphNode(
        id_=1,
        label_="Person",
        properties_={"name": "Alice", config.PROP_GID: "g1", "temp_": 42},
    )
    flattened = node.flatten_properties()
    # gid_ kept
    assert config.PROP_GID in flattened
    # dynamic property name preserved
    assert flattened["name"] == "Alice"
    # internal style suffix field removed
    assert "temp_" not in flattened


def test_create_gid_if_missing():
    node = GraphNode(label_="Person", properties_={})
    assert config.PROP_GID not in node.properties_
    node.create_gid_if_missing()
    assert config.PROP_GID in node.properties_
    assert len(node.properties_[config.PROP_GID]) == config.GID_LENGTH


def test_identifier_bind_unbind():
    node = GraphNode(label_="Person", properties_={})
    assert node.id_ is None
    node.bind_id(123)
    assert node.id_ == 123
    prev = node.unbind_id()
    assert prev == 123
    assert node.id_ is None


def test_edge_build_from_nodes_and_ids():
    n1 = GraphNode(id_=10, label_="A", properties_={})
    n2 = GraphNode(id_=11, label_="B", properties_={})
    e = GraphEdge.build(
        n1,
        n2,
        label_="REL",
        properties_={},
    )
    assert e.start_id_ == 10 and e.end_id_ == 11
    # build using direct ids
    e2 = GraphEdge.build(20, 21, label_="REL", properties_={})
    assert e2.start_id_ == 20 and e2.end_id_ == 21


def test_node_and_edge_serialization():
    n = GraphNode(id_=1, label_="Person", properties_={"name": "Alice"})
    e = GraphEdge(
        id_=100,
        start_id_=1,
        end_id_=2,
        label_="KNOWS",
        properties_={"since": 2020},
    )
    n_ser = n.model_dump()
    e_ser = e.model_dump()
    assert n_ser["node"]["label_"] == "Person"
    assert e_ser["edge"]["label_"] == "KNOWS"
    assert e_ser["edge"]["properties_"]["since"] == 2020


def test_graph_merge_deduplicates_by_id():
    g = Graph()
    n1 = GraphNode(id_=1, label_="X", properties_={})
    g.merge(n1)
    g.merge(n1)  # second merge should not duplicate
    assert len(g.nodes) == 1
    # merging another object with same id different instance is ignored
    n1_clone = GraphNode(id_=1, label_="X", properties_={})
    g.merge(n1_clone)
    assert len(g.nodes) == 1


def test_graph_path_append_and_iter():
    n1 = GraphNode(id_=1, label_="A", properties_={})
    n2 = GraphNode(id_=2, label_="B", properties_={})
    e = GraphEdge(
        id_=100,
        start_id_=1,
        end_id_=2,
        label_="REL",
        properties_={},
    )
    path = GraphPath()
    path.append(n1)
    path.append(e)
    path.append(n2)
    assert len(path.entities) == 3
    assert list(iter(path))[0] == n1
    dumped = path.model_dump()
    assert dumped["path"][0]["node"]["id_"] == 1


def test_resolve_with_label_and_without_label():
    # resolve with label provided
    n = GraphNode(label_="Person", properties_={})
    n.resolve()
    assert n.label_ == "Person"
    # resolve without label should raise
    n2 = GraphNode(properties_={})  # missing label
    with pytest.raises(RuntimeError):
        n2.resolve()


# ---------------------------------------------------------------------------
# Additional mixin coverage
# ---------------------------------------------------------------------------


def test_is_valid_label():
    assert GraphLabelMixin.is_valid_label("Product") is True
    assert GraphLabelMixin.is_valid_label("RELATES_TO") is True
    # invalid starts with ':'
    assert GraphLabelMixin.is_valid_label(":Bad") is False
    # invalid numeric start
    assert GraphLabelMixin.is_valid_label("123ABC") is False


def test_identifier_has_id_toggle():
    node = GraphNode(label_="Item", properties_={})
    assert node.has_id is False
    node.bind_id(55)
    assert node.has_id is True
    node.unbind_id()
    assert node.has_id is False


def test_gid_accessor_and_preserve_existing():
    node = GraphNode(
        label_="Person",
        properties_={config.PROP_GID: "existing"},
    )
    assert node.has_gid is True
    assert node.gid_ == "existing"
    node.create_gid_if_missing()  # should not overwrite
    assert node.gid_ == "existing"
    node2 = GraphNode(label_="Person", properties_={})
    assert node2.gid_ is None
    node2.create_gid_if_missing()
    assert node2.gid_ is not None


def test_resolve_model_properties_defaults():
    props = {"name": "Alice", "age": 30}
    node = GraphNode(label_="Person", properties_=props)
    fields = node.resolve_model_properties()
    assert "name" in fields and "age" in fields
    # internal & reserved removed
    assert "id_" not in fields and "label_" not in fields
    # default types (not original values)
    assert fields["name"].default == ""
    assert fields["age"].default == 0


def test_resolve_model_properties_with_values():
    props = {"name": "Bob", "active": True}
    node = GraphNode(label_="Person", properties_=props)
    fields = node.resolve_model_properties(default_from_values=True)
    assert fields["name"].default == "Bob"
    assert fields["active"].default is True


def test_node_serialization_without_type_context():
    node = GraphNode(id_=7, label_="Thing", properties_={"x": 1})
    dumped = node.model_dump(context={"with_type": False})
    # when with_type False we expect raw dict WITHOUT wrapper key
    assert dumped["id_"] == 7
    assert dumped["label_"] == "Thing"
    assert dumped["properties_"]["x"] == 1


def test_edge_serialization_without_type_context():
    edge = GraphEdge(
        id_=701,
        start_id_=7,
        end_id_=8,
        label_="REL",
        properties_={"w": 2},
    )
    dumped = edge.model_dump(context={"with_type": False})
    assert dumped["id_"] == 701
    assert dumped["label_"] == "REL"
    assert dumped["properties_"]["w"] == 2
