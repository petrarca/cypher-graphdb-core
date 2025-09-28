from cypher_graphdb.models import Graph, GraphNode


def test_graph_includes_node_with_id_zero():
    g = Graph()
    n0 = GraphNode(id_=0, label_="Test", properties_={"gid_": "zero"})
    n1 = GraphNode(id_=1, label_="Test", properties_={"gid_": "one"})

    g.merge([n0, n1])

    ids = {n.id_ for n in g.nodes.values()}
    assert 0 in ids and 1 in ids
    assert len(ids) == 2
