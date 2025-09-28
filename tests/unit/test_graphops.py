import pytest

import cypher_graphdb.graphops as gops
from cypher_graphdb import Graph, GraphEdge, GraphNode

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def graph_simple() -> Graph:
    graph = Graph()
    for i in range(1, 6):
        graph.merge(GraphNode(id_=i))
    graph.merge(GraphEdge(id_=1001, start_id_=1, end_id_=2))  # 1 -> 2
    return graph


@pytest.fixture
def graph_tree() -> Graph:
    graph = Graph()
    for i in range(1, 7):
        graph.merge(GraphNode(id_=i))
    # node(6) remains unbound
    graph.merge(GraphEdge(id_=1002, start_id_=2, end_id_=1, label_="SCO"))
    graph.merge(GraphEdge(id_=1003, start_id_=3, end_id_=1, label_="SCO"))
    graph.merge(GraphEdge(id_=1004, start_id_=4, end_id_=2, label_="SCO"))
    graph.merge(GraphEdge(id_=1005, start_id_=5, end_id_=4, label_="SCO"))
    graph.merge(GraphEdge(id_=1006, start_id_=3, end_id_=3, label_="SCO"))  # self edge
    return graph


@pytest.fixture
def graph_selfref() -> Graph:
    graph = Graph()
    for i in range(1, 3):
        graph.merge(GraphNode(id_=i))
    graph.merge(GraphEdge(id_=1001, start_id_=1, end_id_=1))
    graph.merge(GraphEdge(id_=1002, start_id_=1, end_id_=2))
    return graph


@pytest.fixture
def graph_missing() -> Graph:
    graph = Graph()
    for i in range(1, 3):
        graph.merge(GraphNode(id_=i))
    graph.merge(GraphEdge(id_=1001, start_id_=1, end_id_=2))
    graph.merge(GraphEdge(id_=1002, start_id_=1, end_id_=101))
    graph.merge(GraphEdge(id_=1003, start_id_=1, end_id_=101))
    graph.merge(GraphEdge(id_=1004, start_id_=102, end_id_=2))
    graph.merge(GraphEdge(id_=1005, start_id_=101, end_id_=103))
    graph.merge(GraphEdge(id_=1006, start_id_=101, end_id_=101))
    return graph


@pytest.fixture
def empty_graph() -> Graph:
    return Graph()


# ---------------------------------------------------------------------------
# Basic metrics & retrieval
# ---------------------------------------------------------------------------


def test_is_empty(empty_graph):
    assert gops.is_empty(empty_graph) is True


def test_density(graph_simple):
    assert gops.density(graph_simple) == 0.05


def test_density_empty_graph_raises(empty_graph):
    # Current implementation divides by zero when n < 2; document behavior.
    with pytest.raises(ZeroDivisionError):
        gops.density(empty_graph)


def test_edges_and_nodes_collections(graph_simple):
    assert len(gops.nodes(graph_simple)) == 5
    assert len(gops.edges(graph_simple)) == 1


def test_get_node_and_edge(graph_simple):
    assert gops.get_node(graph_simple, 1) is graph_simple.nodes[1]
    assert gops.get_edge(graph_simple, 1001) is graph_simple.edges[1001]
    assert gops.get_node(graph_simple, 9999) is None
    assert gops.get_edge(graph_simple, 9999) is None


# ---------------------------------------------------------------------------
# Edge / adjacency queries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "start_id,end_id,expected_count",
    [
        (1, 2, 1),
        (2, 1, 0),
        (1, 3, 0),
    ],
)
def test_edges_between_nodes(graph_simple, start_id, end_id, expected_count):
    result = gops.edges_between_nodes(graph_simple, start_id, end_id)
    assert len(result) == expected_count
    if expected_count:
        assert result[0] == graph_simple.edges[1001]


def test_incoming_nodes(graph_simple):
    result = gops.incoming_nodes(graph_simple, graph_simple.nodes[2])
    assert len(result) == 1
    edge, source = result[0]
    assert edge == graph_simple.edges[1001]
    assert source == graph_simple.nodes[1]


def test_outgoing_nodes(graph_simple):
    result = gops.outgoing_nodes(graph_simple, graph_simple.nodes[1])
    assert len(result) == 1
    edge, target = result[0]
    assert edge == graph_simple.edges[1001]
    assert target == graph_simple.nodes[2]


def test_incoming_outgoing_label_filter(graph_tree):
    # all edges are label SCO
    incoming = gops.incoming_nodes(graph_tree, graph_tree.nodes[1], label="SCO")
    assert len(incoming) == 2
    incoming_none = gops.incoming_nodes(graph_tree, graph_tree.nodes[1], label="X")
    assert incoming_none == ()
    outgoing = gops.outgoing_nodes(graph_tree, graph_tree.nodes[2], label="SCO")
    assert len(outgoing) == 1


# ---------------------------------------------------------------------------
# Structural queries
# ---------------------------------------------------------------------------


def test_unbound_nodes(graph_tree):
    result = gops.unbound_nodes(graph_tree)
    assert len(result) == 1
    assert graph_tree.nodes[6] in result


def test_self_referenced_nodes(graph_tree, graph_selfref):
    # tree graph has one self edge on node 3
    result_tree = gops.self_referenced_nodes(graph_tree)
    assert graph_tree.nodes[3] in result_tree
    # label filter hit
    result_tree_label = gops.self_referenced_nodes(graph_tree, label="SCO")
    assert graph_tree.nodes[3] in result_tree_label
    # label filter miss
    assert gops.self_referenced_nodes(graph_tree, label="OTHER") == set()
    # separate selfref graph (node 1 self loop)
    result_sr = gops.self_referenced_nodes(graph_selfref)
    assert graph_selfref.nodes[1] in result_sr


def test_missing_nodes(graph_missing):
    result = gops.missing_nodes(graph_missing)
    assert result == {101, 102, 103}


@pytest.mark.parametrize(
    "direction,expected_root_ids",
    [
        ("incoming", (2,)),
        ("outgoing", (1,)),
    ],
)
def test_root_nodes_simple(graph_simple, direction, expected_root_ids):
    roots = gops.root_nodes(graph_simple, direction=direction)
    assert tuple(n.id_ for n in roots) == expected_root_ids


def test_root_nodes_tree(graph_tree):
    roots_in = gops.root_nodes(graph_tree, direction="incoming")
    assert tuple(n.id_ for n in roots_in) == (1,)
    roots_with_unbound = gops.root_nodes(graph_tree, direction="incoming", with_unbound_nodes=True)
    assert {n.id_ for n in roots_with_unbound} == {1, 6}


# ---------------------------------------------------------------------------
# Tree building
# ---------------------------------------------------------------------------


def test_create_tree_incoming(graph_simple):
    result = gops.create_tree(graph_simple, graph_simple.nodes[2], direction="incoming")
    assert len(result) == 1
    root_node, _root_edge, children = result[0]
    assert root_node == graph_simple.nodes[2]
    assert len(children) == 1
    child_node, edge, grand_children = children[0]
    assert child_node == graph_simple.nodes[1]
    assert edge == graph_simple.edges[1001]
    assert grand_children == ()


def test_create_tree_outgoing(graph_simple):
    result = gops.create_tree(graph_simple, graph_simple.nodes[1], direction="outgoing")
    assert len(result[0][2]) == 1  # one child from node 1
    child_node, edge, _gc = result[0][2][0]
    assert child_node == graph_simple.nodes[2]
    assert edge == graph_simple.edges[1001]


def test_create_tree_max_depth(graph_tree):
    # root = 1 (incoming) children are nodes 2 & 3 (depth=1), we cap at depth=1 so their children not expanded
    tree = gops.create_tree(graph_tree, graph_tree.nodes[1], direction="incoming", max_depth=1)
    root_children = tree[0][2]
    assert len(root_children) == 2
    # each child tuple has an empty children expansion placeholder (since depth limit reached)
    for child in root_children:
        # child = [node, edge, children]
        assert child[2] == () or all(isinstance(x, list) for x in child[2])


def test_build_tree(graph_tree):
    # build complete incoming tree including unbound nodes
    full = gops.build_tree(graph_tree, direction="incoming", with_unbound_nodes=True)
    # Should have two roots: 1 and 6
    root_ids = {r[0].id_ for r in full}
    assert root_ids == {1, 6}


# ---------------------------------------------------------------------------
# Regression / invariants
# ---------------------------------------------------------------------------


def test_graph_indexing_stability(graph_simple):
    # direct access by numeric ID returns the same object
    assert graph_simple.edges[1001] == graph_simple.edges[1001]
    assert graph_simple.nodes[1] == graph_simple.nodes[1]
    # retrieving a missing key returns None for direct access
    assert graph_simple.nodes.get(9999) is None
    assert graph_simple.edges.get(9999) is None
