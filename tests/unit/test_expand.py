"""Unit tests for the node-expansion collection logic (no DB).

The DB-facing path (``expand_node`` issuing queries) is covered by the
integration suite against both Memgraph and AGE. Here we test the pure
dedup / unique-node-cap / has_more semantics in isolation.
"""

from dataclasses import dataclass

from cypher_graphdb.cyphergraphdb.expand import ExpandMixin


@dataclass
class _Obj:
    """Stand-in for a GraphNode/GraphEdge -- only ``id_`` is read by the collector."""

    id_: int


def _rows(pairs):
    """Build (node, edge) rows from (node_id, edge_id) pairs."""
    return [(_Obj(n), _Obj(e)) for n, e in pairs]


def test_collect_deduplicates_nodes_and_edges():
    rows = _rows([(1, 100), (1, 100), (2, 101)])
    nodes, edges, has_more = ExpandMixin._collect_expand_results(rows, limit=10)
    assert [n.id_ for n in nodes] == [1, 2]
    assert [e.id_ for e in edges] == [100, 101]
    assert has_more is False


def test_collect_respects_unique_node_cap_and_sets_has_more():
    rows = _rows([(1, 100), (2, 101), (3, 102)])
    nodes, edges, has_more = ExpandMixin._collect_expand_results(rows, limit=2)
    assert [n.id_ for n in nodes] == [1, 2]
    assert has_more is True


def test_collect_keeps_edges_to_accepted_nodes_but_drops_dangling_after_cap():
    # cap=1 -> only node 1 is accepted. Edge 101 points to node 2 (rejected by
    # the cap) so it would dangle and is dropped. Edge 102 points back to the
    # already-accepted node 1, so it is kept (this matters for direction="both",
    # where a second row can reference an already-accepted neighbor).
    rows = _rows([(1, 100), (2, 101), (1, 102)])
    nodes, edges, has_more = ExpandMixin._collect_expand_results(rows, limit=1)
    assert [n.id_ for n in nodes] == [1]
    assert [e.id_ for e in edges] == [100, 102]
    assert has_more is True


def test_collect_empty_rows():
    nodes, edges, has_more = ExpandMixin._collect_expand_results([], limit=10)
    assert nodes == []
    assert edges == []
    assert has_more is False
