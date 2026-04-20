"""Unit tests for bulk write input normalization.

Covers both node and edge normalizers: dict path, typed path, mixed input
rejection, empty lists, label derivation, and all error paths.
"""

import pytest

from cypher_graphdb import GraphEdge, GraphNode, edge, node
from cypher_graphdb.cyphergraphdb.bulk_normalize import normalize_edges_input, normalize_nodes_input


@node()
class _TestPerson(GraphNode):
    """Unit-test fixture node class."""

    id: str
    name: str
    age: int = 0


@node(label="CustomLabel")
class _TestTagged(GraphNode):
    """Fixture node with an explicit label override."""

    id: str


@edge(label="KNOWS")
class _TestKnows(GraphEdge):
    """Fixture edge with extra property."""

    since: int = 0


@edge(label="FOLLOWS")
class _TestFollows(GraphEdge):
    """Fixture edge with no extra properties."""

    pass


# ── normalize_nodes_input ─────────────────────────────────────────────────────


class TestNormalizeNodesInput:
    """Tests for bulk_create_nodes normalizer."""

    def test_dict_path_returns_label_and_rows(self):
        rows = [{"id": "x"}, {"id": "y"}]
        label, result = normalize_nodes_input(rows, "Foo")
        assert label == "Foo"
        assert result == rows
        assert result is not rows  # defensive copy

    def test_dict_path_missing_label_raises(self):
        with pytest.raises(ValueError, match="'label' is required"):
            normalize_nodes_input([{"id": "x"}], None)

    def test_typed_path_derives_label(self):
        persons = [_TestPerson(id="p1", name="a"), _TestPerson(id="p2", name="b")]
        label, result = normalize_nodes_input(persons, None)
        assert label == "_TestPerson"
        assert result == [
            {"id": "p1", "name": "a", "age": 0},
            {"id": "p2", "name": "b", "age": 0},
        ]

    def test_typed_path_custom_label(self):
        tagged = [_TestTagged(id="a"), _TestTagged(id="b")]
        label, result = normalize_nodes_input(tagged, None)
        assert label == "CustomLabel"
        assert result == [{"id": "a"}, {"id": "b"}]

    def test_typed_path_explicit_label_match_ok(self):
        persons = [_TestPerson(id="p1", name="a")]
        label, result = normalize_nodes_input(persons, "_TestPerson")
        assert label == "_TestPerson"
        assert len(result) == 1

    def test_typed_path_explicit_label_mismatch_raises(self):
        persons = [_TestPerson(id="p1", name="a")]
        with pytest.raises(ValueError, match="does not match instances' label"):
            normalize_nodes_input(persons, "Wrong")

    def test_mixed_types_raises(self):
        rows = [_TestPerson(id="p1", name="a"), {"id": "p2"}]
        with pytest.raises(TypeError, match="cannot mix types"):
            normalize_nodes_input(rows, None)

    def test_non_homogeneous_labels_raises(self):
        rows = [_TestPerson(id="p1", name="a"), _TestTagged(id="t1")]
        with pytest.raises(ValueError, match="must share the same label"):
            normalize_nodes_input(rows, None)

    def test_invalid_row_type_raises(self):
        with pytest.raises(TypeError, match="list\\[dict\\] or list\\[GraphNode\\]"):
            normalize_nodes_input([42], None)

    def test_empty_with_label(self):
        label, result = normalize_nodes_input([], "Foo")
        assert label == "Foo"
        assert result == []

    def test_empty_without_label_raises(self):
        with pytest.raises(ValueError, match="'label' is required when 'rows' is empty"):
            normalize_nodes_input([], None)


# ── normalize_edges_input ─────────────────────────────────────────────────────


class TestNormalizeEdgesInput:
    """Tests for bulk_create_edges normalizer."""

    def test_dict_path_basic(self):
        edges = [{"src": "a", "dst": "b", "since": 2020}]
        label, result = normalize_edges_input(edges, None, None, "KNOWS")
        assert label == "KNOWS"
        assert result == edges

    def test_dict_path_missing_label_raises(self):
        with pytest.raises(ValueError, match="'label' is required when 'edges' are dicts"):
            normalize_edges_input([{"src": "a", "dst": "b"}], None, None, None)

    def test_dict_path_missing_src_dst_raises(self):
        with pytest.raises(ValueError, match="missing required 'src'/'dst'"):
            normalize_edges_input([{"src": "a"}], None, None, "KNOWS")

    def test_dict_path_with_refs_raises(self):
        with pytest.raises(ValueError, match="must be None when 'edges' are dicts"):
            normalize_edges_input([{"src": "a", "dst": "b"}], ["x"], None, "KNOWS")

    def test_typed_path_derives_label_and_builds_dicts(self):
        edges = [_TestKnows(since=2020), _TestKnows(since=2021)]
        label, result = normalize_edges_input(edges, ["a", "b"], ["c", "d"], None)
        assert label == "KNOWS"
        assert result == [
            {"src": "a", "dst": "c", "since": 2020},
            {"src": "b", "dst": "d", "since": 2021},
        ]

    def test_typed_path_no_extra_properties(self):
        edges = [_TestFollows(), _TestFollows()]
        label, result = normalize_edges_input(edges, ["a", "b"], ["c", "d"], None)
        assert label == "FOLLOWS"
        assert result == [{"src": "a", "dst": "c"}, {"src": "b", "dst": "d"}]

    def test_typed_path_missing_refs_raises(self):
        with pytest.raises(ValueError, match="'src_refs' and 'dst_refs' are required"):
            normalize_edges_input([_TestKnows()], None, None, None)

    def test_typed_path_length_mismatch_raises(self):
        edges = [_TestKnows(), _TestKnows()]
        with pytest.raises(ValueError, match="parallel lists must have equal length"):
            normalize_edges_input(edges, ["a"], ["c", "d"], None)

    def test_typed_path_explicit_label_mismatch_raises(self):
        with pytest.raises(ValueError, match="does not match instances' label"):
            normalize_edges_input([_TestKnows()], ["a"], ["b"], "WRONG")

    def test_mixed_types_raises(self):
        # First element is typed -> typed path is chosen. Provide matching-length refs so
        # the length check passes and the per-element type validation fires.
        edges = [_TestKnows(), {"src": "a", "dst": "b"}]
        with pytest.raises(TypeError, match="cannot mix types"):
            normalize_edges_input(edges, ["a", "b"], ["c", "d"], None)

    def test_non_homogeneous_labels_raises(self):
        edges = [_TestKnows(), _TestFollows()]
        with pytest.raises(ValueError, match="must share the same label"):
            normalize_edges_input(edges, ["a", "b"], ["c", "d"], None)

    def test_invalid_edge_type_raises(self):
        with pytest.raises(TypeError, match="list\\[dict\\] or list\\[GraphEdge\\]"):
            normalize_edges_input([42], None, None, None)

    def test_empty_with_label(self):
        label, result = normalize_edges_input([], None, None, "KNOWS")
        assert label == "KNOWS"
        assert result == []

    def test_empty_without_label_raises(self):
        with pytest.raises(ValueError, match="'label' is required when 'edges' is empty"):
            normalize_edges_input([], None, None, None)
