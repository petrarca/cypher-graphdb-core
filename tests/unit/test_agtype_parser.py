"""Tests for the JSON-based agtype parser (replaces ANTLR parser)."""

from age.models import Edge, Path, Vertex

from cypher_graphdb.backends.age.agtype_parser import parse_agtype


class TestScalars:
    """Scalar value parsing."""

    def test_integer(self):
        assert parse_agtype("42") == 42

    def test_negative_integer(self):
        assert parse_agtype("-7") == -7

    def test_zero(self):
        assert parse_agtype("0") == 0

    def test_float(self):
        assert abs(parse_agtype("3.14") - 3.14) < 0.001

    def test_bool_true(self):
        assert parse_agtype("true") is True

    def test_bool_false(self):
        assert parse_agtype("false") is False

    def test_null(self):
        assert parse_agtype("null") is None

    def test_empty_string(self):
        assert parse_agtype("") is None

    def test_simple_string(self):
        assert parse_agtype('"hello"') == "hello"

    def test_empty_string_value(self):
        assert parse_agtype('""') == ""

    def test_string_with_double_quotes(self):
        """The bug that the ANTLR parser gets wrong: \" must be unescaped."""
        assert parse_agtype('"say \\"hello\\""') == 'say "hello"'

    def test_string_with_backslash(self):
        assert parse_agtype('"path\\\\to\\\\file"') == "path\\to\\file"

    def test_string_with_newline(self):
        assert parse_agtype('"line1\\nline2"') == "line1\nline2"

    def test_string_with_tab(self):
        assert parse_agtype('"col1\\tcol2"') == "col1\tcol2"

    def test_numeric_annotation(self):
        """::numeric suffix should be stripped, value parsed as number."""
        assert parse_agtype("123.456::numeric") == 123.456


class TestCollections:
    """Array and map parsing."""

    def test_array(self):
        assert parse_agtype("[1, 2, 3]") == [1, 2, 3]

    def test_empty_array(self):
        assert parse_agtype("[]") == []

    def test_nested_array(self):
        assert parse_agtype("[[1, 2], [3]]") == [[1, 2], [3]]

    def test_map(self):
        assert parse_agtype('{"a": 1, "b": "two"}') == {"a": 1, "b": "two"}

    def test_empty_map(self):
        assert parse_agtype("{}") == {}

    def test_map_with_escaped_string(self):
        result = parse_agtype('{"val": "say \\"hello\\""}')
        assert result == {"val": 'say "hello"'}


class TestVertex:
    """Vertex (::vertex) parsing."""

    def test_basic_vertex(self):
        raw = '{"id": 123, "label": "Person", "properties": {"name": "Alice", "age": 30}}::vertex'
        v = parse_agtype(raw)
        assert isinstance(v, Vertex)
        assert v.id == 123
        assert v.label == "Person"
        assert v.properties == {"name": "Alice", "age": 30}

    def test_vertex_with_escaped_string(self):
        raw = '{"id": 1, "label": "Node", "properties": {"val": "say \\"hello\\""}}::vertex'
        v = parse_agtype(raw)
        assert isinstance(v, Vertex)
        assert v.properties["val"] == 'say "hello"'

    def test_vertex_empty_properties(self):
        raw = '{"id": 1, "label": "Empty", "properties": {}}::vertex'
        v = parse_agtype(raw)
        assert isinstance(v, Vertex)
        assert v.properties == {}


class TestEdge:
    """Edge (::edge) parsing."""

    def test_basic_edge(self):
        raw = '{"id": 456, "label": "KNOWS", "end_id": 2, "start_id": 1, "properties": {"since": 2020}}::edge'
        e = parse_agtype(raw)
        assert isinstance(e, Edge)
        assert e.id == 456
        assert e.label == "KNOWS"
        assert e.start_id == 1
        assert e.end_id == 2
        assert e.properties == {"since": 2020}

    def test_edge_empty_properties(self):
        raw = '{"id": 1, "label": "REL", "end_id": 3, "start_id": 2, "properties": {}}::edge'
        e = parse_agtype(raw)
        assert isinstance(e, Edge)
        assert e.properties == {}

    def test_edge_with_float_property(self):
        raw = '{"id": 1, "label": "CALLS", "end_id": 3, "start_id": 2, "properties": {"weight": 0.95}}::edge'
        e = parse_agtype(raw)
        assert abs(e.properties["weight"] - 0.95) < 0.001


class TestPath:
    """Path (::path) parsing."""

    def test_simple_path(self):
        raw = (
            '[{"id": 1, "label": "A", "properties": {"name": "a"}}::vertex, '
            '{"id": 10, "label": "R", "end_id": 2, "start_id": 1, "properties": {}}::edge, '
            '{"id": 2, "label": "B", "properties": {"name": "b"}}::vertex]::path'
        )
        p = parse_agtype(raw)
        assert isinstance(p, Path)
        assert len(p) == 3
        assert isinstance(p[0], Vertex)
        assert isinstance(p[1], Edge)
        assert isinstance(p[2], Vertex)
        assert p[0].properties["name"] == "a"
        assert p[1].label == "R"
        assert p[2].properties["name"] == "b"

    def test_path_preserves_edge_direction(self):
        raw = (
            '[{"id": 1, "label": "N", "properties": {}}::vertex, '
            '{"id": 10, "label": "E", "end_id": 2, "start_id": 1, "properties": {}}::edge, '
            '{"id": 2, "label": "N", "properties": {}}::vertex]::path'
        )
        p = parse_agtype(raw)
        assert p[1].start_id == 1
        assert p[1].end_id == 2
