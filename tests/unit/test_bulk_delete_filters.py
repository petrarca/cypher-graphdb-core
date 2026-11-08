"""Unit tests: the property filters of the bulk deletes -- one value or a list, validated, never interpolated."""

import pytest

from cypher_graphdb.backend import cypher_filter, filter_values


@pytest.mark.unit
class TestBulkDeleteFilters:
    def test_one_value_is_equality_and_a_list_is_in(self):
        conditions, params = cypher_filter({"source_key": "s", "filepath": ["a/b.py", "c d.cs"]})
        assert conditions == "n.source_key = $filter_0 AND n.filepath IN $filter_1"
        assert params == {"filter_0": "s", "filter_1": ["a/b.py", "c d.cs"]}

    def test_values_are_parameters_not_query_text(self):
        conditions, params = cypher_filter({"filepath": "x' OR 1=1 //"})
        assert "OR" not in conditions and params == {"filter_0": "x' OR 1=1 //"}

    def test_a_tuple_counts_as_a_list(self):
        assert filter_values("k", ("a", "b")) == ["a", "b"]

    @pytest.mark.parametrize(
        ("key", "value", "message"),
        [("bad key", "v", "filter key"), ("k", [], "empty list"), ("k", ["a", 1], "must be strings")],
    )
    def test_invalid_filters_raise(self, key, value, message):
        with pytest.raises(ValueError, match=message):
            filter_values(key, value)
