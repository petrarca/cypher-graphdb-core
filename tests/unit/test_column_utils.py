"""Tests for column name resolution utilities."""

from cypher_graphdb.utils import resolve_column_names


class TestResolveColumnNames:
    """Test the resolve_column_names function."""

    def test_no_wildcard_simple(self):
        """Test that non-wildcard queries return unchanged."""
        return_args = {"p.name": "p.name", "p.id": "p.id"}
        result = [["Alice", 1], ["Bob", 2]]
        col_count = 2

        resolved = resolve_column_names(return_args, result, col_count)

        assert resolved == {"p.name": "p.name", "p.id": "p.id"}

    def test_no_wildcard_single_column(self):
        """Test single column without wildcard."""
        return_args = {"name": "p.name"}
        result = [["Alice"], ["Bob"]]
        col_count = 1

        resolved = resolve_column_names(return_args, result, col_count)

        assert resolved == {"name": "p.name"}

    def test_wildcard_only_with_dict_rows(self):
        """Test RETURN * with dict-like rows (node/edge format)."""
        return_args = {"*": "*"}
        result = [
            [
                {"node": {"id_": 1, "label_": "Person", "properties_": {}}},
                {"edge": {"id_": 10, "label_": "KNOWS", "properties_": {}}},
                {"node": {"id_": 2, "label_": "Company", "properties_": {}}},
            ]
        ]
        col_count = 3

        resolved = resolve_column_names(return_args, result, col_count)

        # Should detect labels from the data
        assert "0" in resolved
        assert "1" in resolved
        assert "2" in resolved
        assert resolved["0"] == "Person"
        assert resolved["1"] == "KNOWS"
        assert resolved["2"] == "Company"

    def test_wildcard_only_with_simple_values(self):
        """Test RETURN * with simple value rows."""
        return_args = {"*": "*"}
        result = [["Alice", 25, True], ["Bob", 30, False]]
        col_count = 3

        resolved = resolve_column_names(return_args, result, col_count)

        # Should use generic names for scalar values
        assert "0" in resolved
        assert "1" in resolved
        assert "2" in resolved
        # Generic names for scalars
        assert resolved["0"] == "col_0"
        assert resolved["1"] == "col_1"
        assert resolved["2"] == "col_2"

    def test_wildcard_with_explicit_column(self):
        """Test RETURN *, p.name mixed case."""
        return_args = {"*": "*", "name": "p.name"}
        result = [
            [
                {"node": {"id_": 1, "label_": "Person", "properties_": {}}},
                {"edge": {"id_": 10, "label_": "KNOWS", "properties_": {}}},
                {"node": {"id_": 2, "label_": "Company", "properties_": {}}},
                "Alice",
            ]
        ]
        col_count = 4

        resolved = resolve_column_names(return_args, result, col_count)

        # First 3 from wildcard, last one explicit
        assert "0" in resolved
        assert "1" in resolved
        assert "2" in resolved
        assert "name" in resolved
        assert resolved["0"] == "Person"
        assert resolved["1"] == "KNOWS"
        assert resolved["2"] == "Company"
        assert resolved["name"] == "p.name"

    def test_wildcard_with_multiple_explicit_columns(self):
        """Test RETURN *, p.name, t.name with multiple explicit cols."""
        return_args = {
            "*": "*",
            "person_name": "p.name",
            "tech_name": "t.name",
        }
        result = [
            [
                {"node": {"id_": 1, "label_": "Person", "properties_": {}}},
                "Alice",
                "Python",
            ]
        ]
        col_count = 3

        resolved = resolve_column_names(return_args, result, col_count)

        # First from wildcard, rest explicit
        assert "0" in resolved
        assert "person_name" in resolved
        assert "tech_name" in resolved
        assert resolved["0"] == "Person"
        assert resolved["person_name"] == "p.name"
        assert resolved["tech_name"] == "t.name"

    def test_empty_result(self):
        """Test with empty result set."""
        return_args = {"p.name": "p.name"}
        result = []
        col_count = 1

        resolved = resolve_column_names(return_args, result, col_count)

        # No wildcard, return unchanged
        assert resolved == {"p.name": "p.name"}

    def test_wildcard_with_empty_result(self):
        """Test wildcard with empty result set."""
        return_args = {"*": "*"}
        result = []
        col_count = 3

        resolved = resolve_column_names(return_args, result, col_count)

        # Should use generic names when no data available
        assert "0" in resolved
        assert "1" in resolved
        assert "2" in resolved
        assert resolved["0"] == "col_0"
        assert resolved["1"] == "col_1"
        assert resolved["2"] == "col_2"

    def test_wildcard_with_dict_data(self):
        """Test wildcard with dict rows (object-like results)."""
        return_args = {"*": "*"}
        # When result is a list of dicts, each dict is treated as a row
        # and the keys become the column names
        result = [
            {"label_": "Person", "properties_": {"name": "Alice"}},
            {"label_": "Person", "properties_": {"name": "Bob"}},
        ]
        col_count = 2  # Two keys: label_, properties_

        resolved = resolve_column_names(return_args, result, col_count)

        # Should use dict keys as column names
        assert "0" in resolved
        assert "1" in resolved
        # First key is label_, second is properties_
        assert resolved["0"] == "label_"
        assert resolved["1"] == "properties_"

    def test_single_value_result(self):
        """Test with single scalar value results."""
        return_args = {"count": "count(p)"}
        result = [42]
        col_count = 1

        resolved = resolve_column_names(return_args, result, col_count)

        assert resolved == {"count": "count(p)"}

    def test_nested_list_result(self):
        """Test with result containing nested structures."""
        return_args = {"p": "p", "friends": "collect(f.name)"}
        result = [
            [
                {"node": {"id_": 1, "label_": "Person", "properties_": {}}},
                ["Alice", "Bob", "Charlie"],
            ]
        ]
        col_count = 2

        resolved = resolve_column_names(return_args, result, col_count)

        assert resolved == {"p": "p", "friends": "collect(f.name)"}

    def test_wildcard_only_no_explicit_cols(self):
        """Test pure wildcard case with no explicit columns after."""
        return_args = {"*": "*"}
        result = [
            [
                {"node": {"label_": "Product"}},
                {"edge": {"label_": "USES_TECHNOLOGY"}},
            ]
        ]
        col_count = 2

        resolved = resolve_column_names(return_args, result, col_count)

        assert len(resolved) == 2
        assert resolved["0"] == "Product"
        assert resolved["1"] == "USES_TECHNOLOGY"

    def test_col_count_mismatch_protection(self):
        """Test that function handles col_count mismatch gracefully."""
        return_args = {"*": "*", "extra": "p.id"}
        result = [["value1"]]
        col_count = 2  # Says 2 but data only has 1 + extra

        # Should not crash, should handle gracefully
        resolved = resolve_column_names(return_args, result, col_count)

        assert "extra" in resolved
        assert resolved["extra"] == "p.id"
