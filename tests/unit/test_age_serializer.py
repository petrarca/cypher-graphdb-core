"""Tests for AGE inline Cypher literal serializer."""

import pytest

from cypher_graphdb.backends.age.ageserializer import escape_value, to_cypher_list


class TestEscapeValue:
    """Tests for escape_value function."""

    def test_none(self):
        assert escape_value(None) == "null"

    def test_bool_true(self):
        assert escape_value(True) == "true"

    def test_bool_false(self):
        assert escape_value(False) == "false"

    def test_int(self):
        assert escape_value(42) == "42"

    def test_negative_int(self):
        assert escape_value(-7) == "-7"

    def test_zero(self):
        assert escape_value(0) == "0"

    def test_float(self):
        assert escape_value(3.14) == "3.14"

    def test_simple_string(self):
        assert escape_value("hello") == '"hello"'

    def test_string_with_double_quotes(self):
        assert escape_value('say "hi"') == '"say \\"hi\\""'

    def test_string_with_backslash(self):
        assert escape_value("path\\to\\file") == '"path\\\\to\\\\file"'

    def test_string_with_newline(self):
        assert escape_value("line1\nline2") == '"line1\\nline2"'

    def test_string_with_carriage_return(self):
        assert escape_value("line1\rline2") == '"line1\\rline2"'

    def test_string_with_tab(self):
        assert escape_value("col1\tcol2") == '"col1\\tcol2"'

    def test_string_with_null_byte(self):
        # Null bytes are stripped
        assert escape_value("before\0after") == '"beforeafter"'

    def test_empty_string(self):
        assert escape_value("") == '""'

    def test_string_with_mixed_special_chars(self):
        result = escape_value('a"b\\c\nd\re\tf')
        assert result == '"a\\"b\\\\c\\nd\\re\\tf"'

    def test_bool_before_int(self):
        """Ensure bool is checked before int (bool is subclass of int in Python)."""
        # True == 1 in Python, but should serialize as "true" not "1"
        assert escape_value(True) == "true"
        assert escape_value(False) == "false"

    def test_non_string_coerced(self):
        """Non-primitive types are coerced to str."""
        result = escape_value(["a", "b"])
        assert result.startswith('"')
        assert result.endswith('"')


class TestToCypherList:
    """Tests for to_cypher_list function."""

    def test_single_row(self):
        result = to_cypher_list([{"name": "Foo", "line": 42}])
        assert result == '[{name: "Foo", line: 42}]'

    def test_multiple_rows(self):
        result = to_cypher_list([{"name": "Foo", "line": 42}, {"name": "Bar", "line": 99}])
        assert result == '[{name: "Foo", line: 42}, {name: "Bar", line: 99}]'

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="Cannot serialize empty list"):
            to_cypher_list([])

    def test_none_values(self):
        result = to_cypher_list([{"name": "Foo", "parent": None}])
        assert result == '[{name: "Foo", parent: null}]'

    def test_bool_values(self):
        result = to_cypher_list([{"name": "Foo", "is_static": True, "is_async": False}])
        assert result == '[{name: "Foo", is_static: true, is_async: false}]'

    def test_special_characters_in_values(self):
        result = to_cypher_list([{"name": 'say "hi"', "path": "a\\b"}])
        assert '"say \\"hi\\""' in result
        assert '"a\\\\b"' in result

    def test_newlines_in_values(self):
        result = to_cypher_list([{"doc": "line1\nline2"}])
        assert '"line1\\nline2"' in result

    def test_single_property(self):
        result = to_cypher_list([{"id": "mod_1"}])
        assert result == '[{id: "mod_1"}]'

    def test_numeric_values(self):
        result = to_cypher_list([{"start_line": 10, "end_line": 20, "score": 0.95}])
        assert result == "[{start_line: 10, end_line: 20, score: 0.95}]"

    def test_dollar_sign_in_value(self):
        # Single $ is safe -- only $$ terminates the dollar-quote block
        result = to_cypher_list([{"name": "price$tag"}])
        assert '"price$tag"' in result

    def test_double_dollar_in_value(self):
        # $$ in a value would terminate the AGE dollar-quote block if not escaped.
        # escape_value must not let $$ pass through unmodified inside a string literal.
        # The value is wrapped in double-quotes so $$ inside is already safe from the
        # dollar-quote perspective (the delimiter is outside the string). This test
        # documents the expected behaviour and guards against regression.
        result = escape_value("path$$name")
        assert result == '"path$$name"'
        # The serialized form is safe because the $$ is inside the double-quoted string,
        # not in the surrounding Cypher text. The outer $age_cypher$ tag in the SQL
        # builder (not $$) provides the protection against premature termination.
        serialized = to_cypher_list([{"symbol": "fld:pkg.Cls$$inner.field"}])
        assert '"fld:pkg.Cls$$inner.field"' in serialized
