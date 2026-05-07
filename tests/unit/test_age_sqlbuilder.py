"""Tests for AGE SQL builder -- focused on dollar-quote safety."""

from unittest.mock import MagicMock

from cypher_graphdb.backends.age.agesqlbuilder import SQLBuilder
from cypher_graphdb.cypherparser import ParsedCypherQuery


def _make_parsed(query: str) -> ParsedCypherQuery:
    """Build a minimal ParsedCypherQuery stub for SQL builder tests."""
    parsed = MagicMock(spec=ParsedCypherQuery)
    parsed.parsed_query = query
    parsed.return_arguments = {}
    return parsed


class TestCypherSqlTaggedDollarQuote:
    """Verify that create_cypher_sql uses a tagged dollar-quote delimiter.

    Apache AGE wraps the Cypher query in a dollar-quoted string when building
    the cypher() function call. Using bare $$ as the delimiter means any $$
    sequence inside a property value (e.g. a file path containing $$) will
    prematurely terminate the block and cause a PostgreSQL SyntaxError.

    The fix: use $age_cypher$...$age_cypher$ as the delimiter. The tag is
    chosen to be impossible in user data (source paths, class names, symbols).
    """

    def test_uses_tagged_delimiter_not_bare_double_dollar(self):
        parsed = _make_parsed("MATCH (n) RETURN n")
        sql_obj, _ = SQLBuilder.create_cypher_sql("mygraph", parsed)
        sql_str = sql_obj.as_string(None)
        assert "$$" not in sql_str, "bare $$ delimiter must not appear in generated SQL"
        assert "$age_cypher$" in sql_str

    def test_tagged_delimiter_wraps_cypher_query(self):
        parsed = _make_parsed("MATCH (n:Class) WHERE n.name = 'Foo' RETURN n")
        sql_obj, _ = SQLBuilder.create_cypher_sql("codegraph", parsed)
        sql_str = sql_obj.as_string(None)
        assert "$age_cypher$ MATCH (n:Class) WHERE n.name = 'Foo' RETURN n $age_cypher$" in sql_str

    def test_dollar_dollar_in_cypher_value_does_not_break_sql(self):
        # Simulate a Cypher query that contains $$ in an inline literal value
        # (as produced by bulk_create_nodes when a symbol contains $$).
        cypher_with_dollars = (
            'UNWIND [{symbol: "fld:pkg.Cls$$inner.field", name: "field"}] AS props CREATE (n:Field) SET n = props'
        )
        parsed = _make_parsed(cypher_with_dollars)
        sql_obj, _ = SQLBuilder.create_cypher_sql("codegraph", parsed)
        sql_str = sql_obj.as_string(None)
        # The $$ inside the Cypher literal is now safely enclosed within
        # $age_cypher$...$age_cypher$ which PostgreSQL will not confuse with
        # the inner $$ sequence.
        assert "$age_cypher$" in sql_str
        assert "fld:pkg.Cls$$inner.field" in sql_str

    def test_with_params_also_uses_tagged_delimiter(self):
        parsed = _make_parsed("MATCH (n) WHERE n.id = $id RETURN n")
        parsed.return_arguments = {"n": "agtype"}
        sql_obj, _ = SQLBuilder.create_cypher_sql("codegraph", parsed, params={"id": "123"})
        sql_str = sql_obj.as_string(None)
        assert "$$" not in sql_str
        assert "$age_cypher$" in sql_str
