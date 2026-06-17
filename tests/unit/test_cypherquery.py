"""Unit tests for the fluent CypherQuery builder."""

import pytest

from cypher_graphdb.cypherparser import ParsedCypherQuery
from cypher_graphdb.cypherquery import CypherQuery, param
from cypher_graphdb.exceptions import CypherQueryError


def test_where_id_binds_parameter():
    q = CypherQuery().match("(n)").where_id("n", 42).return_("n")
    cypher, params = q.build()
    assert "id(n) = $" in cypher
    assert 42 in params.values()
    assert "42" not in cypher  # value is bound, not interpolated


def test_where_eq_binds_value():
    q = CypherQuery().match("(n)").where_eq("n.name", "Alice").return_("n")
    cypher, params = q.build()
    assert "n.name = $" in cypher
    assert "Alice" in params.values()
    assert "Alice" not in cypher


def test_where_in_no_manual_escaping():
    q = CypherQuery().match("(n)-[e]-()").where_in("type(e)", ["A", "B'C"]).return_("e")
    cypher, params = q.build()
    assert "IN $" in cypher
    assert ["A", "B'C"] in params.values()


def test_multiple_where_join_with_and():
    q = CypherQuery().match("(n)").where_eq("n.a", 1).where_eq("n.b", 2).return_("n")
    cypher, _ = q.build()
    assert cypher.count("WHERE") == 1
    assert " AND " in cypher


def test_multiple_match_clauses_accumulate():
    q = CypherQuery().match("(a)").match("(b)").return_("a", "b")
    cypher, _ = q.build()
    assert cypher.count("MATCH") == 2


def test_optional_match():
    q = CypherQuery().match("(a)").optional_match("(a)-[r]->(b)").return_("a", "b")
    cypher, _ = q.build()
    assert "OPTIONAL MATCH (a)-[r]->(b)" in cypher


def test_clause_order_is_canonical():
    q = CypherQuery().match("(n)").where_eq("n.x", 1).return_("n").order_by("n.y").limit(10)
    cypher, _ = q.build()
    assert (
        cypher.index("MATCH") < cypher.index("WHERE") < cypher.index("RETURN") < cypher.index("ORDER BY") < cypher.index("LIMIT")
    )


def test_return_distinct():
    q = CypherQuery().match("(n)-[e]-()").where_id("n", 1).return_distinct("type(e) AS t")
    cypher, _ = q.build()
    assert "RETURN DISTINCT type(e) AS t" in cypher


def test_with_clause():
    q = CypherQuery().match("(n)").with_("n", "count(*) AS c").return_("n", "c")
    cypher, _ = q.build()
    assert "WITH n, count(*) AS c" in cypher


def test_skip_and_limit_bound():
    q = CypherQuery().match("(n)").return_("n").skip(5).limit(10)
    cypher, params = q.build()
    assert "SKIP $" in cypher
    assert "LIMIT $" in cypher
    assert 5 in params.values()
    assert 10 in params.values()


def test_immutability_base_query_unchanged():
    base = CypherQuery().match("(p)").where_eq("p.active", True)
    q1 = base.return_("p").limit(10)
    q2 = base.return_("count(p)")
    assert "LIMIT" not in base.build()[0]
    assert "LIMIT" in q1.build()[0]
    assert "count" in q2.build()[0]


def test_str_shows_placeholders_not_values():
    q = CypherQuery().match("(n)").where_eq("n.x", 99).return_("n")
    assert "99" not in str(q)
    assert "$" in str(q)


def test_cypher_property_matches_str():
    q = CypherQuery().match("(n)").return_("n")
    assert q.cypher == str(q)


def test_literal_binds_inlines_values():
    q = CypherQuery().match("(n)").where_eq("n.x", 99).return_("n")
    debug = q.build(literal_binds=True)
    assert isinstance(debug, str)
    assert "99" in debug
    assert "$" not in debug


def test_literal_binds_escapes_strings():
    q = CypherQuery().match("(n)").where_eq("n.name", 'a"b').return_("n")
    debug = q.build(literal_binds=True)
    assert '"a\\"b"' in debug


def test_literal_binds_renders_list():
    q = CypherQuery().match("(n)").where_in("n.tag", ["x", "y"]).return_("n")
    debug = q.build(literal_binds=True)
    assert '["x", "y"]' in debug


def test_where_raw_with_named_binds():
    q = CypherQuery().match("(p)").where("p.name STARTS WITH $pfx", pfx="Al").return_("p")
    cypher, params = q.build()
    assert "$pfx" in cypher
    assert params["pfx"] == "Al"


def test_param_helper_bound_value_in_params():
    p = param("threshold", 100)
    q = CypherQuery().match("(n)").where("n.v > $threshold", threshold=p).return_("n")
    _, params = q.build()
    assert params["threshold"] == 100


def test_param_helper_named_and_deferred():
    p = param("threshold")  # no value -- deferred to execute time
    q = CypherQuery().match("(n)").where("n.v > $threshold", threshold=p).return_("n")
    cypher, params = q.build()
    assert "$threshold" in cypher
    assert "threshold" not in params


def test_param_helper_rejects_reserved_prefix():
    with pytest.raises(CypherQueryError):
        param("_p0", 1)


def test_bind_keyword_must_match_param_name():
    p = param("real_name", 1)
    with pytest.raises(CypherQueryError):
        CypherQuery().match("(n)").where("n.x = $real_name", wrong_key=p)


def test_raw_fragment_appended():
    q = CypherQuery().match("(n)").return_("n").raw("UNION MATCH (m) RETURN m")
    cypher, _ = q.build()
    assert cypher.strip().endswith("UNION MATCH (m) RETURN m")


def test_build_empty_query_raises():
    with pytest.raises(CypherQueryError):
        CypherQuery().build()


def test_build_without_match_is_allowed_for_writes():
    # A write query legitimately has no MATCH (e.g. CREATE ... RETURN).
    q = CypherQuery().create("(n:Node {x: 1})").return_("n")
    cypher, _ = q.build()
    assert cypher.startswith("CREATE")
    assert "RETURN n" in cypher


def test_to_parsed_roundtrips_through_parser():
    q = CypherQuery().match("(n)").return_("n")
    parsed = q.to_parsed()
    assert isinstance(parsed, ParsedCypherQuery)
    assert parsed.submitted_query == str(q)


def test_auto_param_names_use_reserved_prefix():
    q = CypherQuery().match("(n)").where_id("n", 1).where_eq("n.x", 2).return_("n")
    _, params = q.build()
    assert set(params.keys()) == {"_p0", "_p1"}


def test_params_property_is_a_copy():
    q = CypherQuery().match("(n)").where_eq("n.x", 1).return_("n")
    p = q.params
    p["injected"] = 99
    assert "injected" not in q.params
