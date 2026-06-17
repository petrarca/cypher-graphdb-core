"""Unit tests for typed pattern objects and write clauses in CypherQuery."""

import pytest

from cypher_graphdb.cypherquery import CypherQuery, node, rel
from cypher_graphdb.cypherquery.patterns import Predicate, PropertyRef


def test_node_renders_labels_and_alias():
    q = CypherQuery().match(node("Person", alias="p")).return_("p")
    cypher, _ = q.build()
    assert "MATCH (p:Person)" in cypher


def test_node_inline_props_are_bound():
    q = CypherQuery().match(node("Person", alias="p", name="Alice")).return_("p")
    cypher, params = q.build()
    assert "name: $_p0" in cypher
    assert params == {"_p0": "Alice"}


def test_node_multiple_labels():
    q = CypherQuery().match(node("Person", "Admin", alias="p")).return_("p")
    cypher, _ = q.build()
    assert "(p:Person:Admin)" in cypher


def test_property_access_requires_alias():
    n = node("Person")
    with pytest.raises(ValueError, match="aliased node"):
        _ = n["age"]


def test_predicate_comparison_binds_value():
    p = node("Person", alias="p")
    q = CypherQuery().match(p).where(p["age"] > 30).return_("p")
    cypher, params = q.build()
    assert "p.age > $_p0" in cypher
    assert params == {"_p0": 30}


def test_predicate_and_or():
    p = node("Person", alias="p")
    pred = (p["age"] > 30) & (p["active"] == True)  # noqa: E712
    cypher, params = CypherQuery().match(p).where(pred).return_("p").build()
    assert "AND" in cypher
    assert set(params.values()) == {30, True}


def test_predicate_invert():
    p = node("Person", alias="p")
    cypher, _ = CypherQuery().match(p).where(~(p["age"] > 30)).return_("p").build()
    assert cypher.count("NOT") == 1


def test_property_in_list():
    p = node("Person", alias="p")
    cypher, params = CypherQuery().match(p).where(p["age"].in_([1, 2, 3])).return_("p").build()
    assert "p.age IN $_p0" in cypher
    assert params == {"_p0": [1, 2, 3]}


def test_property_is_null_and_starts_with():
    p = node("Person", alias="p")
    cypher, params = CypherQuery().match(p).where(p["deleted"].is_null()).where(p["name"].starts_with("Al")).return_("p").build()
    assert "p.deleted IS NULL" in cypher
    assert "p.name STARTS WITH $_p0" in cypher
    assert params == {"_p0": "Al"}


def test_property_compared_to_property_no_bind():
    p = node("Person", alias="p")
    f = node("Person", alias="f")
    cypher, params = CypherQuery().match(p).match(f).where(p["age"] == f["age"]).return_("p").build()
    assert "p.age = f.age" in cypher
    assert params == {}


def test_directed_path_to():
    p = node("Person", alias="p")
    f = node("Person", alias="f")
    k = rel("KNOWS", alias="k")
    cypher, _ = CypherQuery().match(p.to(k, f)).return_("p", "f").build()
    assert "(p:Person)-[k:KNOWS]->(f:Person)" in cypher


def test_minus_operator_path_uses_rel_direction():
    p = node("Person", alias="p")
    f = node("Person", alias="f")
    cypher, _ = CypherQuery().match(p - rel("KNOWS") - f).return_("p").build()
    assert "(p:Person)-[:KNOWS]->(f:Person)" in cypher


def test_undirected_rel():
    p = node(alias="p")
    f = node(alias="f")
    cypher, _ = CypherQuery().match(p.to(rel(direction="--"), f)).return_("p").build()
    assert "(p)-[]-(f)" in cypher


def test_multi_hop_path():
    a = node("A", alias="a")
    b = node("B", alias="b")
    c = node("C", alias="c")
    path = a.to(rel("R1"), b).to(rel("R2"), c)
    cypher, _ = CypherQuery().match(path).return_("a", "b", "c").build()
    assert "(a:A)-[:R1]->(b:B)-[:R2]->(c:C)" in cypher


def test_unwind_create_clause_order_preserved():
    q = CypherQuery().unwind("$rows", "row").create("(n:Node {id: row.id})")
    cypher, params = q.build()
    lines = cypher.splitlines()
    assert lines[0] == "UNWIND $rows AS row"
    assert lines[1].startswith("CREATE")
    assert params == {}  # $rows is caller-supplied, not builder-bound


def test_merge_with_on_create_and_on_match():
    q = CypherQuery().merge("(n:Node {k: 1})").on_create_set("n.created = true").on_match_set("n.seen = true").return_("n")
    cypher, _ = q.build()
    assert "MERGE (n:Node {k: 1})" in cypher
    assert "ON CREATE SET n.created = true" in cypher
    assert "ON MATCH SET n.seen = true" in cypher


def test_set_with_bind():
    q = CypherQuery().match("(n)").where_id("n", 1).set("n.name = $name", name="Bob")
    cypher, params = q.build()
    assert "SET n.name = $name" in cypher
    assert params["name"] == "Bob"


def test_detach_delete():
    q = CypherQuery().match("(n)").where_id("n", 5).delete("n", detach=True)
    cypher, _ = q.build()
    assert "DETACH DELETE n" in cypher


def test_remove_clause():
    q = CypherQuery().match("(n)").where_id("n", 5).remove("n.tmp", "n:Stale")
    cypher, _ = q.build()
    assert "REMOVE n.tmp, n:Stale" in cypher


def test_write_query_interleaves_correctly():
    q = (
        CypherQuery()
        .unwind("$rows", "row")
        .match("(c:Component {id: row.src})")
        .merge("(l:Language {name: row.dst})")
        .merge("(c)-[r:USES]->(l)")
        .set("r.pct = row.pct")
    )
    cypher, _ = q.build()
    lines = cypher.splitlines()
    assert [line.split()[0] for line in lines] == ["UNWIND", "MATCH", "MERGE", "MERGE", "SET"]


def test_pattern_objects_are_query_independent():
    # The same pattern object can be reused across queries; binding happens
    # at attach time, so each query gets its own $_pN allocation from 0.
    p = node("Person", alias="p", name="Alice")
    q1 = CypherQuery().match(p).return_("p")
    q2 = CypherQuery().match(p).where(p["age"] > 18).return_("p")
    assert q1.params == {"_p0": "Alice"}
    assert q2.params == {"_p0": "Alice", "_p1": 18}


def test_predicate_and_property_types():
    p = node("X", alias="p")
    assert isinstance(p["a"], PropertyRef)
    assert isinstance(p["a"] > 1, Predicate)
