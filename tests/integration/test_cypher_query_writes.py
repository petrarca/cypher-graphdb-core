"""Integration tests for fluent CypherQuery write clauses and typed patterns.

Exercises CREATE / MERGE / SET / DELETE and typed pattern objects against real
Memgraph and Apache AGE backends.
"""

import pytest

from cypher_graphdb.cypherquery import CypherQuery, node, rel

pytestmark = pytest.mark.integration


@pytest.fixture
def clean_db(request):
    """Provide a database wiped clean before and after the test."""
    db = request.getfixturevalue(request.param)
    db.execute("MATCH (n) DETACH DELETE n")
    yield db
    db.execute("MATCH (n) DETACH DELETE n")


class TestBuilderCreateMergeSet:
    """CREATE / MERGE / SET via the builder against real backends."""

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_create_and_read_back(self, clean_db):
        clean_db.execute(CypherQuery().create("(p:Product {name: $name, price: $price})", name="Widget", price=100))
        result = clean_db.execute(
            CypherQuery().match("(p:Product)").where_eq("p.name", "Widget").return_("p.price"),
            unnest_result="rc",
        )
        assert result == 100

    # AGE's Cypher subset does not support ON CREATE SET / ON MATCH SET, so this
    # is gated to Memgraph. The builder emits standard Cypher; backend-specific
    # dialect handling is a separate concern (not yet implemented).
    @pytest.mark.parametrize("clean_db", ["memgraph_db"], indirect=True)
    def test_merge_with_on_create_set(self, clean_db):
        q = (
            CypherQuery()
            .merge("(p:Product {key: $key})", key="prod-1")
            .on_create_set("p.name = $name", name="Gadget")
            .return_("p.name")
        )
        result = clean_db.execute(q, unnest_result="rc")
        assert result == "Gadget"

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_set_updates_property(self, clean_db):
        clean_db.execute("CREATE (p:Product {key: 'k1', price: 100})")
        clean_db.execute(CypherQuery().match("(p:Product)").where_eq("p.key", "k1").set("p.price = $price", price=250))
        result = clean_db.execute("MATCH (p:Product {key: 'k1'}) RETURN p.price", unnest_result="rc")
        assert result == 250

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_detach_delete(self, clean_db):
        node_id = clean_db.execute("CREATE (p:Person {name: 'Temp'}) RETURN id(p)", unnest_result="rc")
        clean_db.execute(CypherQuery().match("(p)").where_id("p", node_id).delete("p", detach=True))
        remaining = clean_db.execute("MATCH (p:Person) RETURN count(p)", unnest_result="rc")
        assert remaining == 0


class TestBuilderTypedPatterns:
    """Typed node/rel patterns against real backends."""

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_typed_pattern_match_with_predicate(self, clean_db):
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice', age: 30})
            CREATE (b:Person {name: 'Bob', age: 25})
        """)
        p = node("Person", alias="p")
        q = CypherQuery().match(p).where(p["age"] > 28).return_("p.name")
        result = clean_db.execute(q, unnest_result="rc")
        assert result == "Alice"

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_typed_relationship_pattern(self, clean_db):
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice'})
            CREATE (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        a = node("Person", alias="a")
        b = node("Person", alias="b")
        q = CypherQuery().match(a.to(rel("KNOWS"), b)).where(a["name"] == "Alice").return_("b.name")
        result = clean_db.execute(q, unnest_result="rc")
        assert result == "Bob"

    @pytest.mark.parametrize("clean_db", ["memgraph_db", "age_db"], indirect=True)
    def test_typed_in_predicate(self, clean_db):
        clean_db.execute("""
            CREATE (a:Person {name: 'Alice'})
            CREATE (b:Person {name: 'Bob'})
            CREATE (c:Person {name: 'Carol'})
        """)
        p = node("Person", alias="p")
        q = CypherQuery().match(p).where(p["name"].in_(["Alice", "Carol"])).return_("p.name").order_by("p.name")
        result = clean_db.execute(q)
        assert [row[0] for row in result] == ["Alice", "Carol"]
