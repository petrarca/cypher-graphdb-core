"""Integration tests for the fluent CypherQuery builder against real backends.

Verifies that builder-produced queries execute correctly on Memgraph and Apache
AGE, that auto-bound parameters reach the backend, and that the builder works
through ``execute``, ``execute_with_stats``, and ``execute_cypher_page``.
"""

import pytest

from cypher_graphdb.cypherquery import param

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def db_with_products(request):
    """Create Product test data and clean up afterwards."""
    db = request.getfixturevalue(request.param)
    db.execute("MATCH (n) DETACH DELETE n")
    db.execute("""
        CREATE (p1:Product {key: 'prod-001', name: 'Widget', price: 100})
        CREATE (p2:Product {key: 'prod-002', name: 'Gadget', price: 200})
        CREATE (p3:Product {key: 'prod-003', name: 'Gizmo', price: 150})
    """)
    yield db
    db.execute("MATCH (n) DETACH DELETE n")


class TestBuilderExecute:
    """Builder queries executed via execute()."""

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_where_eq_string(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").where_eq("p.name", "Widget").return_("p.price")
        result = db_with_products.execute(q, unnest_result="rc")
        assert result == 100

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_where_in_list(self, db_with_products):
        q = (
            db_with_products.query()
            .match("(p:Product)")
            .where_in("p.name", ["Widget", "Gizmo"])
            .return_("p.name")
            .order_by("p.name")
        )
        result = db_with_products.execute(q)
        names = [row[0] for row in result]
        assert names == ["Gizmo", "Widget"]

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_where_raw_fragment_with_bind(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").where("p.price > $min", min=120).return_("p.name").order_by("p.price")
        result = db_with_products.execute(q)
        names = [row[0] for row in result]
        assert names == ["Gizmo", "Gadget"]

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_order_by_and_limit(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").return_("p.name").order_by("p.price DESC").limit(1)
        result = db_with_products.execute(q, unnest_result="rc")
        assert result == "Gadget"

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_return_distinct(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").return_distinct("labels(p)[0] AS label")
        result = db_with_products.execute(q)
        labels = [row[0] for row in result]
        assert labels == ["Product"]

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_named_param_helper(self, db_with_products):
        threshold = param("threshold", 150)
        q = (
            db_with_products.query()
            .match("(p:Product)")
            .where("p.price >= $threshold", threshold=threshold)
            .return_("p.name")
            .order_by("p.name")
        )
        result = db_with_products.execute(q)
        names = [row[0] for row in result]
        assert names == ["Gadget", "Gizmo"]


class TestBuilderWhereId:
    """where_id() against real backend ids."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_where_id_roundtrip(self, test_db):
        test_db.execute("MATCH (n) DETACH DELETE n")
        node_id = test_db.execute(
            "CREATE (p:Person {name: 'Alice'}) RETURN id(p)",
            unnest_result="rc",
        )

        q = test_db.query().match("(p)").where_id("p", node_id).return_("p.name")
        result = test_db.execute(q, unnest_result="rc")
        assert result == "Alice"
        test_db.execute("MATCH (n) DETACH DELETE n")


class TestBuilderWithStats:
    """Builder queries executed via execute_with_stats()."""

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_execute_with_stats(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").where_eq("p.key", "prod-002").return_("p.name", "p.price")
        result = db_with_products.execute_with_stats(q, unnest_result=True)
        assert result.data[0] == "Gadget"
        assert result.data[1] == 200
        assert result.exec_statistics is not None


class TestBuilderPagination:
    """Builder queries executed via execute_cypher_page()."""

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_paginated_builder_query(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").return_("p.name").order_by("p.price")
        page = db_with_products.execute_cypher_page(q, offset=0, limit=2)
        assert len(page.rows) == 2

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_paginated_builder_with_where(self, db_with_products):
        q = db_with_products.query().match("(p:Product)").where("p.price >= $min", min=100).return_("p.name").order_by("p.price")
        page = db_with_products.execute_cypher_page(q, offset=1, limit=1)
        assert len(page.rows) == 1


class TestBuilderCallerParamsMerge:
    """Caller-supplied params merge with builder-bound params."""

    @pytest.mark.parametrize("db_with_products", ["memgraph_db", "age_db"], indirect=True)
    def test_caller_param_alongside_builder(self, db_with_products):
        # Builder binds p.price via where_eq; caller supplies a named param used in a raw fragment.
        q = (
            db_with_products.query()
            .match("(p:Product)")
            .where_eq("p.price", 150)
            .where("p.name = $expected", expected=param("expected"))
            .return_("p.key")
        )
        result = db_with_products.execute(q, params={"expected": "Gizmo"}, unnest_result="rc")
        assert result == "prod-003"
