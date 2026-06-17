"""Unit tests for CypherQuery integration with the CypherGraphDB facade.

Uses a capturing MockBackend (no Docker) to verify the facade coercion path:
the ``query()`` factory, auto-bound params reaching the backend, caller-param
merge semantics, and builder support in execute_cypher_page.
"""

import pytest

from cypher_graphdb import CypherGraphDB
from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.cypherquery import CypherQuery, param

from .mock_backend import MockBackend


class CapturingBackend(MockBackend):
    """MockBackend that records the cypher and params passed to execute_cypher."""

    def __init__(self):
        super().__init__()
        self.last_cypher = None
        self.last_params = None

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):  # noqa: D401
        self.last_cypher = cypher_query.submitted_query
        self.last_params = params
        return [], ExecStatistics()

    def get_capability(self, capability):  # noqa: D401
        """Declare no optional capabilities so paths use backend-agnostic fallbacks."""
        raise NotImplementedError(capability)


@pytest.fixture
def cdb():
    """Facade wired to a capturing backend."""
    return CypherGraphDB(CapturingBackend())


def test_query_factory_returns_builder(cdb):
    assert isinstance(cdb.query(), CypherQuery)


def test_execute_coerces_builder_and_binds_params(cdb):
    q = cdb.query().match("(p:Product)").where_eq("p.name", "Widget").return_("p").limit(5)
    cdb.execute(q)

    assert "WHERE p.name = $_p0" in cdb._backend.last_cypher
    assert "LIMIT $_p1" in cdb._backend.last_cypher
    assert cdb._backend.last_params == {"_p0": "Widget", "_p1": 5}


def test_execute_with_stats_coerces_builder(cdb):
    q = cdb.query().match("(n)").where_id("n", 7).return_("n")
    cdb.execute_with_stats(q)

    assert "id(n) = $_p0" in cdb._backend.last_cypher
    assert cdb._backend.last_params == {"_p0": 7}


def test_caller_params_merge_with_builder(cdb):
    q = (
        cdb.query()
        .match("(p:Product)")
        .where_eq("p.price", 150)
        .where("p.name = $expected", expected=param("expected"))
        .return_("p")
    )
    cdb.execute(q, params={"expected": "Gizmo"})

    assert cdb._backend.last_params == {"_p0": 150, "expected": "Gizmo"}


def test_caller_params_win_on_collision(cdb):
    q = cdb.query().match("(n)").where_eq("n.x", 1).return_("n")
    # Force a collision on the auto-name to exercise the override path.
    cdb.execute(q, params={"_p0": 999})

    assert cdb._backend.last_params == {"_p0": 999}


@pytest.mark.filterwarnings("ignore:Backend does not support native pagination")
def test_pagination_accepts_builder(cdb):
    q = cdb.query().match("(p:Product)").return_("p").order_by("p.price")
    cdb.execute_cypher_page(q, offset=0, limit=10)

    assert cdb._backend.last_cypher is not None
    assert "MATCH (p:Product)" in cdb._backend.last_cypher


def test_string_input_still_passes_through(cdb):
    cdb.execute("MATCH (n) RETURN n", params={"x": 1})
    assert cdb._backend.last_cypher == "MATCH (n) RETURN n"
    assert cdb._backend.last_params == {"x": 1}
