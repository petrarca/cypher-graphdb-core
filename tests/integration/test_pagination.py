"""Integration tests for windowed pagination (execute_cypher_page).

Tests the cache-and-slice fallback against live Memgraph and AGE backends.
Both backends currently use the fallback (no native PAGINATION_SUPPORT yet);
these tests assert the contract the server/web layers depend on:

- Correct windowing (offset/limit) and ``returned`` count.
- Exact ``total`` and correct ``has_more`` transitions across pages.
- **Stable order across pages** (no gaps, no duplicates) when the query has a
  deterministic ORDER BY.
- ``params`` binding flows through.
- ``col_names`` resolution.

When a backend later declares ``PAGINATION_SUPPORT``, these same assertions
must continue to hold for the native path.
"""

import warnings

import pytest

from cypher_graphdb import BackendCapability, Page

pytestmark = pytest.mark.integration

ORDERED_QUERY = "MATCH (p:Person) RETURN p.name ORDER BY p.name"


def _seed_people(db, count: int):
    db.execute("MATCH (n) DETACH DELETE n")
    for i in range(count):
        db.execute(f"CREATE (p:Person {{name: 'P{i:03d}', age: {20 + i}}})")


def _page(db, query, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # ignore fallback warning
        return db.execute_cypher_page(query, **kwargs)


class _PaginationContract:
    """Shared assertions run against each backend fixture."""

    def _db(self, request):
        raise NotImplementedError

    def test_returns_page_instance(self, db):
        _seed_people(db, 5)
        page = _page(db, ORDERED_QUERY, offset=0, limit=10)
        assert isinstance(page, Page)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_first_page_window_and_total(self, db):
        _seed_people(db, 25)
        page = _page(db, ORDERED_QUERY, offset=0, limit=10)
        assert page.returned == 10
        assert page.total == 25
        assert page.has_more is True
        assert page.rows[0] == ("P000",)
        assert page.rows[-1] == ("P009",)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_last_partial_page(self, db):
        _seed_people(db, 25)
        page = _page(db, ORDERED_QUERY, offset=20, limit=10)
        assert page.returned == 5
        assert page.has_more is False
        assert page.rows[0] == ("P020",)
        assert page.rows[-1] == ("P024",)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_stable_order_across_pages(self, db):
        _seed_people(db, 25)
        p0 = _page(db, ORDERED_QUERY, offset=0, limit=10)
        p1 = _page(db, ORDERED_QUERY, offset=10, limit=10)
        p2 = _page(db, ORDERED_QUERY, offset=20, limit=10)
        names = [r[0] for r in (p0.rows + p1.rows + p2.rows)]
        assert names == sorted(names)  # fully ordered
        assert len(set(names)) == 25  # no duplicates across pages
        db.execute("MATCH (n) DETACH DELETE n")

    def test_offset_past_end_is_empty(self, db):
        _seed_people(db, 5)
        page = _page(db, ORDERED_QUERY, offset=100, limit=10)
        assert page.returned == 0
        assert page.is_empty()
        assert page.has_more is False
        assert page.total == 5
        db.execute("MATCH (n) DETACH DELETE n")

    def test_params_binding(self, db):
        _seed_people(db, 25)
        page = _page(
            db,
            "MATCH (p:Person) WHERE p.age >= $minage RETURN p.name ORDER BY p.name",
            offset=0,
            limit=5,
            params={"minage": 40},
        )
        # ages 40..44 -> P020..P024
        assert page.total == 5
        assert [r[0] for r in page.rows] == ["P020", "P021", "P022", "P023", "P024"]
        db.execute("MATCH (n) DETACH DELETE n")

    def test_col_names_resolved(self, db):
        _seed_people(db, 3)
        page = _page(db, ORDERED_QUERY, offset=0, limit=10)
        assert page.col_names is not None
        assert "p.name" in page.col_names
        db.execute("MATCH (n) DETACH DELETE n")

    def test_empty_result(self, db):
        db.execute("MATCH (n) DETACH DELETE n")
        page = _page(db, "MATCH (p:Person {name: 'Nope'}) RETURN p.name", offset=0, limit=10)
        assert page.returned == 0
        assert page.total == 0
        assert page.has_more is False


class TestMemgraphPagination(_PaginationContract):
    """Pagination contract against live Memgraph (fallback path)."""

    @pytest.fixture
    def db(self, memgraph_db):
        # Memgraph still uses the cache-and-slice fallback (no native pagination yet).
        assert memgraph_db._backend.has_capability(BackendCapability.PAGINATION_SUPPORT) is False
        yield memgraph_db
        memgraph_db.execute("MATCH (n) DETACH DELETE n")


class TestAGEPagination(_PaginationContract):
    """Pagination contract against live Apache AGE (native windowing path)."""

    @pytest.fixture
    def db(self, age_db):
        # AGE supports native outer-SQL OFFSET/LIMIT windowing.
        assert age_db._backend.has_capability(BackendCapability.PAGINATION_SUPPORT) is True
        yield age_db
        age_db.execute("MATCH (n) DETACH DELETE n")

    def test_safe_query_uses_native_path(self, db):
        """A safe query must NOT emit the cache-and-slice fallback warning."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            db.execute_cypher_page(ORDERED_QUERY, offset=0, limit=5)
        assert not any("native pagination" in str(w.message) for w in caught)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_unsafe_query_falls_back(self, db):
        """RETURN * (unsafe to wrap) must fall back to cache-and-slice."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            page = db.execute_cypher_page("MATCH (p:Person) RETURN *", offset=0, limit=5)
        assert any("native pagination" in str(w.message) for w in caught)
        assert page.returned == 5
        db.execute("MATCH (n) DETACH DELETE n")
