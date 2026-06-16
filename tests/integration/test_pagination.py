"""Integration tests for windowed pagination (execute_cypher_page).

Covers both backends with their native pagination paths:
- AGE: outer-SQL OFFSET/LIMIT wrap + count(*) for exact total.
- Memgraph: Cypher SKIP/LIMIT + limit+1 has_more probe (total is always None).

Contract assertions (both backends):
- Correct windowing (offset/limit) and ``returned`` count.
- Correct ``has_more`` transitions across pages.
- Exact ``total`` where the backend declares EXACT_COUNT (AGE), else ``None``.
- **Stable order across pages** when the query has a deterministic ORDER BY.
- ``params`` binding flows through.
- ``col_names`` resolution.
- Safe queries use the native path (no fallback warning).
- Unsafe queries (RETURN *, existing LIMIT, etc.) fall back transparently.
"""

import warnings

import pytest

from cypher_graphdb import BackendCapability, Page

pytestmark = pytest.mark.integration

ORDERED_QUERY = "MATCH (p:Person) RETURN p.name ORDER BY p.name"


def _seed_people(db, count: int):
    """Seed test data using UNWIND to minimise round-trips."""
    db.execute("MATCH (n) DETACH DELETE n")
    rows = [{"name": f"P{i:03d}", "age": 20 + i} for i in range(count)]
    db.execute("UNWIND $rows AS r CREATE (p:Person {name: r.name, age: r.age})", params={"rows": rows})


def _page(db, query, **kwargs):
    """Call execute_cypher_page suppressing the fallback UserWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return db.execute_cypher_page(query, **kwargs)


class _PaginationContract:
    """Shared contract assertions for both backends.

    Subclasses MUST set ``exact_count`` — there is no default. AGE sets it
    ``True`` (exact total available); Memgraph sets it ``False`` (total is
    always ``None`` on the native path).
    """

    # No default: each subclass must declare this explicitly to avoid silently
    # running wrong assertions in a new backend's test class.
    exact_count: bool

    def test_returns_page_instance(self, db):
        _seed_people(db, 5)
        page = _page(db, ORDERED_QUERY, offset=0, limit=10)
        assert isinstance(page, Page)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_first_page_window_and_total(self, db):
        _seed_people(db, 25)
        page = _page(db, ORDERED_QUERY, offset=0, limit=10)
        assert page.returned == 10
        assert page.total == (25 if self.exact_count else None)
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
        assert names == sorted(names), "rows not in stable order across pages"
        assert len(set(names)) == 25, "duplicate rows across pages"
        db.execute("MATCH (n) DETACH DELETE n")

    def test_offset_past_end_is_empty(self, db):
        _seed_people(db, 5)
        page = _page(db, ORDERED_QUERY, offset=100, limit=10)
        assert page.returned == 0
        assert page.has_more is False
        assert page.total == (5 if self.exact_count else None)
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
        assert page.total == (5 if self.exact_count else None)
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
        assert page.total == (0 if self.exact_count else None)
        assert page.has_more is False


class TestMemgraphPagination(_PaginationContract):
    """Pagination contract against live Memgraph (native SKIP/LIMIT path).

    Memgraph reports exact ``has_more`` via a limit+1 probe but ``total`` is
    always ``None`` (Memgraph does not declare EXACT_COUNT).
    """

    exact_count = False

    @pytest.fixture
    def db(self, memgraph_db):
        assert memgraph_db._backend.get_capability(BackendCapability.PAGINATION_SUPPORT) is True
        assert memgraph_db._backend.get_capability(BackendCapability.EXACT_COUNT) is False
        yield memgraph_db
        memgraph_db.execute("MATCH (n) DETACH DELETE n")

    def test_safe_query_uses_native_path(self, db):
        """Safe query must NOT emit the fallback warning; total is None."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            page = db.execute_cypher_page(ORDERED_QUERY, offset=0, limit=5)
        assert not any("native pagination" in str(w.message) for w in caught)
        assert page.total is None
        assert page.has_more is True
        db.execute("MATCH (n) DETACH DELETE n")

    def test_unsafe_query_falls_back_with_exact_total(self, db):
        """RETURN * falls back to cache-and-slice, which gives an exact total."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            page = db.execute_cypher_page("MATCH (p:Person) RETURN *", offset=0, limit=5)
        assert any("native pagination" in str(w.message) for w in caught)
        assert page.returned == 5
        assert page.total == 12
        db.execute("MATCH (n) DETACH DELETE n")

    def test_reserved_param_names_rejected(self, db):
        """Reserved param names must raise ValueError before any DB call."""
        with pytest.raises(ValueError, match="reserved"):
            db.execute_cypher_page(ORDERED_QUERY, offset=0, limit=5, params={"__cypher_page_skip__": 99})


class TestAGEPagination(_PaginationContract):
    """Pagination contract against live Apache AGE (native outer-SQL path)."""

    exact_count = True

    @pytest.fixture
    def db(self, age_db):
        assert age_db._backend.get_capability(BackendCapability.PAGINATION_SUPPORT) is True
        assert age_db._backend.get_capability(BackendCapability.EXACT_COUNT) is True
        yield age_db
        age_db.execute("MATCH (n) DETACH DELETE n")

    def test_safe_query_uses_native_path(self, db):
        """Safe query must NOT emit the fallback warning."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            db.execute_cypher_page(ORDERED_QUERY, offset=0, limit=5)
        assert not any("native pagination" in str(w.message) for w in caught)
        db.execute("MATCH (n) DETACH DELETE n")

    def test_unsafe_query_falls_back(self, db):
        """RETURN * (unsafe) must fall back to cache-and-slice."""
        _seed_people(db, 12)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            page = db.execute_cypher_page("MATCH (p:Person) RETURN *", offset=0, limit=5)
        assert any("native pagination" in str(w.message) for w in caught)
        assert page.returned == 5
        db.execute("MATCH (n) DETACH DELETE n")
