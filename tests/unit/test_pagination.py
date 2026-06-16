"""Unit tests for the pagination Page model and cache-and-slice fallback.

These tests exercise ``PaginationMixin.execute_cypher_page`` via a controllable
mock backend (no real database). Integration tests in
``tests/integration/test_pagination.py`` cover the live AGE/Memgraph paths.
"""

import warnings

import pytest
from pydantic import ValidationError

from cypher_graphdb import BackendCapability, Page
from cypher_graphdb.backend import CypherBackend, ExecStatistics
from cypher_graphdb.cyphergraphdb import CypherGraphDB


class _RowsBackend(CypherBackend):
    """Mock backend that returns a fixed list of rows from execute_cypher.

    Declares no PAGINATION_SUPPORT so the fallback path is always taken.
    Counts how many times ``execute_cypher`` is called so tests can assert
    the number of materialization round-trips.
    """

    name = "rows-mock"

    def __init__(self, rows):
        super().__init__(None, autocommit=True)
        self._rows = rows
        self.graph_name = "test"
        self.execute_count = 0  # materialization round-trip counter

    @property
    def connected(self) -> bool:
        return True

    def connect(self, *args, **kwargs):
        return None

    def disconnect(self):
        return None

    def create_graph(self, graph_name=None):
        return None

    def drop_graph(self, graph_name=None):
        return None

    def graph_exists(self, graph_name: str = None) -> bool:
        return True

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
        self.execute_count += 1
        rows = self._rows
        if params and "minval" in params:
            rows = [r for r in rows if r[0] >= params["minval"]]
        stats = ExecStatistics(row_count=len(rows), col_count=1)
        return list(rows), stats

    def execute_cypher_stream(self, cypher_query, chunk_size=1000, raw_data=False, params=None):
        return
        yield

    def fulltext_search(self, cypher_query, fts_query, language=None):
        return [], None

    def labels(self):
        return []

    def graphs(self):
        return []

    def commit(self):
        return None

    def rollback(self):
        return None


def _db(rows):
    return CypherGraphDB(_RowsBackend(rows))


def _ignore():
    warnings.simplefilter("ignore")


# ── Page model ─────────────────────────────────────────────────────────────


def test_page_returned_is_computed_from_rows():
    page = Page(rows=[(1,), (2,), (3,)], offset=0, limit=10)
    assert page.returned == 3


def test_page_returned_is_always_len_rows():
    """returned is a computed_field, always len(rows) regardless of what is passed."""
    page = Page(rows=[(1,), (2,)], offset=0, limit=10)
    assert page.returned == len(page.rows) == 2


def test_page_empty():
    page = Page(rows=[], offset=0, limit=10, total=0)
    assert page.returned == 0
    assert page.is_empty()


def test_page_is_frozen():
    page = Page(rows=[(1,)], offset=0, limit=10)
    with pytest.raises(ValidationError):
        page.offset = 5


def test_page_truncated_requires_total():
    """truncated=True with total=None must be rejected by the model validator."""
    with pytest.raises(ValidationError, match="truncated"):
        Page(rows=[], offset=0, limit=10, truncated=True, total=None)


def test_page_truncated_with_total_is_valid():
    page = Page(rows=[], offset=0, limit=10, truncated=True, total=50_000)
    assert page.truncated is True
    assert page.total == 50_000


# ── Capability ─────────────────────────────────────────────────────────────


def test_pagination_capability_enum_exists():
    assert BackendCapability.PAGINATION_SUPPORT is not None
    assert BackendCapability.EXACT_COUNT is not None


def test_mock_backend_has_no_pagination_capability():
    db = _db([(i,) for i in range(5)])
    assert db._backend.has_capability(BackendCapability.PAGINATION_SUPPORT) is False


def test_reserved_params_are_rejected():
    db = _db([(1,)])
    with pytest.raises(ValueError, match="reserved"):
        db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=5, params={"__cypher_page_skip__": 0})


# ── Fallback windowing ─────────────────────────────────────────────────────


def test_fallback_first_page():
    db = _db([(i,) for i in range(25)])
    with warnings.catch_warnings():
        _ignore()
        page = db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=10)
    assert page.returned == 10
    assert page.total == 25
    assert page.has_more is True
    assert page.rows[0] == (0,)
    assert page.rows[-1] == (9,)


def test_fallback_last_partial_page():
    db = _db([(i,) for i in range(25)])
    with warnings.catch_warnings():
        _ignore()
        page = db.execute_cypher_page("MATCH (n) RETURN n", offset=20, limit=10)
    assert page.returned == 5
    assert page.total == 25
    assert page.has_more is False
    assert page.rows[0] == (20,)


def test_fallback_offset_past_end():
    db = _db([(i,) for i in range(5)])
    with warnings.catch_warnings():
        _ignore()
        page = db.execute_cypher_page("MATCH (n) RETURN n", offset=100, limit=10)
    assert page.returned == 0
    assert page.is_empty()
    assert page.has_more is False
    assert page.total == 5


def test_fallback_stable_order_and_no_dupes_across_pages():
    """Pages from the fallback must be stable-ordered and duplicate-free.

    The fallback materializes once per call (no server-side cache), so three
    page requests mean three execute_cypher calls. This is documented behaviour
    of the fallback; the test explicitly asserts this so future caching changes
    are visible here.
    """
    db = _db([(i,) for i in range(25)])
    with warnings.catch_warnings():
        _ignore()
        p0 = db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=10)
        p1 = db.execute_cypher_page("MATCH (n) RETURN n", offset=10, limit=10)
        p2 = db.execute_cypher_page("MATCH (n) RETURN n", offset=20, limit=10)
    combined = [r[0] for r in (p0.rows + p1.rows + p2.rows)]
    assert combined == list(range(25)), "rows not in stable order across pages"
    assert len(set(combined)) == 25, "duplicates across pages"
    # The fallback re-materializes on every call — 3 pages = 3 backend calls.
    assert db._backend.execute_count == 3


def test_fallback_params_are_applied():
    db = _db([(i,) for i in range(25)])
    with warnings.catch_warnings():
        _ignore()
        page = db.execute_cypher_page("MATCH (n) WHERE n >= $minval RETURN n", offset=0, limit=5, params={"minval": 20})
    assert page.total == 5  # 20..24
    assert [r[0] for r in page.rows] == [20, 21, 22, 23, 24]


def test_fallback_truncates_at_max_rows():
    db = _db([(i,) for i in range(100)])
    with warnings.catch_warnings():
        _ignore()
        page = db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=10, max_rows=30)
    assert page.truncated is True
    assert page.total == 30
    assert page.has_more is True


def test_fallback_emits_warning():
    db = _db([(1,)])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=10)
    assert any("does not support native pagination" in str(w.message) for w in caught)


def test_negative_offset_rejected():
    db = _db([(1,)])
    with pytest.raises(ValueError, match="offset"):
        db.execute_cypher_page("MATCH (n) RETURN n", offset=-1, limit=10)


def test_negative_limit_rejected():
    db = _db([(1,)])
    with pytest.raises(ValueError, match="limit"):
        db.execute_cypher_page("MATCH (n) RETURN n", offset=0, limit=-5)
