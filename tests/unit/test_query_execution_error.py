"""Facade wraps backend execution failures in backend-agnostic QueryExecutionError.

Backend drivers raise backend-specific exceptions (e.g. Memgraph's
``mgclient.DatabaseError`` or AGE's ``AGEExecutionError``). Those must never
escape the library: the facade re-raises them as ``QueryExecutionError`` so
consumers can catch a single type. Already-agnostic exceptions
(``ReadOnlyModeError``, ``LabelNotFoundError``) pass through unchanged.
"""

from __future__ import annotations

import pytest

from cypher_graphdb import LabelNotFoundError, QueryExecutionError, ReadOnlyModeError
from cypher_graphdb.cyphergraphdb import CypherGraphDB

from .mock_backend import MockBackend


class _RaisingBackend(MockBackend):
    """Mock backend whose execute_cypher raises a configured exception."""

    def __init__(self, exc: Exception):
        super().__init__()
        self._exc = exc

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
        raise self._exc


class _FakeDriverError(Exception):
    """Stand-in for a backend driver exception (e.g. mgclient.DatabaseError)."""


def _db(exc: Exception) -> CypherGraphDB:
    return CypherGraphDB(_RaisingBackend(exc))


def test_backend_driver_error_wrapped_in_query_execution_error():
    cdb = _db(_FakeDriverError("syntax error near 'RETURN'"))

    with pytest.raises(QueryExecutionError) as ei:
        cdb.execute_with_stats("MATCH (n RETURN n")

    assert "syntax error near 'RETURN'" in str(ei.value)
    # Original backend exception preserved as the cause.
    assert isinstance(ei.value.__cause__, _FakeDriverError)


def test_wrapping_applies_to_execute_path_too():
    cdb = _db(_FakeDriverError("boom"))

    with pytest.raises(QueryExecutionError):
        cdb.execute("MATCH (n) RETURN n")


def test_read_only_mode_error_passes_through_unwrapped():
    cdb = _db(ReadOnlyModeError("read only"))

    with pytest.raises(ReadOnlyModeError):
        cdb.execute_with_stats("CREATE (n)")


def test_label_not_found_error_passes_through_unwrapped():
    cdb = _db(LabelNotFoundError("no such label"))

    with pytest.raises(LabelNotFoundError):
        cdb.execute_with_stats("MATCH (n:Ghost) RETURN n")
