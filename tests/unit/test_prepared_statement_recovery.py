"""Unit tests for prepared statement recovery in the AGE backend.

The AGE backend caches PostgreSQL prepared statements for parameterized
queries. Two failure modes can make a cached statement stale:

  A. Normal staleness: the connection was idle in the pool while PostgreSQL
     recycled the backend process (idle timeout, PgBouncer reset). The
     Python-side cache thinks the statement exists; the server has forgotten it.
     The connection itself is still alive.

  B. Crash cascade: an AGE C-extension crash on another connection triggered
     PostgreSQL shared-memory recovery, wiping prepared statements on all
     backend processes. The connection may also be dead.

Both cases raise psycopg.errors.InvalidSqlStatementName on EXECUTE.
The recovery path must:
  1. Clear the entire stale cache (not just one entry).
  2. Detect whether the connection is dead (rollback fails = dead).
  3. Reconnect if dead, via _require_connection().
  4. Re-prepare and re-execute -- transparently, without surfacing an error.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg
import pytest


def _make_age_with_connection():
    """Build a minimal AGEGraphDB instance with a mock connection."""
    from cypher_graphdb.backends.age.agegraphdb import AGEGraphDB

    db = AGEGraphDB.__new__(AGEGraphDB)
    db._connection = MagicMock()
    db._query_timeout_s = None
    db.autocommit = True
    db._read_only = False
    db._graph_name = "test_graph"
    db._cursor_factory = None
    db._ckwargs = {}
    db._cinfo = "host=localhost"
    db._model_provider = MagicMock()
    db._prepared_statements = {}
    db._max_cached_statements = 10
    db._bulk_writer = None
    return db


def _make_parsed_query(cypher: str = "MATCH (n) RETURN n"):
    """Build a minimal ParsedCypherQuery mock."""
    pq = MagicMock()
    pq.parsed_query = cypher
    pq.return_arguments = []
    pq.read_only = True
    pq.write_operations = []
    return pq


@pytest.mark.unit
class TestPreparedStatementRecovery:
    def test_case_a_stale_statement_retried_transparently(self):
        """Connection alive, statement gone -- rollback succeeds, re-prepare, re-execute."""
        db = _make_age_with_connection()
        pq = _make_parsed_query()
        params = {"_p0": "test"}
        expected_rows = [("result",)]

        with (
            patch.object(db, "_validate_read_only"),
            patch.object(db, "_get_or_prepare_statement", return_value="cypher_stmt_abc"),
            patch("cypher_graphdb.backends.age.agegraphdb.SQLBuilder") as mock_builder,
            patch.object(db, "_require_connection"),
            patch.object(db, "_run_prepared") as mock_run,
        ):
            mock_builder.create_cypher_sql.return_value = (MagicMock(), None)
            mock_run.side_effect = [
                psycopg.errors.InvalidSqlStatementName("prepared statement does not exist"),
                (expected_rows, MagicMock()),
            ]
            result, _stats, _ = db._execute_prepared(pq, params=params)

        assert result == expected_rows
        assert mock_run.call_count == 2
        # Connection rollback was called (sub-case A: connection alive)
        db._connection.rollback.assert_called_once()

    def test_case_b_dead_connection_reconnects_before_retry(self):
        """Connection dead -- rollback fails, connection closed, reconnect, then retry succeeds."""
        db = _make_age_with_connection()
        pq = _make_parsed_query()
        params = {"_p0": "test"}

        # Rollback fails because connection is dead
        db._connection.rollback.side_effect = psycopg.OperationalError("connection lost")

        reconnect_called = []

        def fake_require_connection():
            reconnect_called.append(True)
            db._connection = MagicMock()

        with (
            patch.object(db, "_validate_read_only"),
            patch.object(db, "_get_or_prepare_statement", return_value="cypher_stmt_new"),
            patch("cypher_graphdb.backends.age.agegraphdb.SQLBuilder") as mock_builder,
            patch.object(db, "_require_connection", side_effect=fake_require_connection),
            patch.object(db, "_run_prepared") as mock_run,
        ):
            mock_builder.create_cypher_sql.return_value = (MagicMock(), None)
            mock_run.side_effect = [
                psycopg.errors.InvalidSqlStatementName("prepared statement does not exist"),
                ([("ok",)], MagicMock()),
            ]
            db._execute_prepared(pq, params=params)

        assert len(reconnect_called) == 1, "_require_connection should reconnect after dead connection"

    def test_stale_statement_clears_full_cache(self):
        """On InvalidSqlStatementName, the entire cache is cleared (not just the one entry).

        After an AGE crash, all server-side statements are gone -- clearing only
        the one that failed would leave stale entries for subsequent calls.
        """
        db = _make_age_with_connection()
        pq = _make_parsed_query()
        params = {"_p0": "test"}

        db._prepared_statements = {
            "hash1": "cypher_stmt_hash1",
            "hash2": "cypher_stmt_hash2",
            "hash3": "cypher_stmt_hash3",
        }

        with (
            patch.object(db, "_validate_read_only"),
            patch.object(db, "_get_or_prepare_statement", return_value="cypher_stmt_new"),
            patch("cypher_graphdb.backends.age.agegraphdb.SQLBuilder") as mock_builder,
            patch.object(db, "_require_connection"),
            patch.object(db, "_run_prepared") as mock_run,
        ):
            mock_builder.create_cypher_sql.return_value = (MagicMock(), None)
            mock_run.side_effect = [
                psycopg.errors.InvalidSqlStatementName("prepared statement does not exist"),
                ([("ok",)], MagicMock()),
            ]
            db._execute_prepared(pq, params=params)

        # All stale entries must be gone after retry
        assert "hash1" not in db._prepared_statements
        assert "hash2" not in db._prepared_statements
        assert "hash3" not in db._prepared_statements

    def test_retry_failure_raises_and_cleans_up(self):
        """If re-prepare also fails, AGEExecutionError is raised and connection is cleaned up."""
        from cypher_graphdb.backends.age.agegraphdb import AGEExecutionError

        db = _make_age_with_connection()
        pq = _make_parsed_query()
        params = {"_p0": "test"}

        with (
            patch.object(db, "_validate_read_only"),
            patch.object(
                db,
                "_get_or_prepare_statement",
                side_effect=[
                    "cypher_stmt_first",
                    psycopg.OperationalError("connection failed"),
                ],
            ),
            patch("cypher_graphdb.backends.age.agegraphdb.SQLBuilder") as mock_builder,
            patch.object(db, "_require_connection"),
            patch.object(db, "_run_prepared") as mock_run,
        ):
            mock_builder.create_cypher_sql.return_value = (MagicMock(), None)
            mock_run.side_effect = [
                psycopg.errors.InvalidSqlStatementName("prepared statement does not exist"),
            ]
            with pytest.raises(AGEExecutionError, match="re-prepare"):
                db._execute_prepared(pq, params=params)

        # Connection must be closed and set to None after unrecoverable failure
        assert db._connection is None
        assert db._prepared_statements == {}

    def test_non_stale_error_not_retried(self):
        """Errors other than InvalidSqlStatementName are not retried."""
        from cypher_graphdb.backends.age.agegraphdb import AGEExecutionError

        db = _make_age_with_connection()
        pq = _make_parsed_query()
        params = {"_p0": "test"}

        with (
            patch.object(db, "_validate_read_only"),
            patch.object(db, "_get_or_prepare_statement", return_value="cypher_stmt_abc"),
            patch("cypher_graphdb.backends.age.agegraphdb.SQLBuilder") as mock_builder,
            patch.object(db, "_run_prepared") as mock_run,
        ):
            mock_builder.create_cypher_sql.return_value = (MagicMock(), None)
            mock_run.side_effect = psycopg.OperationalError("connection lost")
            with pytest.raises(AGEExecutionError):
                db._execute_prepared(pq, params=params)

        assert mock_run.call_count == 1
        assert db._connection is None
        assert db._prepared_statements == {}


@pytest.mark.unit
class TestReconnectClearsCache:
    def test_reconnect_clears_prepared_statements(self):
        """reconnect() must clear the prepared statement cache."""
        db = _make_age_with_connection()
        db._prepared_statements = {"hash1": "stmt1", "hash2": "stmt2"}

        with patch.object(db, "connect_to_db"):
            db.reconnect()

        assert db._prepared_statements == {}

    def test_reconnect_closes_old_connection(self):
        """reconnect() closes the old connection before creating a new one."""
        db = _make_age_with_connection()
        old_conn = db._connection

        with patch.object(db, "connect_to_db"):
            db.reconnect()

        old_conn.close.assert_called_once()
        # After close, _connection is set to None before connect_to_db rebuilds it
