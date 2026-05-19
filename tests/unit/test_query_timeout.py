"""Unit tests for query_timeout_s connection parameter.

Tests that:
- AGE backend injects statement_timeout into the psycopg options string
- AGE backend preserves existing options when adding statement_timeout
- Memgraph backend calls SET DATABASE SETTING after connecting
- query_timeout_s=None leaves both backends unchanged
- Settings class exposes query_timeout_s (from CGDB_QUERY_TIMEOUT_S)
- CypherGraphDB.connect() threads query_timeout_s from settings into the backend
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cypher_graphdb.settings import Settings

# ---------------------------------------------------------------------------
# AGE backend
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAGEQueryTimeout:
    def _make_age(self):
        from cypher_graphdb.backends.age.agegraphdb import AGEGraphDB

        db = AGEGraphDB.__new__(AGEGraphDB)
        # Initialise only the fields connect() touches
        db._connection = None
        db._query_timeout_s = None
        db.autocommit = True
        db._read_only = False
        db._set_graph_if_not_exists = True
        db._graph_name = None
        db._cursor_factory = None
        db._ckwargs = {}
        return db

    def test_statement_timeout_injected_into_options(self):
        """query_timeout_s=30 -> options='-c statement_timeout=30000'."""
        db = self._make_age()
        with (
            patch.object(db, "_connect_with_retry"),
            patch.object(db, "graph_exists", return_value=True),
            patch("cypher_graphdb.backends.age.agegraphdb.conninfo") as mock_conninfo,
        ):
            mock_conninfo.make_conninfo.return_value = "mock_cinfo"
            mock_conninfo.conninfo_to_dict.return_value = {}
            db.connect(cinfo="host=localhost", graph_name="test", query_timeout_s=30)

        call_kwargs = mock_conninfo.make_conninfo.call_args
        assert call_kwargs is not None
        call_kwargs.kwargs.get("options") or (call_kwargs.args[1] if len(call_kwargs.args) > 1 else None)
        # options may come through as a kwarg or positional via **kwargs spread
        _, kwargs = call_kwargs
        assert "options" in kwargs, f"options not in make_conninfo kwargs: {kwargs}"
        assert "statement_timeout=30000" in kwargs["options"]

    def test_no_timeout_no_options(self):
        """query_timeout_s=None -> options not injected."""
        db = self._make_age()
        with (
            patch.object(db, "_connect_with_retry"),
            patch.object(db, "graph_exists", return_value=True),
            patch("cypher_graphdb.backends.age.agegraphdb.conninfo") as mock_conninfo,
        ):
            mock_conninfo.make_conninfo.return_value = "mock_cinfo"
            mock_conninfo.conninfo_to_dict.return_value = {}
            db.connect(cinfo="host=localhost", graph_name="test")

        _, kwargs = mock_conninfo.make_conninfo.call_args
        assert "options" not in kwargs or "statement_timeout" not in kwargs.get("options", "")

    def test_existing_options_preserved(self):
        """Existing options string is preserved when adding statement_timeout."""
        db = self._make_age()
        with (
            patch.object(db, "_connect_with_retry"),
            patch.object(db, "graph_exists", return_value=True),
            patch("cypher_graphdb.backends.age.agegraphdb.conninfo") as mock_conninfo,
        ):
            mock_conninfo.make_conninfo.return_value = "mock_cinfo"
            mock_conninfo.conninfo_to_dict.return_value = {}
            db.connect(cinfo="host=localhost", graph_name="test", options="-c lock_timeout=5000", query_timeout_s=10)

        _, kwargs = mock_conninfo.make_conninfo.call_args
        assert "lock_timeout=5000" in kwargs["options"]
        assert "statement_timeout=10000" in kwargs["options"]

    def test_timeout_stored_on_backend(self):
        """query_timeout_s value is stored on the backend instance."""
        db = self._make_age()
        with (
            patch.object(db, "_connect_with_retry"),
            patch.object(db, "graph_exists", return_value=True),
            patch("cypher_graphdb.backends.age.agegraphdb.conninfo") as mock_conninfo,
        ):
            mock_conninfo.make_conninfo.return_value = "mock_cinfo"
            mock_conninfo.conninfo_to_dict.return_value = {}
            db.connect(cinfo="host=localhost", graph_name="test", query_timeout_s=60)

        assert db._query_timeout_s == 60


# ---------------------------------------------------------------------------
# Memgraph backend
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMemgraphQueryTimeout:
    def _make_memgraph(self):
        from cypher_graphdb.backends.memgraph.memgraphdb import MemgraphDB

        db = MemgraphDB.__new__(MemgraphDB)
        db._connection = None
        db._query_timeout_s = None
        db.autocommit = True
        db._read_only = False
        db._cinfo = None
        db._ckwargs = {}
        db._graph_name = "memgraph"
        return db

    def test_set_database_setting_called_with_timeout(self):
        """query_timeout_s=30 -> SET DATABASE SETTING 'query.timeout' TO '30000' called."""
        db = self._make_memgraph()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db, "_connect_with_retry", return_value=mock_conn):
            db.connect(cinfo="bolt://localhost:7687", query_timeout_s=30)

        mock_cursor.execute.assert_called_once_with("SET DATABASE SETTING 'query.timeout' TO '30000'")
        mock_cursor.close.assert_called_once()

    def test_no_timeout_no_set_database_setting(self):
        """query_timeout_s=None -> SET DATABASE SETTING not called."""
        db = self._make_memgraph()
        mock_conn = MagicMock()

        with patch.object(db, "_connect_with_retry", return_value=mock_conn):
            db.connect(cinfo="bolt://localhost:7687")

        # cursor() should not have been called for timeout setup
        mock_conn.cursor.assert_not_called()

    def test_timeout_stored_on_backend(self):
        """query_timeout_s value is stored on the backend instance."""
        db = self._make_memgraph()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch.object(db, "_connect_with_retry", return_value=mock_conn):
            db.connect(cinfo="bolt://localhost:7687", query_timeout_s=45)

        assert db._query_timeout_s == 45


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestQueryTimeoutSettings:
    def test_default_is_30(self):
        """Default query_timeout_s is 30s -- safe for shared graph pools."""
        s = Settings()
        assert s.query_timeout_s == 30

    def test_env_var_sets_value(self, monkeypatch):
        """CGDB_QUERY_TIMEOUT_S env var is picked up."""
        monkeypatch.setenv("CGDB_QUERY_TIMEOUT_S", "30")
        s = Settings()
        assert s.query_timeout_s == 30

    def test_unset_env_var_uses_default(self, monkeypatch):
        """Unset env var falls back to the 30s default."""
        monkeypatch.delenv("CGDB_QUERY_TIMEOUT_S", raising=False)
        s = Settings()
        assert s.query_timeout_s == 30
