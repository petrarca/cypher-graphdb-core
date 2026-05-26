"""Unit tests for cypher_graphdb.settings.Settings.

Covers:
- Default prefix behaviour (CGDB_*)
- Custom env_prefix via Settings.with_prefix()
- Multi-instance isolation (two settings reading two prefixes)
- Case-insensitive env var matching
- Type coercion (bool, int) from prefixed env vars
"""

import os

import pytest

from cypher_graphdb.settings import DEFAULT_ENV_PREFIX, Settings, get_settings


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch, tmp_path):
    """Clear CGDB-related env vars and prevent .env from being read.

    The local ``.env`` file in the repo root would otherwise pollute these
    tests with real connection strings. We point pydantic-settings at a
    nonexistent file to disable .env loading for the duration of each test.
    """
    for key in list(os.environ.keys()):
        if "CGDB" in key.upper() or key in {
            "BACKEND",
            "CINFO",
            "GRAPH",
            "READ_ONLY",
            "CREATE_GRAPH_IF_NOT_EXISTS",
            "QUERY_TIMEOUT_S",
        }:
            monkeypatch.delenv(key, raising=False)
    # Patch Settings.model_config to not read any .env during tests.
    monkeypatch.setitem(Settings.model_config, "env_file", str(tmp_path / "nonexistent.env"))
    # Reset the lru_cache.
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestDefaultPrefix:
    """Default CGDB_ prefix behaviour (unchanged)."""

    def test_reads_default_cgdb_env_vars(self, monkeypatch):
        monkeypatch.setenv("CGDB_BACKEND", "age")
        monkeypatch.setenv("CGDB_CINFO", "postgresql://localhost/x")
        monkeypatch.setenv("CGDB_GRAPH", "techgraph")
        s = Settings()
        assert s.backend == "age"
        assert s.cinfo == "postgresql://localhost/x"
        assert s.graph == "techgraph"

    def test_defaults_when_no_env_vars(self):
        s = Settings()
        assert s.backend is None
        assert s.cinfo is None
        assert s.graph is None
        assert s.read_only is False
        assert s.create_graph is False
        assert s.query_timeout_s == 30

    def test_get_settings_returns_cached_instance(self, monkeypatch):
        monkeypatch.setenv("CGDB_BACKEND", "memgraph")
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2
        assert s1.backend == "memgraph"

    def test_default_env_prefix_constant(self):
        assert DEFAULT_ENV_PREFIX == "CGDB_"


class TestWithPrefix:
    """Settings.with_prefix() for multi-instance setups."""

    def test_reads_prefixed_env_vars(self, monkeypatch):
        monkeypatch.setenv("CGDB_TS_BACKEND", "age")
        monkeypatch.setenv("CGDB_TS_CINFO", "postgresql://localhost/techstack")
        monkeypatch.setenv("CGDB_TS_GRAPH", "techgraph")
        s = Settings.with_prefix("CGDB_TS_")
        assert s.backend == "age"
        assert s.cinfo == "postgresql://localhost/techstack"
        assert s.graph == "techgraph"

    def test_ignores_default_env_vars_when_prefixed(self, monkeypatch):
        # Default vars set, but with_prefix should ignore them.
        monkeypatch.setenv("CGDB_BACKEND", "default-backend")
        monkeypatch.setenv("CGDB_GRAPH", "default-graph")
        # Only the prefixed graph is set.
        monkeypatch.setenv("CGDB_CG_GRAPH", "codegraph")
        s = Settings.with_prefix("CGDB_CG_")
        assert s.backend is None  # CGDB_CG_BACKEND not set
        assert s.graph == "codegraph"

    def test_two_instances_isolated(self, monkeypatch):
        monkeypatch.setenv("CGDB_TS_BACKEND", "age")
        monkeypatch.setenv("CGDB_TS_GRAPH", "techgraph")
        monkeypatch.setenv("CGDB_CG_BACKEND", "memgraph")
        monkeypatch.setenv("CGDB_CG_GRAPH", "codegraph")
        ts = Settings.with_prefix("CGDB_TS_")
        cg = Settings.with_prefix("CGDB_CG_")
        assert ts.backend == "age"
        assert ts.graph == "techgraph"
        assert cg.backend == "memgraph"
        assert cg.graph == "codegraph"

    def test_with_default_prefix_matches_settings(self, monkeypatch):
        monkeypatch.setenv("CGDB_BACKEND", "age")
        monkeypatch.setenv("CGDB_GRAPH", "g")
        plain = Settings()
        prefixed = Settings.with_prefix(DEFAULT_ENV_PREFIX)
        assert plain.backend == prefixed.backend
        assert plain.graph == prefixed.graph

    def test_case_insensitive_env_lookup(self, monkeypatch):
        # Lowercase env var should still match (case_sensitive=False).
        monkeypatch.setenv("cgdb_ts_backend", "age")
        s = Settings.with_prefix("CGDB_TS_")
        assert s.backend == "age"

    def test_bool_coercion(self, monkeypatch):
        monkeypatch.setenv("CGDB_TS_READ_ONLY", "true")
        monkeypatch.setenv("CGDB_TS_CREATE_GRAPH_IF_NOT_EXISTS", "1")
        s = Settings.with_prefix("CGDB_TS_")
        assert s.read_only is True
        assert s.create_graph is True

    def test_int_coercion(self, monkeypatch):
        monkeypatch.setenv("CGDB_TS_QUERY_TIMEOUT_S", "120")
        s = Settings.with_prefix("CGDB_TS_")
        assert s.query_timeout_s == 120

    def test_defaults_when_prefixed_env_not_set(self):
        s = Settings.with_prefix("CGDB_UNSET_")
        assert s.backend is None
        assert s.cinfo is None
        assert s.graph is None
        assert s.read_only is False
        assert s.create_graph is False
        assert s.query_timeout_s == 30

    def test_empty_prefix_reads_all_unprefixed(self, monkeypatch):
        # Empty prefix means read BACKEND/CINFO/GRAPH directly.
        monkeypatch.setenv("BACKEND", "age")
        monkeypatch.setenv("GRAPH", "g")
        s = Settings.with_prefix("")
        assert s.backend == "age"
        assert s.graph == "g"


class TestPrefixSanitization:
    """cinfo sanitization works regardless of prefix source."""

    def test_sanitized_cinfo_with_prefix(self, monkeypatch):
        monkeypatch.setenv("CGDB_TS_CINFO", "postgresql://user:secret@host/db")
        s = Settings.with_prefix("CGDB_TS_")
        # Sanitized variant must redact the secret.
        assert s.cinfo == "postgresql://user:secret@host/db"
        sanitized = s.cinfo_sanitized
        assert sanitized is not None
        assert "secret" not in sanitized
