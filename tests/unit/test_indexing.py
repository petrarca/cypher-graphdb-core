"""Tests for indexing capabilities, IndexInfo model, and facade methods."""

import pytest

from cypher_graphdb.backend import BackendCapability, CypherBackend
from cypher_graphdb.statistics import IndexInfo, IndexType

# ── IndexInfo model tests ─────────────────────────────────────────────────


class TestIndexType:
    """Tests for IndexType enum."""

    def test_property_value(self):
        assert IndexType.PROPERTY.value == "property"

    def test_unique_value(self):
        assert IndexType.UNIQUE.value == "unique"

    def test_fulltext_value(self):
        assert IndexType.FULLTEXT.value == "fulltext"

    def test_vector_value(self):
        assert IndexType.VECTOR.value == "vector"

    def test_is_string_enum(self):
        """IndexType values can be compared to strings."""
        assert IndexType.PROPERTY == "property"
        assert IndexType.UNIQUE == "unique"


class TestIndexInfo:
    """Tests for IndexInfo Pydantic model."""

    def test_minimal_creation(self):
        idx = IndexInfo(label="Method")
        assert idx.label == "Method"
        assert idx.property_names is None
        assert idx.index_type == IndexType.PROPERTY
        assert idx.index_name is None
        assert idx.unique is False

    def test_full_creation(self):
        idx = IndexInfo(
            label="Method",
            property_names=["id", "name"],
            index_type=IndexType.PROPERTY,
            index_name="test_Method_props_gin",
            unique=False,
        )
        assert idx.label == "Method"
        assert idx.property_names == ["id", "name"]
        assert idx.index_name == "test_Method_props_gin"

    def test_unique_constraint(self):
        idx = IndexInfo(label="User", property_names=["email"], index_type=IndexType.UNIQUE, unique=True)
        assert idx.unique is True
        assert idx.index_type == IndexType.UNIQUE

    def test_gin_index_none_properties(self):
        """AGE GIN indexes cover all properties, so property_names is None."""
        idx = IndexInfo(label="Method", property_names=None, index_name="graph_Method_props_gin")
        assert idx.property_names is None

    def test_serialization(self):
        idx = IndexInfo(label="Method", property_names=["id"], index_type=IndexType.PROPERTY, index_name="idx_1")
        data = idx.model_dump()
        assert data["label"] == "Method"
        assert data["property_names"] == ["id"]
        assert data["index_type"] == "property"
        assert data["index_name"] == "idx_1"
        assert data["unique"] is False


# ── BackendCapability enum tests ──────────────────────────────────────────


class TestBackendCapabilityExtensions:
    """Tests for new BackendCapability enum values."""

    def test_property_index_exists(self):
        assert hasattr(BackendCapability, "PROPERTY_INDEX")

    def test_unique_constraint_exists(self):
        assert hasattr(BackendCapability, "UNIQUE_CONSTRAINT")

    def test_fulltext_index_exists(self):
        assert hasattr(BackendCapability, "FULLTEXT_INDEX")

    def test_vector_index_exists(self):
        assert hasattr(BackendCapability, "VECTOR_INDEX")

    def test_enum_values_are_unique(self):
        values = [cap.value for cap in BackendCapability]
        assert len(values) == len(set(values))


# ── CypherBackend default method tests ────────────────────────────────────


class MinimalBackend(CypherBackend):
    """Backend with only abstract methods implemented (no index/bulk support)."""

    def __init__(self):
        super().__init__(None)

    def connect(self, *args, **kwargs):
        pass

    def disconnect(self):
        pass

    def create_graph(self, graph_name=None):
        pass

    def drop_graph(self, graph_name=None):
        pass

    def graph_exists(self, graph_name=None):
        return True

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
        return [], None

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
        pass

    def rollback(self):
        pass


class TestDefaultBackendMethods:
    """Test that default CypherBackend methods raise NotImplementedError."""

    def setup_method(self):
        self.backend = MinimalBackend()

    def test_create_property_index_raises(self):
        with pytest.raises(NotImplementedError, match="does not support create_property_index"):
            self.backend.create_property_index("Label", "prop")

    def test_drop_index_raises(self):
        with pytest.raises(NotImplementedError, match="does not support drop_index"):
            self.backend.drop_index("Label", "prop")

    def test_list_indexes_raises(self):
        with pytest.raises(NotImplementedError, match="does not support list_indexes"):
            self.backend.list_indexes()

    def test_bulk_create_nodes_raises(self):
        with pytest.raises(NotImplementedError, match="does not support bulk_create_nodes"):
            self.backend.bulk_create_nodes("Label", [{"a": 1}])

    def test_bulk_create_edges_raises(self):
        with pytest.raises(NotImplementedError, match="does not support bulk_create_edges"):
            self.backend.bulk_create_edges("Label", [{"src": "a", "dst": "b"}])

    def test_has_capability_false_for_property_index(self):
        """MinimalBackend doesn't override get_capability, so PROPERTY_INDEX is unsupported."""
        assert self.backend.has_capability(BackendCapability.PROPERTY_INDEX) is False

    def test_has_capability_false_for_unique_constraint(self):
        assert self.backend.has_capability(BackendCapability.UNIQUE_CONSTRAINT) is False

    def test_has_capability_false_for_fulltext_index(self):
        assert self.backend.has_capability(BackendCapability.FULLTEXT_INDEX) is False

    def test_has_capability_false_for_vector_index(self):
        assert self.backend.has_capability(BackendCapability.VECTOR_INDEX) is False


# ── AGE _parse_index_def static method tests ──────────────────────────────


class TestAGEParseIndexDef:
    """Test AGEGraphDB._parse_index_def without needing a live connection."""

    def setup_method(self):
        from cypher_graphdb.backends.age.agegraphdb import AGEGraphDB

        self.parse = AGEGraphDB._parse_index_def

    def test_gin_index(self):
        result = self.parse(
            "Method",
            "graph_Method_props_gin",
            'CREATE INDEX graph_Method_props_gin ON graph."Method" USING gin (properties)',
        )
        assert result is not None
        assert result.label == "Method"
        assert result.index_type == IndexType.PROPERTY
        assert result.property_names is None  # GIN covers all
        assert result.index_name == "graph_Method_props_gin"
        assert result.unique is False

    def test_unique_index(self):
        result = self.parse(
            "User",
            "user_email_unique",
            'CREATE UNIQUE INDEX user_email_unique ON graph."User" USING btree (email)',
        )
        assert result is not None
        assert result.label == "User"
        assert result.index_type == IndexType.UNIQUE
        assert result.unique is True
        assert result.property_names == ["email"]

    def test_btree_index(self):
        result = self.parse(
            "Method",
            "method_name_idx",
            'CREATE INDEX method_name_idx ON graph."Method" USING btree (name)',
        )
        assert result is not None
        assert result.index_type == IndexType.PROPERTY
        assert result.property_names == ["name"]
