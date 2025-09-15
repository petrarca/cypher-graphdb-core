"""Test backend capability system functionality."""

import pytest

from cypher_graphdb.backend import BackendCapability, CypherBackend


class MockBackendForTesting(CypherBackend):
    """Mock implementation of CypherBackend for testing capabilities."""

    def __init__(self, **kwargs):
        super().__init__(None, **kwargs)
        self._id = "test_backend"

    def connect(self, *args, **kwargs):
        self._connection = "test_connection"

    def disconnect(self):
        self._connection = None

    def create_graph(self, graph_name=None):
        return True

    def drop_graph(self, graph_name=None):
        return True

    def graph_exists(self, graph_name: str = None) -> bool:
        return True

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False):
        return [], None

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

    # Override capability for testing
    def get_capability(self, capability: BackendCapability):
        if capability == BackendCapability.LABEL_FUNCTION:
            return True
        elif capability == BackendCapability.SUPPORT_MULTIPLE_LABELS:
            return False
        else:
            return super().get_capability(capability)


class BareBackend(CypherBackend):
    """Minimal backend without capability implementation."""

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

    def graph_exists(self, graph_name: str = None) -> bool:
        return True

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False):
        return [], None

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


def test_backend_capability_enum():
    """Test that BackendCapability enum has expected values."""
    assert BackendCapability.LABEL_FUNCTION.value == "label_function"
    assert BackendCapability.SUPPORT_MULTIPLE_LABELS.value == "support_multiple_labels"


def test_base_capability_not_implemented():
    """Test that base CypherBackend raises NotImplementedError."""
    backend = BareBackend()

    with pytest.raises(NotImplementedError):
        backend.get_capability(BackendCapability.LABEL_FUNCTION)


def test_backend_has_capability():
    """Test has_capability method."""
    backend = MockBackendForTesting()

    # Should return True for supported capabilities
    assert backend.has_capability(BackendCapability.LABEL_FUNCTION) is True
    assert backend.has_capability(BackendCapability.SUPPORT_MULTIPLE_LABELS) is True

    # Should return False for unsupported capabilities
    class MockCapability:
        value = "unsupported_capability"

    assert backend.has_capability(MockCapability()) is False


def test_backend_capability_inheritance():
    """Test that subclasses can override specific capabilities."""

    class CustomBackend(MockBackendForTesting):
        def get_capability(self, capability: BackendCapability):
            if capability == BackendCapability.LABEL_FUNCTION:
                return "custom_single_label_support"
            return super().get_capability(capability)

    backend = CustomBackend()
    assert backend.get_capability(BackendCapability.LABEL_FUNCTION) == "custom_single_label_support"
    assert backend.get_capability(BackendCapability.SUPPORT_MULTIPLE_LABELS) is False
