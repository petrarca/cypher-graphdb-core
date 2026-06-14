"""Pytest configuration for automatic test marking based on directory."""

import pathlib

import pytest

from cypher_graphdb.modelprovider import model_provider


@pytest.fixture
def cleanup_model_provider():
    """Clean up models registered during tests.

    Use this fixture in tests that register models to the global model_provider
    to ensure proper test isolation.

    Example:
        def test_my_model(cleanup_model_provider):
            @node()
            class MyModel(GraphNode):
                name: str
            # Model will be cleaned up after test
    """
    initial_labels = set(model_provider._models.keys())
    yield
    # Remove models added during the test
    current_labels = set(model_provider._models.keys())
    added_labels = current_labels - initial_labels
    for label in added_labels:
        model_provider.remove(model_provider.get(label))


def pytest_collection_modifyitems(items):
    """Auto-mark tests by directory: tests/integration -> integration, else unit.

    Lets `-m unit` / `-m integration` select tests without every file needing an
    explicit marker (matches the server repos' convention).
    """
    for item in items:
        parts = pathlib.Path(str(item.fspath)).parts
        if "integration" in parts:
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)
