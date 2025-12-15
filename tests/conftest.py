"""Pytest configuration for automatic test marking based on directory."""

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
    """Automatically mark tests based on their directory location."""
    for item in items:
        # Get the test file path relative to the tests directory
        test_path = str(item.fspath.relto(item.session.fspath))

        # Mark tests in the unit directory as unit tests
        if "tests/unit" in test_path:
            item.add_marker(pytest.mark.unit)

        # Mark tests in the integration directory as integration tests
        elif "tests/integration" in test_path:
            item.add_marker(pytest.mark.integration)

        # If no specific directory marker is found, default to unit
        else:
            item.add_marker(pytest.mark.unit)
