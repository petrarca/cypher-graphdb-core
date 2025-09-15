"""Pytest configuration for automatic test marking based on directory."""

import pytest


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
