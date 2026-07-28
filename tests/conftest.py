"""Pytest configuration for automatic test marking based on directory."""

import pathlib

import pytest

from cypher_graphdb import ModelProvider


@pytest.fixture
def mp() -> ModelProvider:
    """A fresh, isolated ModelProvider for one test.

    There is no global provider, so every test that registers models simply
    declares its own. Isolation is structural -- nothing leaks between tests and
    no cleanup is required.

    Example:
        def test_my_model(mp):
            @mp.node()
            class MyModel(GraphNode):
                name: str
    """
    return ModelProvider()


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
