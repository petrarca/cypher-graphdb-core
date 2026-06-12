"""Unit tests for the fail-fast connection guard."""

import pytest

from cypher_graphdb.connection_guard import (
    ConnectionGuardError,
    assert_connection_allowed,
    clear_connection_guard,
    install_connection_guard,
)


@pytest.fixture(autouse=True)
def _clear_guard():
    clear_connection_guard()
    yield
    clear_connection_guard()


def test_no_guard_allows_any_target():
    # No guard installed -> any cinfo passes (no raise).
    assert_connection_allowed("bolt://localhost:7687")
    assert_connection_allowed(None)


def test_guard_allows_approved_target():
    install_connection_guard(lambda cinfo: bool(cinfo) and "test-host" in cinfo)
    assert_connection_allowed("bolt://test-host:7687")  # no raise


def test_guard_refuses_rejected_target():
    install_connection_guard(lambda cinfo: bool(cinfo) and "test-host" in cinfo)
    with pytest.raises(ConnectionGuardError):
        assert_connection_allowed("bolt://localhost:7687")


def test_guard_refuses_none_when_predicate_requires_value():
    install_connection_guard(lambda cinfo: bool(cinfo))
    with pytest.raises(ConnectionGuardError):
        assert_connection_allowed(None)


def test_clear_restores_default():
    install_connection_guard(lambda cinfo: False)
    with pytest.raises(ConnectionGuardError):
        assert_connection_allowed("bolt://test-host:7687")
    clear_connection_guard()
    assert_connection_allowed("bolt://test-host:7687")  # no raise
