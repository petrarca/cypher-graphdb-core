"""Fail-fast connection guard for test safety.

Mirrors sonnet-server's database guard for the graph backend: when installed, a
predicate must approve every connection target (cinfo) before ``CypherGraphDB``
connects. Test harnesses install this to make it impossible to ever connect to
(and run destructive Cypher against) a developer/production graph database, even
if a connection param leaks. No-op in production (unset by default).
"""

from __future__ import annotations

from collections.abc import Callable

__all__ = [
    "ConnectionGuardError",
    "assert_connection_allowed",
    "clear_connection_guard",
    "install_connection_guard",
]

_guard: Callable[[str | None], bool] | None = None


class ConnectionGuardError(RuntimeError):
    """Raised when the installed connection guard rejects a connection target."""


def install_connection_guard(predicate: Callable[[str | None], bool]) -> None:
    """Install a guard that must approve every connection target (cinfo).

    ``predicate(cinfo) -> bool``: return True to allow connecting to ``cinfo``,
    False to refuse (raises :class:`ConnectionGuardError`). Test suites install
    this to forbid the developer/production graph DB.
    """
    global _guard
    _guard = predicate


def clear_connection_guard() -> None:
    """Remove the installed connection guard (test teardown)."""
    global _guard
    _guard = None


def assert_connection_allowed(cinfo: str | None) -> None:
    """Raise :class:`ConnectionGuardError` if the installed guard rejects ``cinfo``."""
    if _guard is not None and not _guard(cinfo):
        raise ConnectionGuardError(
            f"Refusing to connect to graph backend '{cinfo}': "
            f"rejected by the installed connection guard (see install_connection_guard)."
        )
