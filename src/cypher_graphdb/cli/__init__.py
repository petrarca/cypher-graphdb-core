"""Command-line interface (CLI) module for CypherGraphDB.

This package provides a comprehensive command-line interface for interacting
with graph databases through CypherGraphDB, including interactive queries,
data import/export, and graph management operations.

The full CLI requires the ``cli`` extra (typer, rich, prompt_toolkit, art);
reaching it without that extra raises a message naming the install.

``CypherGraphCLI`` is re-exported **lazily**. Individual modules here have
narrower needs than the whole application -- ``renderer`` wants only ``rich``,
for instance -- and an eager re-export would make ``import
cypher_graphdb.cli.renderer`` pull in prompt_toolkit and art as well. Importing
a leaf module should cost only what that leaf needs.
"""

from typing import TYPE_CHECKING

from cypher_graphdb._extras import require_extra

if TYPE_CHECKING:  # pragma: no cover - import-time typing only
    from .app import CypherGraphCLI

_CLI_MODULES = {"typer", "rich", "prompt_toolkit", "art", "click"}

__all__ = [
    "CypherGraphCLI",
]


def __getattr__(name: str) -> object:
    """Resolve ``CypherGraphCLI`` on first access (PEP 562).

    Args:
        name: Attribute being looked up on the package.

    Returns:
        The requested attribute.

    Raises:
        ModuleNotFoundError: If the ``cli`` extra is not installed.
        AttributeError: If the attribute is not part of the public surface.
    """
    if name == "CypherGraphCLI":
        try:
            from .app import CypherGraphCLI
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on install shape
            if exc.name in _CLI_MODULES:
                raise require_extra("cli", exc) from exc
            raise
        return CypherGraphCLI

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
