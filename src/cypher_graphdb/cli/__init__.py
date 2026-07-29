"""Command-line interface (CLI) module for CypherGraphDB.

This package provides a comprehensive command-line interface for interacting
with graph databases through CypherGraphDB, including interactive queries,
data import/export, and graph management operations.

The CLI requires the ``cli`` extra (typer, rich, prompt_toolkit, art);
importing this package without it raises a message naming the install.
"""

from cypher_graphdb._extras import require_extra

try:
    from .app import CypherGraphCLI
except ModuleNotFoundError as exc:  # pragma: no cover - depends on install shape
    if exc.name in {"typer", "rich", "prompt_toolkit", "art", "click"}:
        raise require_extra("cli", exc) from exc
    raise

__all__ = [
    "CypherGraphCLI",
]
