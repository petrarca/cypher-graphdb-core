"""Command-line interface (CLI) module for CypherGraphDB.

This package provides a comprehensive command-line interface for interacting
with graph databases through CypherGraphDB, including interactive queries,
data import/export, and graph management operations.
"""

from .app import CypherGraphCLI

__all__ = [
    "CypherGraphCLI",
]
