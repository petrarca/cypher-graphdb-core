"""CLI-specific settings for the cypher-graphdb command-line interface.

Extends the core Settings with CLI-only options. The core Settings class
remains clean and consumer-agnostic; only CLI concerns live here.

Environment variables (in addition to core CGDB_* vars):
    CGDB_MODEL_PATH  -- Path to Python model file/directory for auto-loading.
    CGDB_LOG_LEVEL   -- Log level for the CLI (INFO, DEBUG, TRACE, ...).
"""

from functools import lru_cache

from pydantic import Field

from cypher_graphdb.settings import Settings


class CLISettings(Settings):
    """CLI-specific settings.

    Inherits all connection settings from core Settings (CGDB_BACKEND,
    CGDB_CINFO, CGDB_GRAPH, etc.) and adds CLI-only fields.

    CLI fields:
        model_path: Auto-load Python models on startup.
        log_level:  Log verbosity for the CLI session.
    """

    model_path: str | None = Field(
        default=None,
        description="Path to Python model file or directory to auto-load on startup",
        validation_alias="CGDB_MODEL_PATH",
    )
    log_level: str = Field(
        default="INFO",
        description="Log level for the CLI (INFO, DEBUG, TRACE, ...)",
        validation_alias="CGDB_LOG_LEVEL",
    )


@lru_cache
def get_cli_settings() -> CLISettings:
    """Return the cached CLISettings instance."""
    return CLISettings()


__all__ = ["CLISettings", "get_cli_settings"]
