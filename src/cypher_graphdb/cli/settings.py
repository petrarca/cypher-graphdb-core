"""CLI-specific settings for the cypher-graphdb command-line interface.

Extends the core Settings with CLI-only options. The core Settings class
remains clean and consumer-agnostic; only CLI concerns live here.

Environment variables (CLI-specific, in addition to core CGDB_* vars):
    CGDB_MODEL_PATH  -- Path to Python model file/directory for auto-loading.
    CGDB_LOG_LEVEL   -- Log level for the CLI (INFO, DEBUG, TRACE, ...).

Per-invocation options (not env-configurable by design -- too dangerous as defaults):
    execute, file, verbose, no_progress, yes, json, table, ignore_model_path
    These are set programmatically from CLI args in args.py.
"""

from functools import lru_cache

from pydantic import Field

from cypher_graphdb.settings import Settings


class CLISettings(Settings):
    """CLI-specific settings.

    Inherits all connection settings from core Settings (CGDB_BACKEND,
    CGDB_CINFO, CGDB_GRAPH, etc.) and adds CLI-only fields.

    Env-configurable CLI fields:
        model_path:  Auto-load Python models on startup (CGDB_MODEL_PATH).
        log_level:   Log verbosity for the CLI session (CGDB_LOG_LEVEL).

    Per-invocation fields (set from CLI args, not env vars):
        execute:     Command string from --execute / -e.
        file:        File path from --file / -f.
        json_format: Render output as JSON (--json / -j).
        table_format: Render output as table (--table / -t).
        verbose:     Show each command in file mode (--verbose).
        no_progress: Disable progress bar in file mode (--no-progress).
        yes:         Auto-confirm all operations (--yes / -y).
        ignore_model_path: Ignore CGDB_MODEL_PATH env var (--ignore-model-path).
    """

    # Env-configurable
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

    # Per-invocation (set from CLI args, not from env)
    execute: str | None = Field(default=None, description="Command to execute (-e)")
    file: str | None = Field(default=None, description="File to execute (-f)")
    json_format: bool = Field(default=False, description="Render output as JSON")
    table_format: bool = Field(default=False, description="Render output as table")
    verbose: bool = Field(default=False, description="Show each command in file mode")
    no_progress: bool = Field(default=False, description="Disable progress bar in file mode")
    yes: bool = Field(default=False, description="Auto-confirm all operations")
    ignore_model_path: bool = Field(default=False, description="Ignore CGDB_MODEL_PATH env var")


@lru_cache
def get_cli_settings() -> CLISettings:
    """Return the cached CLISettings instance.

    Connection and env-configurable fields are read from environment / .env.
    Per-invocation fields are set programmatically in args.py before this
    cache is populated.
    """
    return CLISettings()


__all__ = ["CLISettings", "get_cli_settings"]
