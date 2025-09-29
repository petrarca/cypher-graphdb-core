"""CypherGraphDB library configuration using Pydantic Settings.

This module centralizes runtime configuration for the CypherGraphDB library.
Values can be provided via environment variables (preferred) or fall back to
the defaults below. A ``Settings`` instance is intended to be retrieved via
``get_settings`` which caches the object for reuse across the process.

Environment variables: ``CGDB_BACKEND``, ``CGDB_CINFO``, ``CGDB_GRAPH``.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime CypherGraphDB library settings.

    Attributes map directly to environment variables without any prefix.
    For example, ``backend`` <- ``CGDB_BACKEND``.

    These settings control the default behavior for CypherGraphDB connections
    and can be overridden at runtime when creating CypherGraphDB instances.
    """

    backend: str | None = Field(
        default=None,
        description="Default backend type (memgraph, age, etc.)",
        validation_alias="CGDB_BACKEND",
    )
    cinfo: str | None = Field(
        default=None,
        description="Default connection information/DSN",
        validation_alias="CGDB_CINFO",
    )
    graph: str | None = Field(
        default=None,
        description="Default graph name",
        validation_alias="CGDB_GRAPH",
    )

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",  # Ignore unexpected env vars
        env_file=".env",  # Optional .env loading (if present)
        env_file_encoding="utf-8",
    )


@lru_cache
def get_settings() -> Settings:
    """Return the cached ``Settings`` instance.

    The first invocation reads environment variables / .env file; subsequent
    calls reuse the same object to ensure consistent config.

    Returns:
        Settings: The cached settings instance.
    """
    return Settings()


__all__ = ["Settings", "get_settings"]
