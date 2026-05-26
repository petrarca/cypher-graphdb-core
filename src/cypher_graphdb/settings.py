"""CypherGraphDB library configuration using Pydantic Settings.

This module centralizes runtime configuration for the CypherGraphDB library.
Values can be provided via environment variables (preferred) or fall back to
the defaults below. A ``Settings`` instance is intended to be retrieved via
``get_settings`` which caches the object for reuse across the process.

Environment variables (default prefix ``CGDB_``): ``CGDB_BACKEND``,
``CGDB_CINFO``, ``CGDB_GRAPH``, ``CGDB_READ_ONLY``,
``CGDB_CREATE_GRAPH_IF_NOT_EXISTS``, ``CGDB_QUERY_TIMEOUT_S``.

For applications that need multiple independent graph databases in the same
process, ``Settings.with_prefix("CGDB_TS_")`` produces a Settings object
bound to a different env-var prefix (``CGDB_TS_BACKEND``, etc.). The default
prefix is ``CGDB_`` for backward compatibility.
"""

import os
from functools import lru_cache
from typing import Any

from pydantic import Field, computed_field, field_serializer
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict

# Suffixes mapped to Settings fields. The full env var name is the
# extension's ``env_prefix`` concatenated with one of these suffixes.
# This is the single source of truth: field definitions below and
# _PrefixedEnvSource both derive from it.
_ENV_SUFFIXES: dict[str, str] = {
    "backend": "BACKEND",
    "cinfo": "CINFO",
    "graph": "GRAPH",
    "read_only": "READ_ONLY",
    "create_graph": "CREATE_GRAPH_IF_NOT_EXISTS",
    "query_timeout_s": "QUERY_TIMEOUT_S",
}

DEFAULT_ENV_PREFIX = "CGDB_"


class _PrefixedEnvSource(EnvSettingsSource):
    """Env settings source that ignores ``validation_alias`` and reads a custom prefix.

    Used by :meth:`Settings.with_prefix` to populate a Settings instance from
    environment variables under a custom prefix without falling back to the
    default ``CGDB_*`` aliases declared on each Field.

    The source disables ``.env`` file loading -- multi-instance setups are
    expected to provide explicit environment variables per backend.
    """

    def __init__(self, settings_cls: type[BaseSettings], runtime_prefix: str) -> None:
        super().__init__(settings_cls)
        self._runtime_prefix = runtime_prefix.upper()

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        """Look up the env var as ``<prefix><SUFFIX>`` for the given field.

        Ignores ``validation_alias`` entirely. Returns the raw string value
        if present, or ``(None, field_name, False)`` if absent.

        Fields not in ``_ENV_SUFFIXES`` (e.g. computed fields) are skipped.
        """
        suffix = _ENV_SUFFIXES.get(field_name)
        if suffix is None:
            # Computed fields and any future non-env fields are skipped.
            return None, field_name, False
        env_name = f"{self._runtime_prefix}{suffix}"
        # Read env at call time (not construction time) -- consistent with
        # how EnvSettingsSource works and safe for test monkeypatching.
        env_upper = {k.upper(): v for k, v in os.environ.items()}
        value = env_upper.get(env_name)
        if value is not None:
            return value, field_name, False
        return None, field_name, False


class Settings(BaseSettings):
    """Runtime CypherGraphDB library settings.

    Attributes map to environment variables. The default prefix is ``CGDB_``
    so ``backend`` <- ``CGDB_BACKEND``. To use a different prefix (for
    multi-instance setups), construct via ``Settings.with_prefix("CGDB_TS_")``.

    These settings control the default behavior for CypherGraphDB connections
    and can be overridden at runtime when creating CypherGraphDB instances.
    """

    backend: str | None = Field(
        default=None,
        description="Default backend type (memgraph, age, etc.)",
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['backend']}",
    )
    cinfo: str | None = Field(
        default=None,
        description="Default connection information/DSN",
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['cinfo']}",
    )
    graph: str | None = Field(
        default=None,
        description="Default graph name",
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['graph']}",
    )
    read_only: bool = Field(
        default=False,
        description="Enable read-only mode (prevents write operations)",
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['read_only']}",
    )
    create_graph: bool = Field(
        default=False,
        description="Auto-create graph if it does not exist (AGE only)",
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['create_graph']}",
    )
    query_timeout_s: int | None = Field(
        default=30,
        description=(
            "Maximum seconds a single query may run before the backend cancels it. "
            "Default is 30s -- enough for any legitimate graph query while preventing "
            "runaway queries from exhausting the connection pool. "
            "Set to 0 or None to disable. "
            "AGE: sets PostgreSQL statement_timeout at connection time. "
            "Memgraph: sets database setting query.timeout after connecting."
        ),
        validation_alias=f"{DEFAULT_ENV_PREFIX}{_ENV_SUFFIXES['query_timeout_s']}",
    )

    @computed_field  # type: ignore[misc]
    @property
    def cinfo_sanitized(self) -> str | None:
        """Get sanitized connection info safe for logging."""
        return self._sanitize_cinfo(self.cinfo)

    def _sanitize_cinfo(self, cinfo: str | None) -> str | None:
        """Sanitize connection info for safe logging."""
        if not cinfo:
            return None

        from .utils.connection_utils import sanitize_connection_string_for_logging

        return sanitize_connection_string_for_logging(cinfo)

    @field_serializer("cinfo")
    def serialize_cinfo(self, _value: str | None) -> str | None:
        """Serialize cinfo as sanitized version for safe output."""
        return self.cinfo_sanitized

    def __repr__(self) -> str:
        """String representation with sanitized cinfo."""
        from .utils.settings_repr import safe_settings_repr

        return safe_settings_repr(self, field_sanitizers={"cinfo": lambda _: self.cinfo_sanitized})

    def __str__(self) -> str:
        """String representation with sanitized cinfo."""
        from .utils.settings_repr import safe_settings_str

        return safe_settings_str(self, field_sanitizers={"cinfo": lambda _: self.cinfo_sanitized})

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",  # Ignore unexpected env vars
        env_file=".env",  # Optional .env loading (if present)
        env_file_encoding="utf-8",
        populate_by_name=True,  # Allow construction by field name (used by with_prefix)
    )

    @classmethod
    def with_prefix(cls, env_prefix: str) -> Settings:
        """Build a Settings instance from environment variables with a custom prefix.

        This enables multi-instance setups where independent CypherGraphDB pools
        each read their own set of env vars. For example, with
        ``env_prefix="CGDB_TS_"`` the instance reads ``CGDB_TS_BACKEND``,
        ``CGDB_TS_CINFO``, ``CGDB_TS_GRAPH``, etc.

        Unlike the default ``Settings()`` constructor, this method does NOT
        fall back to the default ``CGDB_*`` env vars or the ``.env`` file -- it
        reads exclusively from env vars matching the given prefix. Pass
        ``Settings.with_prefix("CGDB_")`` to read the default prefix without
        consulting ``.env``.

        Args:
            env_prefix: Prefix to apply to env var names. Must end with ``_``
                by convention (not enforced). Pass ``""`` to read unprefixed
                env vars (``BACKEND``, ``CINFO``, ...).

        Returns:
            A Settings instance populated from env vars with the given prefix.
            Fields whose env var is not set use the field default.

        Examples:
            ```python
            # Default (reads CGDB_BACKEND, CGDB_GRAPH, ...; also .env)
            settings = Settings()

            # Scoped, env only -- no .env fallback, no default-prefix fallback
            ts_settings = Settings.with_prefix("CGDB_TS_")
            cg_settings = Settings.with_prefix("CGDB_CG_")
            ```
        """
        from pydantic_core import PydanticUndefinedType

        source = _PrefixedEnvSource(cls, env_prefix)
        env_data = source()
        # Build kwargs for only the regular (non-computed) fields that are in
        # _ENV_SUFFIXES. For each field:
        #   - use the env value if the source found one
        #   - use the declared default if it is defined (not PydanticUndefined)
        #   - leave absent fields out so pydantic uses its own default mechanism
        # Because we pass a value for every regular field as an init kwarg,
        # pydantic-settings' init_settings source (highest priority) wins over
        # the env/dotenv sources -- masking the default CGDB_* env vars and .env.
        full_kwargs: dict[str, Any] = {}
        for field_name in _ENV_SUFFIXES:
            if field_name in env_data:
                full_kwargs[field_name] = env_data[field_name]
            else:
                field_info = cls.model_fields.get(field_name)
                if field_info is not None and not isinstance(field_info.default, PydanticUndefinedType):
                    full_kwargs[field_name] = field_info.default
                else:
                    full_kwargs[field_name] = None
        return cls(_env_file=None, **full_kwargs)


@lru_cache
def get_settings() -> Settings:
    """Return the cached ``Settings`` instance.

    The first invocation reads environment variables / .env file; subsequent
    calls reuse the same object to ensure consistent config.

    Returns:
        Settings: The cached settings instance.
    """
    return Settings()


__all__ = ["DEFAULT_ENV_PREFIX", "Settings", "get_settings"]
