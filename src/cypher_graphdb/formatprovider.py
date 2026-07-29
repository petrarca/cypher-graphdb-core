"""Format registry for tabular and hierarchical import/export.

Provides :class:`FormatProvider` for registering and resolving data formats by
name or by file extension, and the :func:`data_format` decorator.

This mirrors :mod:`cypher_graphdb.backendprovider`: a process-global registry
with lazy, convention-based loading. A format is global in the way a backend is
-- ``"csv"`` means the same thing everywhere -- unlike graph models, which are
per-graph and therefore live on an explicit ``ModelProvider`` (see
``docs/adr/0001-explicit-model-providers.md``).

Formats whose dependency is an optional extra declare it via ``extra=``; the
registry then turns a missing package into a message naming the install rather
than a bare ``ModuleNotFoundError``.

Registering your own format::

    from cypher_graphdb import BaseExporter, BaseImporter, data_format

    @data_format("parquet", extensions=(".parquet",), extra=None)
    class ParquetFormat:
        importer = MyParquetImporter
        exporter = MyParquetExporter
"""

from __future__ import annotations

import collections.abc
import importlib
import threading
from typing import TYPE_CHECKING, Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, model_serializer

from cypher_graphdb._extras import require_extra

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterator

# Built-in formats live at cypher_graphdb.tools.formats.<name>
_FORMAT_MODULE_ROOT = "cypher_graphdb.tools.formats"


class FormatSpec(BaseModel):
    """A registered data format.

    Attributes:
        name: Canonical format name (lower-case), e.g. ``"excel"``.
        extensions: File extensions that resolve to this format, each including
            the leading dot and lower-case, e.g. ``(".xlsx", ".xls")``.
        importer: Importer class, or None if the format is export-only.
        exporter: Exporter class, or None if the format is import-only.
        extra: Optional-dependency group providing this format, or None when it
            needs nothing beyond the core install.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    extensions: tuple[str, ...] = ()
    importer: type | None = None
    exporter: type | None = None
    extra: str | None = None

    @model_serializer
    def serialize_model(self, _) -> dict[str, Any]:
        """Convert FormatSpec to a serializable dictionary.

        Importer and exporter are rendered as class names, since a class object
        is not portable across a serialization boundary.
        """
        return {
            "name": self.name,
            "extensions": list(self.extensions),
            "importer": self.importer.__name__ if self.importer else None,
            "exporter": self.exporter.__name__ if self.exporter else None,
            "extra": self.extra,
        }

    def supports_import(self) -> bool:
        """Return True if this format can be imported."""
        return self.importer is not None

    def supports_export(self) -> bool:
        """Return True if this format can be exported."""
        return self.exporter is not None


class FormatAvailability(BaseModel):
    """Whether a format can actually be used in this installation.

    Answers "can I use this?" without raising, so a caller can list formats,
    pick a fallback, or explain the situation to a user.

    Attributes:
        name: The format asked about.
        available: True if the format is registered and its dependencies import.
        registered: True if the format is known at all (an unknown name is not
            the same problem as a missing dependency).
        reason: Human-readable explanation when unavailable, else None.
        extra: Optional-dependency group that would provide it, if any.
        missing_package: The package that failed to import, if that was the cause.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    available: bool
    registered: bool = False
    reason: str | None = None
    extra: str | None = None
    missing_package: str | None = None

    def __bool__(self) -> bool:
        """Allow ``if provider.probe("excel"):``."""
        return self.available


class FormatProvider(collections.abc.Collection):
    """Maintain a registry of data formats and resolve them by name or extension."""

    def __init__(self) -> None:
        """Initialize an empty format registry."""
        self._by_name: dict[str, FormatSpec] = {}
        self._by_extension: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        *,
        extensions: tuple[str, ...] | list[str] = (),
        importer: type | None = None,
        exporter: type | None = None,
        extra: str | None = None,
    ) -> FormatSpec:
        """Register a format under the given name.

        Re-registering a name replaces the previous entry, which lets a consumer
        override a built-in format with its own implementation.

        Args:
            name: Format identifier (case-insensitive), e.g. ``"csv"``.
            extensions: File extensions mapping to this format. A missing
                leading dot is added, and case is normalized.
            importer: Importer class, or None for an export-only format.
            exporter: Exporter class, or None for an import-only format.
            extra: Optional-dependency group required by this format.

        Returns:
            The registered FormatSpec.

        Raises:
            ValueError: If the name is empty, or neither importer nor exporter
                is supplied.
        """
        key = name.strip().casefold()
        if not key:
            raise ValueError("Format name must not be empty")
        if importer is None and exporter is None:
            raise ValueError(f"Format {name!r} must supply an importer, an exporter, or both")

        normalized = tuple(self._normalize_extension(ext) for ext in extensions)
        spec = FormatSpec(name=key, extensions=normalized, importer=importer, exporter=exporter, extra=extra)

        with self._lock:
            previous = self._by_name.get(key)
            if previous is not None:
                # Drop extensions the previous registration owned, so an
                # override does not leave stale extension mappings behind.
                for ext in previous.extensions:
                    if self._by_extension.get(ext) == key:
                        del self._by_extension[ext]

            self._by_name[key] = spec
            for ext in normalized:
                self._by_extension[ext] = key

        logger.debug("Registered format '{}' (extensions={}, extra={})", key, normalized, extra)
        return spec

    def get(self, name: str) -> FormatSpec | None:
        """Return a registered format without attempting to load it.

        Args:
            name: Format name (case-insensitive).

        Returns:
            The FormatSpec, or None if not registered.
        """
        with self._lock:
            return self._by_name.get(name.strip().casefold())

    def resolve(self, name: str) -> FormatSpec:
        """Resolve a format by name, loading its module on first use.

        Args:
            name: Format name (case-insensitive).

        Returns:
            The resolved FormatSpec.

        Raises:
            ValueError: If no such format is registered or discoverable.
            ModuleNotFoundError: If the format's optional extra is not installed.
        """
        key = name.strip().casefold()
        spec = self.get(key)
        if spec is not None:
            return spec

        self._try_to_load_format(key)

        spec = self.get(key)
        if spec is None:
            known = ", ".join(sorted(self._by_name)) or "none"
            raise ValueError(f"Unknown format: {name!r} (registered: {known})")
        return spec

    def resolve_for_file(self, filename: str) -> FormatSpec | None:
        """Resolve the format of a file from its extension.

        Args:
            filename: File name, path, or glob pattern.

        Returns:
            The FormatSpec, or None if the extension maps to no known format.
        """
        name = self.format_name_for_file(filename)
        return self.resolve(name) if name else None

    def format_name_for_file(self, filename: str) -> str | None:
        """Return the format name a file extension maps to.

        This is metadata only: naming the format of ``report.xlsx`` never
        imports anything and never raises, so it works even when the format's
        optional extra is absent. Use :meth:`probe` to ask whether it is usable,
        or :meth:`resolve` to actually get the implementation.

        Args:
            filename: File name, path, or glob pattern.

        Returns:
            The format name, or None if the extension is unknown.
        """
        if not filename:
            return None

        ext = self._extension_of(filename)
        if not ext:
            return None

        with self._lock:
            name = self._by_extension.get(ext)
        if name is not None:
            return name

        # The format may not be loaded yet. Discovery is by name, so the
        # built-in extension table answers without importing anything.
        return _BUILTIN_EXTENSIONS.get(ext)

    def probe(self, name: str) -> FormatAvailability:
        """Report whether a format is usable, and why not when it isn't.

        Never raises: this is the non-throwing counterpart to :meth:`resolve`,
        for listing formats or choosing a fallback.

        Args:
            name: Format name (case-insensitive).

        Returns:
            A FormatAvailability describing the outcome.
        """
        key = name.strip().casefold()

        if self.get(key) is not None:
            return FormatAvailability(name=key, available=True, registered=True)

        extra = _BUILTIN_EXTRAS.get(key)
        try:
            loaded = self._try_to_load_format(key)
        except ModuleNotFoundError as exc:
            # _try_to_load_format re-raises through require_extra, whose message
            # carries the install command but not the failing module name; the
            # original import error is kept as __cause__.
            cause = exc.__cause__
            missing = exc.name or (cause.name if isinstance(cause, ModuleNotFoundError) else None)
            missing_text = missing or "an optional dependency"
            return FormatAvailability(
                name=key,
                available=False,
                registered=True,
                reason=(f"Format {key!r} needs the '{extra}' extra, which is not installed (missing: {missing_text})."),
                extra=extra,
                missing_package=missing,
            )

        if loaded and self.get(key) is not None:
            return FormatAvailability(name=key, available=True, registered=True)

        known = ", ".join(sorted(self._by_name)) or "none"
        return FormatAvailability(
            name=key,
            available=False,
            registered=False,
            reason=f"Unknown format {key!r} (registered: {known}).",
        )

    def probe_all(self) -> dict[str, FormatAvailability]:
        """Probe every built-in and already-registered format.

        Returns:
            Mapping of format name to its availability.
        """
        names = set(_BUILTIN_EXTENSIONS.values()) | set(self.items())
        return {name: self.probe(name) for name in sorted(names)}

    def extensions(self) -> dict[str, str]:
        """Return a copy of the extension-to-format mapping."""
        with self._lock:
            return dict(self._by_extension)

    def items(self) -> dict[str, FormatSpec]:
        """Return a copy of the registered formats keyed by name."""
        with self._lock:
            return dict(self._by_name)

    def load_builtins(self) -> None:
        """Eagerly load every built-in format, ignoring missing extras.

        Useful for listing what this installation can actually handle.
        """
        for name in sorted(set(_BUILTIN_EXTENSIONS.values())):
            try:
                self._try_to_load_format(name)
            except ModuleNotFoundError:  # noqa: PERF203 - a missing extra is expected
                logger.debug("Built-in format '{}' unavailable (optional extra not installed)", name)

    def _try_to_load_format(self, name: str) -> bool:
        """Import the module that registers ``name``.

        Args:
            name: Format name.

        Returns:
            True if the module was imported.

        Raises:
            ModuleNotFoundError: If the format's optional extra is missing.
        """
        module_name = f"{_FORMAT_MODULE_ROOT}.{name}"
        logger.debug("Try to load format module {}", module_name)
        try:
            importlib.import_module(module_name)
            return True
        except ModuleNotFoundError as exc:
            # The format module itself being absent means "unknown format" and
            # is reported by resolve(). A dependency of it being absent means an
            # extra is missing, which deserves the actionable message.
            if exc.name and exc.name != module_name and not module_name.startswith(str(exc.name)):
                extra = _BUILTIN_EXTRAS.get(name)
                if extra:
                    raise require_extra(extra, exc) from exc
                raise
            logger.debug("No format module {} ({})", module_name, exc)
            return False

    @staticmethod
    def _normalize_extension(ext: str) -> str:
        """Normalize a file extension to lower-case with a leading dot."""
        ext = ext.strip().casefold()
        return ext if ext.startswith(".") else f".{ext}"

    @staticmethod
    def _extension_of(filename: str) -> str | None:
        """Extract a lower-case extension from a path or glob pattern."""
        tail = filename.rsplit("/", 1)[-1]
        if "." not in tail:
            return None
        return f".{tail.rsplit('.', 1)[-1]}".casefold()

    def __contains__(self, x: object) -> bool:
        """Return True if a format with this name is registered."""
        if not isinstance(x, str):
            return False
        return x.strip().casefold() in self._by_name

    def __len__(self) -> int:
        """Return the number of registered formats."""
        return len(self._by_name)

    def __iter__(self) -> Iterator[tuple[str, FormatSpec]]:
        """Iterate over (name, spec) pairs."""
        return iter(self.items().items())


# Extension map for formats that ship with the library. Discovery is by name, so
# this is what lets an extension resolve before its module has been imported.
_BUILTIN_EXTENSIONS: dict[str, str] = {
    ".xlsx": "excel",
    ".xls": "excel",
    ".csv": "csv",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
}

# Built-in formats whose dependency lives in an optional extra.
_BUILTIN_EXTRAS: dict[str, str] = {
    "excel": "excel",
}

format_provider = FormatProvider()


def data_format(
    name: str,
    *,
    extensions: tuple[str, ...] | list[str] = (),
    extra: str | None = None,
    provider: FormatProvider | None = None,
) -> Any:
    """Register a format class on a provider.

    The decorated class supplies ``importer`` and/or ``exporter`` attributes.

    Args:
        name: Format identifier, e.g. ``"parquet"``.
        extensions: File extensions mapping to this format.
        extra: Optional-dependency group required by this format.
        provider: Provider to register on; defaults to the global one.

    Returns:
        The class decorator.
    """

    def decorator(cls: type) -> type:
        target = provider if provider is not None else format_provider
        target.register(
            name,
            extensions=extensions,
            importer=getattr(cls, "importer", None),
            exporter=getattr(cls, "exporter", None),
            extra=extra,
        )
        return cls

    return decorator


__all__ = ["FormatProvider", "FormatSpec", "data_format", "format_provider"]
