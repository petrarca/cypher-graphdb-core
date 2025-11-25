"""backendprovider module: Registry and loader for CypherBackend implementations.

Provides BackendProvider for registering and resolving backends by name, and the `@backend` decorator.
"""

import collections.abc
import importlib
import threading
from collections.abc import Iterator
from typing import Final

from loguru import logger

from . import utils
from .backend import CypherBackend


class BackendProvider(collections.abc.Collection):
    """Maintain a registry of backend classes/instances and resolve them by name."""

    def __init__(self) -> None:
        """Initialize an empty backend registry."""
        self._backends: dict[str, CypherBackend | type[CypherBackend]] = {}
        self._lock = threading.RLock()

    def register(self, name: str, cls: CypherBackend) -> None:
        """Register a backend class under the given name.

        Args:
            name: Unique identifier for the backend (case-insensitive).
            cls: CypherBackend subclass to register.

        """
        if not issubclass(cls, CypherBackend):
            raise RuntimeError(f"Can only register CypherBackend models. {cls.__name__} is not.")

        self._backends[name.casefold()] = cls

    def check_and_resolve(self, backend_ref: CypherBackend | str, raise_error=False) -> CypherBackend:
        """Validate and resolve a backend reference to an instantiated backend.

        Args:
            backend_ref: Either a CypherBackend instance or a backend name string.
            raise_error: If True, raise an error on invalid reference.

        Returns:
            A CypherBackend instance, or None if not found and raise_error=False.

        """
        if isinstance(backend_ref, CypherBackend):
            return backend_ref

        failure = None
        if not backend_ref:
            failure = "No backend specified!"
        elif not isinstance(backend_ref, str):
            failure = "Backend type must be a valid string!"

        if failure:
            if raise_error:
                raise RuntimeError(failure)
            return None

        if not (result := backend_provider.resolve(backend_ref)) and raise_error:
            raise RuntimeError(f"Invalid backend: '{backend_ref}'.")

        return result

    def get(self, name: str) -> type[CypherBackend] | CypherBackend | None:
        """Retrieve a registered backend class or instance by name without instantiation.

        Args:
            name: Identifier of the backend (case-insensitive).

        Returns:
            The backend class or instance, or None if not registered.

        """
        return self._backends.get(name.casefold(), None)

    def resolve(self, name: str) -> CypherBackend:
        """Load and instantiate the backend for the given name.

        Args:
            name: Identifier of the backend to resolve (case-insensitive).

        Returns:
            An instantiated CypherBackend, or None if resolution fails.

        """
        with self._lock:
            normalized_name = name.casefold()

            for phase in (0, 1):
                result = self._backends.get(normalized_name, None)
                if result:
                    break

                # try to load module in first phase, if load failed, give up
                if not phase and not self._try_to_load_backend(name):
                    break

            # Instantiate only if result is a backend class (not an instance already)
            if result and isinstance(result, type) and issubclass(result, CypherBackend):
                result = result()
                # replace class with singleton instance
                self._backends[normalized_name] = result

            return result

    def items(self) -> dict[str, CypherBackend | type[CypherBackend]]:
        """Return a snapshot of the registry mapping names to backends.

        Returns:
            A dictionary of backend names to classes or instances.

        """
        return dict(self._backends.items())

    def _try_to_load_backend(self, name: str) -> bool:
        name = name.lower()

        # Load from the backend directory structure
        module_name = f"cypher_graphdb.backends.{name}"
        logger.debug(f"Try to load backend module from {module_name}")

        try:
            module = importlib.import_module(module_name)
            logger.debug(f"Backend module successfully loaded: {module}")
            logger.debug(f"Module dir: {dir(module)}")
            return True
        except ModuleNotFoundError as e:
            logger.debug(f"Backend module not found in {module_name}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Error loading backend module {module_name}: {e}")
            return False

    def __contains__(self, x: object) -> bool:
        if isinstance(x, str):
            return x.casefold() in self._backends
        return x in self._backends

    def __len__(self) -> int:
        return len(self._backends)

    def __iter__(self) -> Iterator[tuple[str, CypherBackend]]:
        return iter(self._backends)

    @property
    def __dict__(self) -> dict[str, CypherBackend | str]:
        """Return registry state as a serializable dict for introspection."""
        return {
            name: utils.to_collection(backend) if isinstance(backend, CypherBackend) else backend
            for name, backend in self._backends.items()
        }


# pylint: disable=C0103
backend_provider: Final[BackendProvider] = BackendProvider()
