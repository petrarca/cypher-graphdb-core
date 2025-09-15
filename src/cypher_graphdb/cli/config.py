"""Contains classes to manage runtime configuration for the cypher-graphdb CLI."""

from collections.abc import Callable
from typing import Any

from .provider import ConfigProvider


class CLIConfig(ConfigProvider):
    """Contains configuration for the cypher-graphdb CLI. Those can be modified during the runtime."""

    def __init__(self) -> None:
        self._properties: dict[str, Any] = {}

        self.on_property_change: Callable[[str, Any], bool] = lambda name, value: True

    def get_props(self) -> dict[str, Any]:
        return tuple(self._properties.keys())

    def apply_config(self, args: list[str], kwargs: dict[str, Any]) -> dict[str, Any]:
        result = self.get_properties(args) if not kwargs or args else {}

        if len(kwargs) > 0:
            result.update(self.set_properties(kwargs))

        return result

    def get_property(self, name: str, default: Any = None) -> dict[str, Any]:
        return self._properties.get(name, default)

    def get_properties(self, args: str | dict | tuple | list) -> dict[str, Any]:
        if not args:
            args = []
        elif not isinstance(args, dict | tuple | list):
            args = [args]

        if len(args) == 0:
            return self._properties
        else:
            return {name: self._properties.get(name) for name in args}

    def set_property(self, name: str, value: Any) -> dict[str, Any]:
        item = {}
        if self.on_property_change(name, value):
            item = {name: value}
            self._properties.update(item)

        return item

    def set_properties(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        result = {}

        for name, value in kwargs.items():
            if item := self.set_property(name, value):
                result.update(item)

        return result
