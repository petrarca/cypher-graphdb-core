"""Command registry for CLI commands.

This module provides a centralized registry where commands can self-register
when they are imported. This allows for a more dynamic and extensible
command system.

Commands that have a command_name class attribute will be automatically
registered when their module is imported.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import BaseCommand


class CommandRegistry:
    """Registry for CLI commands that allows self-registration."""

    def __init__(self) -> None:
        """Initialize the command registry."""
        self._commands: dict[str, type[BaseCommand]] = {}

    def register(self, command_class: type[BaseCommand], command_name: str | None = None) -> None:
        """Register a command class with a given name.

        Args:
            command_class: The command class to register
            command_name: Optional name to register the command under.
                         If not provided, uses the class's command_name
                         attribute.

        Raises:
            ValueError: If no command name is available or if command name
                       is already registered
        """
        # Use provided name or fall back to class attribute
        name = command_name or getattr(command_class, "command_name", None)
        if name is None:
            raise ValueError(
                f"No command name available for {command_class.__name__}. "
                "Either provide command_name parameter or set command_name "
                "class attribute."
            )

        if name in self._commands:
            raise ValueError(f"Command '{name}' is already registered")

        self._commands[name] = command_class

    def get_command_class(self, command_name: str) -> type[BaseCommand] | None:
        """Get a command class by name.

        Args:
            command_name: The name of the command to retrieve

        Returns:
            The command class if found, None otherwise
        """
        return self._commands.get(command_name)

    def get_all_commands(self) -> dict[str, type[BaseCommand]]:
        """Get all registered commands.

        Returns:
            A dictionary mapping command names to command classes
        """
        return self._commands.copy()

    def is_registered(self, command_name: str) -> bool:
        """Check if a command is registered.

        Args:
            command_name: The name of the command to check

        Returns:
            True if the command is registered, False otherwise
        """
        return command_name in self._commands

    def unregister(self, command_name: str) -> bool:
        """Unregister a command.

        Args:
            command_name: The name of the command to unregister

        Returns:
            True if the command was unregistered, False if it wasn't registered
        """
        if command_name in self._commands:
            del self._commands[command_name]
            return True
        return False

    def clear(self) -> None:
        """Clear all registered commands."""
        self._commands.clear()


# Global registry instance
registry = CommandRegistry()
