"""Command instance manager for CLI commands.

This module provides a manager that handles instantiation and lifecycle
of command instances, working in conjunction with the command registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import BaseCommand
    from cypher_graphdb.cli.runtime import CLIRuntime


class CommandInstanceManager:
    """Manages command instances for the CLI.

    This class works with the command registry to provide a single
    source of truth for command instances, eliminating the need for
    manual instance management in the CLI app.
    """

    def __init__(self, cli_runtime: CLIRuntime) -> None:
        """Initialize the command instance manager.

        Args:
            cli_runtime: The CLI runtime to pass to command constructors
        """
        self._cli_runtime = cli_runtime
        self._instances: dict[str, BaseCommand] = {}

    def get_instance(self, command_name: str) -> BaseCommand | None:
        """Get a command instance by name.

        Args:
            command_name: The name of the command

        Returns:
            The command instance if found, None otherwise
        """
        if command_name not in self._instances:
            self._create_instance(command_name)

        return self._instances.get(command_name)

    def _create_instance(self, command_name: str) -> None:
        """Create and cache a command instance.

        Args:
            command_name: The name of the command to create
        """
        # Import here to avoid circular imports
        from cypher_graphdb.cli.command_registry import registry

        command_class = registry.get_command_class(command_name)
        if command_class is None:
            return

        # Handle special cases that don't need CLI runtime
        instance = command_class() if command_name == "exit" else command_class(self._cli_runtime)

        self._instances[command_name] = instance

    def get_all_instances(self) -> dict[str, BaseCommand]:
        """Get all command instances.

        Returns:
            Dictionary mapping command names to instances
        """
        # Ensure all registered commands have instances
        from cypher_graphdb.cli.command_registry import registry

        for command_name in registry.get_all_commands():
            if command_name not in self._instances:
                self._create_instance(command_name)

        return self._instances.copy()

    def clear_instances(self) -> None:
        """Clear all cached instances."""
        self._instances.clear()

    def has_instance(self, command_name: str) -> bool:
        """Check if an instance exists for a command.

        Args:
            command_name: The name of the command to check

        Returns:
            True if instance exists, False otherwise
        """
        return command_name in self._instances
