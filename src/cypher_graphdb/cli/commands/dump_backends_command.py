"""Dump backends command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb import utils
from cypher_graphdb.backendprovider import backend_provider
from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpBackendsCommand(BaseCommand):
    """Command to list available backends."""

    command_name = "dump_backends"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_backends", tokens=["backends"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize the dump backends command.

        Args:
            runtime: CLI runtime providing access to processing
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the dump backends command.

        Lists all available backends or details for a specific backend.

        Args:
            parsed_cmd: Parsed command with optional backend name argument

        Returns:
            bool: True if command executed successfully
        """
        if not parsed_cmd.args:
            # List all backends
            result = vars(backend_provider)
            render_kwargs = {"key_name": "Backend", "value_name": "Definition"}
        else:
            # Show specific backend details
            backend_name = parsed_cmd.get_arg(0)
            result = backend_provider.get(backend_name) if backend_name and isinstance(backend_name, str) else None
            if result:
                # Convert to dictionary format for rendering
                result = utils.to_collection(result)

            render_kwargs = {"key_name": "Item"} if result else None

        return self._post_processing_cmd(parsed_cmd, result, False, render_kwargs=render_kwargs)
