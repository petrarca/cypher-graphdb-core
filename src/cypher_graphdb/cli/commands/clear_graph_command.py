"""Clear graph command implementation."""

from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ClearGraphCommand(BaseCommand):
    """Command to clear the current graph."""

    command_name = "clear_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[clear_graph]]_", tokens=["clear"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize the clear graph command.

        Args:
            runtime: CLI runtime providing access to graph data
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the clear graph command.

        Clears all nodes and edges from the current graph.

        Args:
            parsed_cmd: Parsed command (not used but required for interface)

        Returns:
            bool: True if command executed successfully
        """
        self.graph_data.graph.clear()
        rich.print("[green]Graph cleared.")
        return True
