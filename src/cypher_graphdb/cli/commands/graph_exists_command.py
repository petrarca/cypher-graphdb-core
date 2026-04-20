"""Database graph exists command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class GraphExistsCommand(BaseCommand):
    """Command to check if a graph exists in the database."""

    command_name = "graph_exists"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[graph_exists", tokens=["graph exists"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the database graph exists command.

        Args:
            parsed_cmd: The parsed command containing graph name in args

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.graph_exists(parsed_cmd.args)
        return True
