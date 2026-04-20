"""Drop database graph command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DropGraphCommand(BaseCommand):
    """Command to drop a graph from the database."""

    command_name = "drop_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[drop_graph", tokens=["drop graph"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the drop database graph command.

        Args:
            parsed_cmd: The parsed command containing graph name in args

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.drop_graph(parsed_cmd.args)
        return True
