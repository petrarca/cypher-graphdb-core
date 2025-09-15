"""Drop database graph command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DropDbgraphCommand(BaseCommand):
    """Command to drop an existing database graph."""

    command_name = "drop_dbgraph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[drop_dbgraph", tokens=["drop dbgraph"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the drop database graph command.

        Args:
            parsed_cmd: The parsed command containing graph name in args

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.drop_graph(parsed_cmd.args)
        return True
