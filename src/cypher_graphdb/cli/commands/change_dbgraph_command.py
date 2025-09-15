"""Change database graph command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ChangeDbgraphCommand(BaseCommand):
    """Command to change the active database graph."""

    command_name = "change_dbgraph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[change_dbgraph", tokens=["change dbgraph"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the change database graph command.

        Args:
            parsed_cmd: The parsed command containing graph name in args

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.change_graph(parsed_cmd.args)
        return True
