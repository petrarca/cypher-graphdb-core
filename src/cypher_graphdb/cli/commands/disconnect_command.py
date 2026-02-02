"""Disconnect command implementation for database disconnection."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DisconnectCommand(BaseCommand):
    """Command to disconnect from a database."""

    command_name = "disconnect"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[disconnect", tokens=["disconnect"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the disconnect command.

        Args:
            parsed_cmd: The parsed command (no parameters needed)

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        self.graphdb.disconnect()

        # update database reference
        self.graph_data.db = self.graphdb.db
        return True
