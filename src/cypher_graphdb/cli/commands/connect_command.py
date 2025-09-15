"""Connect command implementation for database connection."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ConnectCommand(BaseCommand):
    """Command to connect to a database."""

    command_name = "connect"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[connect_", tokens=["connect"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the connect command.

        Args:
            parsed_cmd: The parsed command with connection parameters

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        self.graphdb.connect(parsed_cmd.kwargs, from_prompt=True)

        # inject database in any case
        self.graph_data.db = self.graphdb.db
        return True
