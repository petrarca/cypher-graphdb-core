"""Fetch nodes command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.models import GraphObjectType

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class FetchNodesCommand(BaseCommand):
    """Command to fetch nodes from the database."""

    command_name = "fetch_nodes"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(
        pattern="[[fetch_nodes", tokens=["fetch nodes"], object_type=GraphObjectType.NODE
    )

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the fetch nodes command.

        Args:
            runtime: CLI runtime providing access to graphdb and processing
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the fetch nodes command.

        Fetches nodes from the database based on provided filters.

        Args:
            parsed_cmd: Parsed command with arguments and filters

        Returns:
            bool: True if command executed successfully and found results
        """
        result = self.graphdb.fetch_nodes(parsed_cmd.args, parsed_cmd.kwargs)
        if not result:
            return False

        return self._post_processing_cmd(parsed_cmd, result)
