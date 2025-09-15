"""Fetch all command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class FetchAllCommand(BaseCommand):
    """Command to fetch all nodes and edges from the database."""

    command_name = "fetch_all"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[fetch_all", tokens=["fetch", "fetch all"])

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the fetch all command.

        Args:
            runtime: CLI runtime providing access to graphdb and processing
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the fetch all command.

        Fetches both nodes and edges from the database and merges results.

        Args:
            parsed_cmd: Parsed command with arguments and filters

        Returns:
            bool: True if command executed successfully and found results
        """
        # Fetch nodes from database
        result_nodes = self.graphdb.fetch_nodes(parsed_cmd.args, parsed_cmd.kwargs)
        if not result_nodes:
            result_nodes = []

        # Fetch edges from database
        result_edges = self.graphdb.fetch_edges(parsed_cmd.args, parsed_cmd.kwargs)
        if not result_edges:
            result_edges = []

        # Merge the results
        result = result_nodes + result_edges
        if not result:
            return False

        return self._post_processing_cmd(parsed_cmd, result)
