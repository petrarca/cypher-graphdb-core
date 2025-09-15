"""Graph operation command implementation for CLI."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.models import Graph


class GraphOpCommand(BaseCommand):
    """Handle graph operation commands.

    This command processes graph operations by either retrieving
    a graph variable or merging input into a new graph.
    """

    command_name = "graph_op"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="graph_op_", tokens=["_"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the graph operation command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        graph: Graph

        if parsed_cmd.is_firstcmd():
            # Get graph from variables
            graph = self.graph_data.get_var(parsed_cmd.cmd)
            parsed_cmd.input = graph
        else:
            # Create new graph and merge input
            graph = Graph()
            graph.merge(parsed_cmd.input)

        return self._post_processing_cmd(parsed_cmd, graph)
