"""Update graph object command implementation for CLI."""

import sys

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd


class UpdateGraphobjCommand(BaseCommand):
    """Handle dynamic update commands for graph objects.

    This command handles update operations for different graph object types
    by extracting the object type from the command action and performing
    the appropriate update operation.
    """

    command_name = "update"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[update", tokens=["update"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the update graph object command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        # Extract object type from command action (e.g., "update_node" -> "node")
        graph_obj_type = parsed_cmd.action.partition("_")[2]

        # Get the object reference from first argument
        obj_ref = parsed_cmd.get_arg(0)

        # Fetch the graph object to update
        graphobj = self.graph_data.fetch_graph_obj(obj_ref, graph_obj_type)
        if not graphobj:
            rich.print("[red]Graph object could not be found!", file=sys.stderr)
            return False

        # Perform the update operation
        result = self.graphdb.update_graph_obj(graphobj, parsed_cmd.args, parsed_cmd.kwargs)

        return self._post_processing_cmd(parsed_cmd, result)
