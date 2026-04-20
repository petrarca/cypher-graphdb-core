"""Delete graph object command implementation for CLI."""

import sys

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd


class DeleteGraphobjCommand(BaseCommand):
    """Handle dynamic delete commands for graph objects.

    This command handles delete operations for different graph object types
    by extracting the object type from the command action and performing
    the appropriate delete operation.
    """

    command_name = "delete"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[delete", tokens=["delete"])
    completion = {"type": "label_props", "label_from_model": False, "resolve_model_props": False}

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the delete graph object command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        # Extract object type from command action (e.g., "delete_node" -> "node")
        graph_obj_type = parsed_cmd.action.partition("_")[2]

        # Get the object reference from first argument
        obj_ref = parsed_cmd.get_arg(0)

        # Fetch the graph object to delete
        graphobj = self.graph_data.fetch_graph_obj(obj_ref, graph_obj_type)
        if graphobj:
            # Perform the delete operation
            self.graphdb.delete_graph_obj(graphobj, parsed_cmd.args, parsed_cmd.kwargs)
            return True
        else:
            rich.print("[red]Graph object could not be found!", file=sys.stderr)
            return False
