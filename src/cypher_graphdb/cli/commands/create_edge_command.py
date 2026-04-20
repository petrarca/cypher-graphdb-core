"""Create Edge Command - Creates new graph edges."""

from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.models import GraphObjectType

if TYPE_CHECKING:
    from cypher_graphdb.cli.runtime import CLIRuntime


class CreateEdgeCommand(BaseCommand):
    """Command to create new graph edges."""

    command_name = "create_edge"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(
        pattern="[[create_edge", tokens=["create edge"], object_type=GraphObjectType.EDGE
    )
    completion = {"type": "label_props", "complete_mandatory_props": True}

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute create edge command.

        Args:
            parsed_cmd: Parsed command with arguments and options
            Expected format: create edge <label> <start_node_ref>
                           <end_node_ref>
                           [prop=val,...]

        Returns:
            True if command executed successfully, False otherwise
        """
        if not parsed_cmd.has_num_args(3):
            # <label> <start_node_ref> <end_node_ref>
            rich.print("Invalid syntax: create edge <label>, <start_node>, <end_node>[,prop=val,...]!")
            return False

        # Extract node references from arguments
        node_ref = parsed_cmd.args.pop(1)
        if not (start_node := self.graph_data.fetch_node(node_ref)):
            rich.print(f"Could not find start node with ref {node_ref}")
            return False

        node_ref = parsed_cmd.args.pop(1)
        if not (end_node := self.graph_data.fetch_node(node_ref)):
            rich.print(f"Could not find end node with ref {node_ref}")
            return False

        # Create the edge
        if not (result := self.graphdb.create_edge(start_node, end_node, parsed_cmd.args, parsed_cmd.kwargs)):
            return False

        return self._post_processing_cmd(parsed_cmd, result)
