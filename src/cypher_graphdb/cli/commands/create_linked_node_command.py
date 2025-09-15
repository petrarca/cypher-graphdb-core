"""Create Linked Node Command - Creates connected graph nodes."""

import sys

import rich

import cypher_graphdb.config as config
from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.cli.runtime import CLIRuntime
from cypher_graphdb.models import GraphObjectType


class CreateLinkedNodeCommand(BaseCommand):
    """Command to create new graph nodes with connections."""

    command_name = "create_linked_node"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(
        pattern="[[create_linked_node", tokens=["create linked node"], object_type=GraphObjectType.NODE
    )

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute create linked node command.

        Args:
            parsed_cmd: Parsed command with arguments and options

        Returns:
            True if command executed successfully, False otherwise
        """

        def fetch_linked_node(node_ref: str):
            if not node_ref:
                return None

            if not (node := self.graph_data.fetch_node(node_ref)):
                rich.print(f"Could not find linked node with ref {node_ref}")
                return None

            return node

        from_ref = (parsed_cmd.kwargs or {}).get("from_", None)
        to_ref = (parsed_cmd.kwargs or {}).get("to_", None)

        if from_ref and to_ref:
            rich.print(
                "[red]Either from_ or to_ are allowed. Not both!",
                file=sys.stderr,
            )
            return False

        if not from_ref and not to_ref:
            rich.print("Missing either from_ or to_!", file=sys.stderr)
            return False

        direction = config.TREE_DIRECTION_INCOMING if from_ref else config.TREE_DIRECTION_OUTGOING

        if not (linked_node := fetch_linked_node(from_ref or "") or fetch_linked_node(to_ref or "")):
            return False

        edge_label = (parsed_cmd.kwargs or {}).pop("edge_label_", None)
        if not edge_label:
            rich.print(
                "Missing edge_label_ for edge to link the nodes!",
                file=sys.stderr,
            )
            return False

        result = self.graphdb.create_linked_node(
            linked_node,
            direction,
            edge_label,
            parsed_cmd.args,
            parsed_cmd.kwargs,
        )

        return self._cli_runtime.post_processing_cmd(parsed_cmd, result)
