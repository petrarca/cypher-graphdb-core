"""Graph To Tree Command - Converts graph to tree structure."""

from typing import TYPE_CHECKING

import rich

import cypher_graphdb.config as config
import cypher_graphdb.graphops as gops
from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.models import Graph

if TYPE_CHECKING:
    from cypher_graphdb.cli.runtime import CLIRuntime


class GraphToTreeCommand(BaseCommand):
    """Command to convert graph to tree structure."""

    command_name = "graph_to_tree"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="graph_to_tree_", tokens=["tree"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute graph to tree command.

        Args:
            parsed_cmd: Parsed command with arguments and options

        Returns:
            True if command executed successfully, False otherwise
        """
        # Resolve edges to get consistent graph
        if parsed_cmd.is_singlecmd():
            graph = self.graph_data.graph
        else:
            if isinstance(parsed_cmd.input, Graph):
                graph = parsed_cmd.input
            else:
                graph = Graph()
                graph.merge(parsed_cmd.input)

        if graph:  # has items
            # Note: For now we skip the _resolve_edges call to avoid complexity
            # during migration. This should be restored once
            # ResolveEdgesCommand is properly integrated.
            pass

        direction = (parsed_cmd.kwargs or {}).get("direction", config.DEFAULT_TREE_DIRECTION)
        with_unbound_nodes = (parsed_cmd.kwargs or {}).get("with_unbound_nodes", True) or "with_unbound_nodes" in (
            parsed_cmd.args or []
        )

        parsed_cmd.output = gops.build_tree(graph, direction, with_unbound_nodes)

        if parsed_cmd.is_finalcmd():
            if parsed_cmd.output:
                self.renderer.render_as_tree(parsed_cmd.output, parsed_cmd.args, parsed_cmd.kwargs)
            else:
                rich.print("[yellow]Could not resolve any tree from graph.")

        return True
