"""Resolve edges command implementation."""

from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ResolveEdgesCommand(BaseCommand):
    """Command to resolve missing edge-referenced nodes in the graph."""

    command_name = "resolve_edges"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[resolve_edges]]", tokens=["resolve edges"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize the resolve edges command.

        Args:
            runtime: CLI runtime providing access to graph data and database
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the resolve edges command.

        Resolves missing edge-referenced nodes by loading them from database.

        Args:
            parsed_cmd: Parsed command (not used but required for interface)

        Returns:
            bool: True if command executed successfully
        """
        return self._resolve_edges(self.graph_data.graph)

    def _resolve_edges(self, graph, silent_success=False) -> bool:
        """Resolve missing edge-referenced nodes in the given graph.

        Args:
            graph: The graph to resolve edges for
            silent_success: If True, don't print success messages

        Returns:
            bool: True if resolution was successful
        """
        result = self.graphdb.db.resolve_edges(graph)
        if result:
            rich.print(f"[green]{len(result)} missing edge-referenced node(s) loaded into the graph.")
        elif not silent_success:
            rich.print("[yellow]All edge-referenced nodes are in the graph. Nothing to resolve.")
        return True
