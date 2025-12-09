"""Export graph command implementation for CLI."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.exporter import GraphExporter
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.models import Graph


class ExportGraphCommand(BaseCommand):
    """Handle graph export commands.

    This command exports graph data to various formats using the GraphExporter.
    It can export either the current graph or input graph data.
    """

    command_name = "export_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="export_graph]]", tokens=["export", "export graph"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the export graph command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        if parsed_cmd.is_firstcmd():
            # Export the current graph
            graph = self.graph_data.graph
        else:
            # Export input data
            if isinstance(parsed_cmd.input, Graph):
                graph = parsed_cmd.input
                is_tree_input = False
            elif self._is_tree_structure(parsed_cmd.input):
                # Input is already a tree structure - convert to graph but skip tree analysis
                graph = Graph()
                graph.merge(parsed_cmd.input)
                is_tree_input = True
            else:
                # Convert input to graph format
                graph = Graph()
                graph.merge(parsed_cmd.input)
                is_tree_input = False

        # Perform the export using GraphExporter
        if is_tree_input:
            # Skip tree analysis for pre-built tree structures
            kwargs = parsed_cmd.kwargs.copy()
            kwargs["skip_tree_analysis"] = True
            GraphExporter(self.graphdb.db).export(graph, parsed_cmd.args, kwargs)
        else:
            GraphExporter(self.graphdb.db).export(graph, parsed_cmd.args, parsed_cmd.kwargs)

        # Set output to input for potential chaining
        parsed_cmd.set_output_to_input()
        return True
