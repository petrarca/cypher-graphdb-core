"""Export graph command implementation for CLI."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.exporter import GraphExporter
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.models import Graph, TreeResult


class ExportGraphCommand(BaseCommand):
    """Handle graph export commands.

    This command exports graph data to various formats using the GraphExporter.
    It can export either the current graph, input graph data, or TreeResult.
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
        exporter = GraphExporter(self.graphdb.db)

        if parsed_cmd.is_firstcmd():
            # Export the current graph
            exporter.export(self.graph_data.graph, parsed_cmd.args, parsed_cmd.kwargs)
        elif isinstance(parsed_cmd.input, TreeResult):
            # TreeResult from | tree - export as tree directly
            exporter.export_tree(parsed_cmd.input, parsed_cmd.args, parsed_cmd.kwargs)
        elif isinstance(parsed_cmd.input, Graph):
            # Graph input - export normally
            exporter.export(parsed_cmd.input, parsed_cmd.args, parsed_cmd.kwargs)
        else:
            # Other input - convert to graph and export
            graph = Graph()
            graph.merge(parsed_cmd.input)
            exporter.export(graph, parsed_cmd.args, parsed_cmd.kwargs)

        # Set output to input for potential chaining
        parsed_cmd.set_output_to_input()
        return True
