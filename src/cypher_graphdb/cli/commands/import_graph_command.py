"""Import graph command implementation for CLI."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.importer import GraphImporter
from cypher_graphdb.cli.promptparser import PromptParserCmd


class ImportGraphCommand(BaseCommand):
    """Handle graph import commands.

    This command imports graph data from various formats using the GraphImporter.
    It passes autoconfirm options and handles the import process.
    """

    command_name = "import_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[import_graph", tokens=["import", "import graph"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the import graph command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        # Pass autoconfirm option to GraphImporter
        autoconfirm_value = self._cli_runtime.autoconfirm
        importer = GraphImporter(self.graphdb.db, autoconfirm=autoconfirm_value)

        # Add autoconfirm to kwargs for _resolve_importer
        if parsed_cmd.kwargs is None:
            parsed_cmd.kwargs = {}
        parsed_cmd.kwargs["autoconfirm"] = autoconfirm_value

        # Perform the import
        importer.load(parsed_cmd.args, parsed_cmd.kwargs)
        return True
