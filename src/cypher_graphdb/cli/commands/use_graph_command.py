"""Change database graph command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class UseGraphCommand(BaseCommand):
    """Command to switch to a different graph."""

    command_name = "use_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[use_graph", tokens=["use graph", "use"])
    completion = "graphs"

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the change database graph command.

        Args:
            parsed_cmd: The parsed command containing graph name in args

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.change_graph(parsed_cmd.args)
        return True
