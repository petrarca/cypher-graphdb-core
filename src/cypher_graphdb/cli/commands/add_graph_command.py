"""Add to graph command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class AddGraphCommand(BaseCommand):
    """Command to add result to the current graph."""

    command_name = "add_graph"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="add_graph_", tokens=["add", "add graph"])

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the add to graph command.

        Args:
            runtime: CLI runtime providing access to graph data and parser
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the add to graph command.

        This command rewrites the parsed command to set a variable,
        effectively adding the result to the graph.

        Args:
            parsed_cmd: Parsed command with arguments

        Returns:
            bool: True if command executed successfully
        """
        rewrite_cmd = self.prompt_parser.parse_cmd(f"set {'_=' if parsed_cmd.is_firstcmd() else ''}{parsed_cmd.get_arg(0, '_')}")
        parsed_cmd.replay(rewrite_cmd)

        return True
