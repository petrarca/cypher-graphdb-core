"""Last Result Operation Command - Operations on last result."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.cli.runtime import CLIRuntime


class LastResultOpCommand(BaseCommand):
    """Command to perform operations on the last result."""

    command_name = "last_result_op"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="last_result_op_", tokens=["."])

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute last result operation command.

        Args:
            parsed_cmd: Parsed command with arguments and options

        Returns:
            True if command executed successfully, False otherwise
        """
        if parsed_cmd.is_firstcmd():
            parsed_cmd.input = self.graph_data.get_var(".")
        return True
