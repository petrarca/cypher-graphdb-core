"""Exit command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ExitCommand(BaseCommand):
    """Command to exit the CLI application."""

    command_name = "exit"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[exit_", tokens=["exit", "quit", "q", "bye"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the exit command.

        Args:
            parsed_cmd: The parsed command

        Returns:
            True to indicate successful execution
        """
        # Exact same logic as original implementation
        parsed_cmd.parse_result.exit_cmd = True
        return True
