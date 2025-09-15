"""Help command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class HelpCommand(BaseCommand):
    """Command to show help information."""

    command_name = "help"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[help_", tokens=["help"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the help command.

        Args:
            parsed_cmd: The parsed command with optional help arguments

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        import cypher_graphdb.cli.help as help

        return help.show_help(parsed_cmd.args, self.prompt_parser)
