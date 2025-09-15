"""Commit command implementation for transaction management."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class CommitCommand(BaseCommand):
    """Command to commit current transaction."""

    command_name = "commit"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[commit", tokens=["commit"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the commit command.

        Args:
            parsed_cmd: The parsed command (no parameters needed)

        Returns:
            True if execution was successful
        """
        # Exact same logic as original lambda implementation
        self.graphdb.commit()
        return True
