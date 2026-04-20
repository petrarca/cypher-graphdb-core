"""Dump database graphs command implementation for database inspection."""

from typing import TYPE_CHECKING

import cypher_graphdb.utils as utils
from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpGraphsCommand(BaseCommand):
    """Command to list all graphs in the database."""

    command_name = "dump_graphs"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_graphs", tokens=["graphs"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the dump database graphs command.

        Args:
            parsed_cmd: The parsed command (no parameters needed)

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        result = utils.to_collection(self.graphdb.db.graphs())

        return self._post_processing_cmd(parsed_cmd, result, render_kwargs={"col_headers": ["DBGraph"]})
