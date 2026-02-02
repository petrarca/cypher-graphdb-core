"""Get command implementation for variable retrieval."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class GetCommand(BaseCommand):
    """Command to get/retrieve variables from the CLI."""

    command_name = "get"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[get_", tokens=["get", "$"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the get command.

        Args:
            parsed_cmd: The parsed command with variable name to retrieve

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        if not parsed_cmd.has_num_args(1):
            rich.print("[red]Invalid syntax. Use get <varname> or $<varname>", file=sys.stderr)
            return False

        varname = parsed_cmd.get_arg(0)
        result = self.graph_data.variables if varname == "$" else self.graph_data.get_var(varname)

        if not self._post_processing_cmd(parsed_cmd, result):
            if varname != "$":
                rich.print(f"[red]'{varname}' not found!")
            return False

        return True
