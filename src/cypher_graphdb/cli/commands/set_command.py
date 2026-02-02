"""Set command implementation for variable management."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class SetCommand(BaseCommand):
    """Command to set variables in the CLI."""

    command_name = "set"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="set_", tokens=["set"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the set command.

        Args:
            parsed_cmd: The parsed command with variable assignments

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        kwargs = parsed_cmd.kwargs

        if parsed_cmd.is_firstcmd():
            if not kwargs or len(kwargs) != 1:
                rich.print("[red]Command 'set' requires exactly one variable assignment: set <varname>=<value>", file=sys.stderr)
                return False
        else:
            if parsed_cmd.has_num_args(1) and parsed_cmd.args:
                kwargs = {parsed_cmd.args[0]: parsed_cmd.input}
            else:
                rich.print(
                    "[red]Command 'set' can only assign one variable if part of piped command: set <varname>", file=sys.stderr
                )
                return False

        (varname,) = kwargs
        result = self.graph_data.set_var(varname, kwargs[varname])

        return self._post_processing_cmd(parsed_cmd, result)
