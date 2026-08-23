"""GID command implementation for generating unique string IDs."""

import sys
from typing import TYPE_CHECKING, cast

import rich

import cypher_graphdb.utils as utils

from .base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class GidCommand(BaseCommand):
    """Command for generating unique string IDs.

    Usage: gid [n]

    Generates n unique string IDs (default: 1) and prints them to stdout.
    Each ID is printed on a separate line in blue color.
    """

    # Command name for self-registration
    command_name = "gid"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry("gid", tokens=["gid"], requires_backend=False)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the gid command.

        Args:
            parsed_cmd: The parsed command with optional count argument

        Returns:
            True if execution was successful, False on error
        """
        try:
            # get_arg's cast_to parameter guarantees an int here, but its
            # signature can't express that dependency statically.
            n = cast(int, parsed_cmd.get_arg(0, 1, int))
        except ValueError:
            rich.print(f"[red]Invalid argument {parsed_cmd.args}!", file=sys.stderr)
            return False

        for _ in range(n):
            rich.print(f"[blue]{utils.generate_unique_string_id()}")

        return True
