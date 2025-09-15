"""Dump statistics command implementation."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb import utils
from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpStatisticsCommand(BaseCommand):
    """Command to show execution statistics."""

    command_name = "stats"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[stats", tokens=["stats", "statistics"])

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the dump statistics command.

        Args:
            runtime: CLI runtime providing access to graphdb and renderer
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the dump statistics command.

        Shows execution statistics for either cypher or SQL operations.

        Args:
            parsed_cmd: Parsed command with optional statistics type

        Returns:
            bool: True if command executed successfully
        """
        stats_type = parsed_cmd.get_arg(0, "exec")

        match stats_type:
            case "exec":
                result = self.graphdb.db.exec_statistics()
            case "sql":
                result = self.graphdb.db.sql_statistics()
            case _:
                rich.print(f"[red]Invalid statistics type {stats_type}!", file=sys.stderr)
                return False

        # Convert to collection format for rendering
        self.renderer.render(utils.to_collection(result))

        return True
