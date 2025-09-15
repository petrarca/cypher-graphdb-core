"""Dump parsed query command implementation."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpParsedQueryCommand(BaseCommand):
    """Command to show parsed query information."""

    command_name = "dump_parsed_query"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_parsed_query]]", tokens=["!", "!!"])

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the dump parsed query command.

        Args:
            runtime: CLI runtime providing access to graphdb and renderer
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the dump parsed query command.

        Shows the parsed query structure from a previous cypher execution.

        Args:
            parsed_cmd: Parsed command

        Returns:
            bool: True if command executed successfully
        """
        parsed_query = self._resolve_parsed_query(parsed_cmd)
        if not parsed_query:
            rich.print("[red]Missing cypher statement with a parsed query!", file=sys.stderr)
            return False

        if parsed_cmd.cmd == "!":
            parsed_cmd.output = parsed_query.model_dump()

            if parsed_cmd.is_finalcmd():
                self.renderer.render_as_json(parsed_cmd.output)
        else:
            # Handle errors properly
            try:
                rich.print(parsed_query.parse_tree.toStringTree())
            except (AttributeError, RuntimeError) as e:
                rich.print(f"[red]Error: {e}", file=sys.stderr)

        return True

    def _resolve_parsed_query(self, parsed_cmd: "PromptParserCmd"):
        """Resolve the parsed query from command context.

        Args:
            parsed_cmd: The parsed command (for future extensions)

        Returns:
            The parsed query object or None if not available
        """
        # This method would need to be implemented based on how
        # parsed queries are stored/accessed in the CLI context
        # For now, delegate to the graphdb's last parsed query
        _ = parsed_cmd  # Mark as intentionally unused for now
        return self.graphdb.db.last_parsed_query if self.graphdb.db else None
