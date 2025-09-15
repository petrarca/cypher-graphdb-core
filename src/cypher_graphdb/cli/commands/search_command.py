"""Search command implementation for full-text search functionality."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd
    from cypher_graphdb.cypherparser import ParsedCypherQuery


class SearchCommand(BaseCommand):
    """Command to perform full-text search on graph data."""

    command_name = "search"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[search]]", tokens=["search"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the search command.

        Args:
            parsed_cmd: The parsed command containing search query

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        if not (parsed_query := self._resolve_parsed_query(parsed_cmd)):
            return False

        if not parsed_cmd.has_num_args(1):
            rich.print("[red]Invalid argument(s): <search> <search query>")
            return False

        parsed_cmd.output = self.graphdb.db.search(parsed_query, parsed_cmd.get_arg(0), parsed_cmd.get_kwarg("language"), False)

        # label columns based on cypher returns
        render_kwargs = {"col_headers": self.graphdb.db.last_parsed_query.return_arguments.values()}

        return self._post_processing_cmd(parsed_cmd, parsed_cmd.output, render_kwargs=render_kwargs)

    def _resolve_parsed_query(self, parsed_cmd: "PromptParserCmd") -> "ParsedCypherQuery | None":
        """Resolve the parsed query for search operations.

        This helper method determines the appropriate parsed query based on
        the command context (single command, first in chain, or chained).

        Args:
            parsed_cmd: The parsed command to resolve query for

        Returns:
            The resolved ParsedCypherQuery or None if resolution fails
        """
        from cypher_graphdb.cypherparser import ParsedCypherQuery

        if parsed_cmd.is_singlecmd():
            if not (parsed_query := self.graphdb.db.last_parsed_query):
                rich.print("[yellow]No parsed query")
                return None
        elif parsed_cmd.is_firstcmd():
            parsed_query = self.graphdb.db.last_parsed_query
        else:
            parsed_query = parsed_cmd.input

        if not parsed_query or not isinstance(parsed_query, ParsedCypherQuery):
            rich.print(
                f"[red]Invalid input for <{parsed_cmd.cmd}> operation or no parsed query!",
                file=sys.stderr,
            )
            return None

        return parsed_query
