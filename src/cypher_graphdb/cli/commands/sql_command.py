"""SQL command implementation for executing SQL queries."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class SqlCommand(BaseCommand):
    """Command to execute SQL queries against the database."""

    command_name = "sql"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[sql*", tokens=["sql"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the SQL command.

        Args:
            parsed_cmd: The parsed command containing SQL query in options

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        if not parsed_cmd.options:
            rich.print("[red]Invalid argument(s): <sql> <sql query>", file=sys.stderr)
            return False

        try:
            result = self.graphdb.execute_sql(parsed_cmd.options)

            render_kwargs = {"col_headers": self.graphdb.db.sql_statistics().col_names, "col_style": "green"}
        # pylint: disable=W0718
        except Exception as e:
            rich.print(e)
            return False

        return self._post_processing_cmd(parsed_cmd, result, render_kwargs=render_kwargs)
