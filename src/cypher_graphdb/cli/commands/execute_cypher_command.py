"""Execute Cypher command implementation for CLI."""

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd


class ExecuteCypherCommand(BaseCommand):
    """Handle Cypher query execution commands.

    This is the core command that executes Cypher queries against the database.
    It handles parsing, execution, result rendering, and statistics display.
    """

    command_name = "execute_cypher"

    # This is the default command handler - not explicitly mapped in CMD_MAP
    # It handles any input that doesn't match other command patterns

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the Cypher command.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if successful, False otherwise
        """
        # Check for parse-only mode (dump_parsed_query or search)
        if parsed_cmd.has_nextcmd() and parsed_cmd.nextcmd().is_action({"dump_parsed_query", "search"}):
            # parse only
            parsed_cmd.output = self.graphdb.db.parse(parsed_cmd.cmd)
            return True

        # Check if we have an active graph
        if not self.graphdb.db.backend.graph_name:
            rich.print("[yellow]No active graph. Change with <change graph>.")
            return False

        try:
            # Execute the cypher command
            result = self.graphdb.execute(parsed_cmd.cmd)
        # pylint: disable=W0718
        except Exception as e:
            rich.print(e)
            return False

        # Store the last cypher command
        self._cli_runtime.set_last_cypher_cmd(parsed_cmd.cmd)

        # Set output for potential command chaining
        parsed_cmd.output = result

        if parsed_cmd.is_finalcmd():
            # Update last result only when not part of a pipeline
            self.graph_data.last_result = result

            # Label columns based on cypher returns
            render_kwargs = {"col_headers": (self.graphdb.db.last_parsed_query.return_arguments.values())}

            self.renderer.render(result, kwargs=render_kwargs)

            # Show execution statistics if enabled
            if self._cli_runtime.exec_stats_enabled:
                rich.print(f"[bright_magenta]{self.graphdb.db.exec_statistics()}\n")

        return True
