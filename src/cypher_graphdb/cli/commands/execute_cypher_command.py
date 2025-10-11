"""Execute Cypher command implementation for CLI."""

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.utils import resolve_column_names


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
            return_args = self.graphdb.db.last_parsed_query.return_arguments
            stats = self.graphdb.db.exec_statistics()

            # Resolve wildcards in column names
            resolved_col_names = resolve_column_names(return_args, result, stats.col_count)

            # Handle RETURN * case
            # When wildcard is present, resolved_col_names has numeric keys
            # for wildcard columns (e.g., "0", "1", "2") and original keys
            # for explicit columns
            if "*" in return_args:
                # Extract the resolved wildcard column values
                # (these have numeric string keys)
                sorted_keys = sorted(resolved_col_names.keys())
                wildcard_cols = [resolved_col_names[k] for k in sorted_keys if k.isdigit()]
                # Extract explicit column values (non-numeric keys)
                explicit_cols = [resolved_col_names[k] for k in resolved_col_names if not k.isdigit()]

                # Pass None to signal wildcard, plus resolved column names
                col_headers = None
                render_kwargs = {"col_headers": col_headers, "wildcard_cols": wildcard_cols, "explicit_cols": explicit_cols}
            else:
                col_headers = list(resolved_col_names.keys())
                render_kwargs = {"col_headers": col_headers}

            self.renderer.render(result, kwargs=render_kwargs)

            # Show execution statistics if enabled
            if self._cli_runtime.exec_stats_enabled:
                rich.print(f"[bright_magenta]{self.graphdb.db.exec_statistics()}\n")

        return True
