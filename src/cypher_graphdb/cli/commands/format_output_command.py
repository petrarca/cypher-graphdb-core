"""Format output command implementation."""

from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class FormatOutputCommand(BaseCommand):
    """Command to handle output formatting."""

    command_name = "format_output"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[format_output]]_", tokens=["json", "table", "list", None])

    def __init__(self, runtime: CLIRuntime):
        """Initialize the format output command.

        Args:
            runtime: CLI runtime providing access to config and renderer
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the format output command.

        Either sets the output format globally or formats specific output.

        Args:
            parsed_cmd: Parsed command with format type and data

        Returns:
            bool: True if command executed successfully
        """
        if parsed_cmd.is_singlecmd():
            # Set global output format
            self.config.set_properties({"output_format": parsed_cmd.cmd})
            rich.print(f"[green]Output format changed to '{parsed_cmd.cmd}'")
        else:
            # Format specific output
            parsed_cmd.set_output_to_input()

            if parsed_cmd.is_finalcmd():
                self.renderer.render(
                    parsed_cmd.output,
                    parsed_cmd.cmd,
                    parsed_cmd.args,
                    parsed_cmd.kwargs,
                )
        return True
