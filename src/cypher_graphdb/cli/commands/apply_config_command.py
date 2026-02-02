"""Apply config command implementation."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ApplyConfigCommand(BaseCommand):
    """Command to apply configuration settings."""

    command_name = "config"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[config_", tokens=["config"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize the apply config command.

        Args:
            runtime: CLI runtime providing access to config and renderer
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the apply config command.

        Applies configuration settings and renders the result.

        Args:
            parsed_cmd: Parsed command with configuration arguments

        Returns:
            bool: True if command executed successfully
        """
        config_result = self.config.apply_config(parsed_cmd.args, parsed_cmd.kwargs)
        self.renderer.render(config_result)
        return True
