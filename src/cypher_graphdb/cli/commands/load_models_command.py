"""Load Models Command - Loads model modules."""

import sys

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.cli.runtime import CLIRuntime


class LoadModelsCommand(BaseCommand):
    """Command to load model modules."""

    command_name = "load_models"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[load_models", tokens=["load"])

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute load models command.

        Args:
            parsed_cmd: Parsed command with arguments and options

        Returns:
            True if command executed successfully, False otherwise
        """
        if not parsed_cmd.has_num_args(0, 1):
            rich.print("[red]Exact one module required!", file=sys.stderr)
            return False

        module_name = parsed_cmd.get_arg(0)
        path = parsed_cmd.get_kwarg("path")

        loaded_models = self.graphdb.db.model_provider.try_to_load_models(module_name, path)

        if loaded_models is None:
            rich.print(
                f"[red]Could not load modules from '{module_name}', {path=}",
                file=sys.stderr,
            )
            return False

        if loaded_models:
            rich.print(f"[blue]Loaded {len(loaded_models)} model(s)")
            for v in loaded_models:
                rich.print(f"[blue]  {v}")
        else:
            rich.print(f"[yellow]No new models loaded from '{module_name}'")

        return True
