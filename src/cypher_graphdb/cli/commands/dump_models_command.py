"""Dump models command implementation."""

from typing import TYPE_CHECKING

import rich

from cypher_graphdb import utils
from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpModelsCommand(BaseCommand):
    """Command to list loaded models."""

    command_name = "dump_models"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_models", tokens=["models"])
    completion = "label_only"

    def __init__(self, runtime: CLIRuntime):
        """Initialize the dump models command.

        Args:
            runtime: CLI runtime providing access to graphdb and processing
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the dump models command.

        Lists all loaded models or details for a specific model.

        Args:
            parsed_cmd: Parsed command with optional model name argument

        Returns:
            bool: True if command executed successfully
        """
        if not parsed_cmd.args:
            # List all models
            result = vars(self.graphdb.db.model_provider)
            render_kwargs = {"key_name": "Model", "value_name": "Definition"}
        else:
            # Show specific model details
            model_name = parsed_cmd.get_arg(0)
            if model_name and isinstance(model_name, str):
                model = self.graphdb.db.model_provider.get(model_name)
                result = utils.to_collection(model.model_dump(context={"with_detailed_fields": True})) if model else None
            else:
                result = None

            render_kwargs = {"key_name": "Item"} if result else None

        parsed_cmd.output = result

        if result:
            return self._post_processing_cmd(parsed_cmd, result, False, render_kwargs=render_kwargs)

        rich.print("[yellow]Model(s) not found")
        return False
