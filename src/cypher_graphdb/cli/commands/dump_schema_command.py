"""Dump schema command implementation."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb import utils
from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.commands.base_command import CLIRuntime
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpSchemaCommand(BaseCommand):
    """Command to show model schema."""

    command_name = "dump_schema"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_schema", tokens=["schema"])
    completion = "label_only"

    def __init__(self, runtime: CLIRuntime):
        """Initialize the dump schema command.

        Args:
            runtime: CLI runtime providing access to graphdb and processing
        """
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the dump schema command.

        Shows the schema for a specific model.

        Args:
            parsed_cmd: Parsed command with model name argument

        Returns:
            bool: True if command executed successfully
        """
        if not parsed_cmd.has_num_args(1):
            rich.print(f"[red]Invalid syntax: {parsed_cmd.cmd} <model>!", file=sys.stderr)
            return False

        model_name = parsed_cmd.get_arg(0)
        if not isinstance(model_name, str):
            rich.print("[red]Model name must be a string!", file=sys.stderr)
            return False

        modelinfo = self.graphdb.db.model_provider.get(model_name)
        if not modelinfo:
            rich.print(f"[red]Model '{model_name}' not found!", file=sys.stderr)
            return False

        render_kwargs = {"key_name": "Schema Property", "value_name": "Value"}

        return self._post_processing_cmd(
            parsed_cmd, utils.to_collection(modelinfo.graph_schema.model_dump()), render_kwargs=render_kwargs
        )
