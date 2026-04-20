"""Create Node Command - Creates new graph nodes."""

from cypher_graphdb.cli.commands.base_command import BaseCommand
from cypher_graphdb.cli.promptparser import PromptParserCmd
from cypher_graphdb.cli.runtime import CLIRuntime
from cypher_graphdb.models import GraphObjectType


class CreateNodeCommand(BaseCommand):
    """Command to create new graph nodes."""

    command_name = "create_node"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(
        pattern="[[create_node", tokens=["create node"], object_type=GraphObjectType.NODE
    )
    completion = {"type": "label_props", "complete_mandatory_props": True}

    def __init__(self, runtime: CLIRuntime):
        """Initialize command with CLI runtime."""
        super().__init__(runtime)

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """
        Execute create node command.

        Args:
            parsed_cmd: Parsed command with arguments and options

        Returns:
            True if command executed successfully, False otherwise
        """
        if not (result := self.graphdb.create_node(parsed_cmd.args, parsed_cmd.kwargs)):
            return False

        return self._cli_runtime.post_processing_cmd(parsed_cmd, result)
