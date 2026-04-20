"""Base command class for CLI command implementations."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from cypher_graphdb.cli.runtime import CLIRuntime

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd
    from cypher_graphdb.models import GraphObjectType


class BaseCommand(ABC):
    """Base class for all CLI commands.

    Provides the API interface between individual commands and the CLI runtime.
    All command implementations should inherit from this class.

    Commands can define a command_name class attribute for identification
    and manual registration with the command registry.
    """

    # Optional: Command name for identification
    command_name: str | None = None

    # Optional: Command map entry for CLI command mapping
    command_map_entry: dict[str, Any] | None = None

    # Optional: Declarative completion spec for command arguments.
    # Interpreted by CommandLineCompleter. Supported values:
    #
    # Shorthand strings:
    #   "label_props"   -- complete labels + properties (fetch, create node/edge)
    #   "label_only"    -- complete labels only (schema, models)
    #   "props_only"    -- complete properties only (search)
    #   "graphs"        -- complete from live graph list
    #   "variables"     -- complete from current variable names
    #   "config"        -- complete from config property names
    #
    # Static list:
    #   ["val1", "val2"] -- complete from a fixed list of strings
    #
    # Dict for fine-grained control:
    #   {
    #     "type": "label_props" | "label_only" | "props_only",
    #     "complete_mandatory_props": bool,   # show mandatory props immediately
    #     "label_from_model": bool,           # use model registry for labels
    #     "resolve_model_props": bool,        # resolve props from model fields
    #     "default_from_values": bool,        # prefill defaults from graph values
    #     "extra_props": list[str],           # additional property names to offer
    #                                         # use "from_|to_" for aliased props
    #                                         # use "edge_label_" for edge label picker
    #   }
    #
    # None (default) -- no argument completion
    completion: str | list[str] | dict[str, Any] | None = None

    @classmethod
    def create_command_map_entry(
        cls, pattern: str, tokens: set[str | None], object_type: GraphObjectType | None = None, **kwargs: Any
    ) -> dict[str, dict[str, Any]]:
        """Create a command map entry for this command.

        Args:
            pattern: The command pattern (e.g., "[[gid_")
            tokens: Set of tokens that trigger this command
            object_type: Optional GraphObjectType for the command
            **kwargs: Additional parameters for the command map entry

        Returns:
            Dictionary representing the complete command map entry
        """
        entry_value: dict[str, Any] = {"tokens": list(tokens)}
        if object_type is not None:
            entry_value["object_type"] = object_type
        entry_value.update(kwargs)
        return {pattern: entry_value}

    def __init__(self, cli_runtime: CLIRuntime | None = None) -> None:
        """Initialize the command with access to the CLI runtime.

        Args:
            cli_runtime: The CLI runtime providing access to shared resources.
                        Can be None for commands that don't need runtime access.
        """
        self._cli_runtime = cli_runtime

    @property
    def graphdb(self):
        """Access to the GraphDB manager."""
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.graphdb

    @property
    def graph_data(self):
        """Access to the graph data manager."""
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.graph_data

    @property
    def config(self):
        """Access to the CLI configuration."""
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.config

    @property
    def renderer(self):
        """Access to the result renderer."""
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.renderer

    @property
    def prompt_parser(self):
        """Access to the prompt parser."""
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.prompt_parser

    @abstractmethod
    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the command.

        Args:
            parsed_cmd: The parsed command with arguments

        Returns:
            True if execution was successful, False if there was an error
            that should terminate command processing
        """

    def _post_processing_cmd(
        self,
        parsed_cmd: PromptParserCmd,
        result: Any,
        append_to_graph: bool = True,
        render_kwargs: dict[str, Any] | None = None,
    ) -> bool:
        """Helper method for common post-processing of command results.

        This delegates to the CLI runtime's post-processing method.
        """
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")
        return self._cli_runtime.post_processing_cmd(parsed_cmd, result, append_to_graph, render_kwargs)
