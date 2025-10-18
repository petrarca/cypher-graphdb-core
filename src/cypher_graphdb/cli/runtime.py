"""CLI Runtime interface for command subsystem."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class CLIRuntime:
    """Interface that the CLI runtime must provide to commands.

    This is a minimal interface that commands can use to access
    the shared CLI resources without creating tight coupling.
    """

    @property
    def graphdb(self):
        """Access to the GraphDB manager."""
        raise NotImplementedError

    @property
    def graph_data(self):
        """Access to the graph data manager."""
        raise NotImplementedError

    @property
    def config(self):
        """Access to the CLI configuration."""
        raise NotImplementedError

    @property
    def renderer(self):
        """Access to the result renderer."""
        raise NotImplementedError

    @property
    def prompt_parser(self):
        """Access to the prompt parser."""
        raise NotImplementedError

    def post_processing_cmd(
        self,
        parsed_cmd: "PromptParserCmd",
        result: Any,
        append_to_graph: bool = True,
        render_kwargs: dict[str, Any] | None = None,
    ) -> bool:
        """Post-process command results."""
        raise NotImplementedError

    @property
    def autoconfirm(self) -> bool:
        """Current autoconfirm state."""
        raise NotImplementedError

    def set_autoconfirm(self, value: bool) -> bool:
        """Set autoconfirm state and return previous value."""
        raise NotImplementedError

    def execute_from_file(self, path: str) -> None:
        """Execute commands from a file."""
        raise NotImplementedError

    @property
    def exec_stats_enabled(self) -> bool:
        """Whether execution statistics are enabled."""
        raise NotImplementedError

    def set_last_cypher_cmd(self, cmd: str) -> None:
        """Store the last executed cypher command."""
        raise NotImplementedError

    def parse_and_execute(self, cmdline: str, terminate_on_failure: bool) -> bool:
        """Parse and execute a command line.

        Args:
            cmdline: The command line to parse and execute
            terminate_on_failure: Whether to terminate on failure

        Returns:
            bool: True if successful, False otherwise
        """
        raise NotImplementedError
