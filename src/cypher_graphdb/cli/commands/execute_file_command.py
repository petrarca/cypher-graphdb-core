"""Execute file command implementation for script execution."""

import sys
from typing import TYPE_CHECKING

import rich

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class ExecuteFileCommand(BaseCommand):
    """Command to execute commands from a file within the interactive prompt."""

    command_name = "execute_file"

    # For command line parsing
    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[execute_file", tokens=["execute", "exec"])

    def execute(self, parsed_cmd: "PromptParserCmd") -> bool:
        """Execute the execute file command.

        Usage: execute <path>  or  exec <path>
        The file content is parsed using the same multi-line semicolon
        aware logic as -f mode. Autoconfirm is forced for duration of
        the execution (script-like semantics) and restored afterwards.

        Args:
            parsed_cmd: The parsed command with file path argument

        Returns:
            True if execution was successful
        """
        # Exact same logic as original implementation
        if not parsed_cmd.has_num_args(1):
            rich.print("[red]Usage: execute <file>")
            return False

        path = str(parsed_cmd.get_arg(0))
        # Ask for confirmation unless already autoconfirm
        # (set by user or non-interactive)
        if self._cli_runtime is None:
            raise RuntimeError("CLI runtime not available")

        if not self._cli_runtime.autoconfirm and sys.stdin.isatty():
            try:
                prompt_msg = f"Execute script '{path}'? [y/N]: "
                answer = input(prompt_msg).strip().lower()
            except EOFError:
                answer = ""
            if answer not in {"y", "yes"}:
                rich.print("[yellow]Execution aborted.")
                return False

        prev_autoconfirm = self._cli_runtime.set_autoconfirm(True)
        try:
            self._cli_runtime.execute_from_file(path)
        finally:
            self._cli_runtime.set_autoconfirm(prev_autoconfirm)
        return True
