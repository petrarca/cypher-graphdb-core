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

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the execute file command.

        Usage:
            execute <path> [, verbose=true|false] [, progress=true|false]
            exec <path> [, verbose=true|false] [, progress=true|false]

        The file content is parsed using the same multi-line semicolon
        aware logic as -f mode. Autoconfirm is forced for duration of
        the execution (script-like semantics) and restored afterwards.

        Default: verbose=false, progress=true

        Args:
            parsed_cmd: The parsed command with file path argument
                        and optional verbose/progress keyword arguments

        Returns:
            True if execution was successful
        """
        # Validate arguments
        if not parsed_cmd.has_num_args(1):
            rich.print("[red]Usage: execute <file> [, verbose=true|false] [, progress=true|false]")
            return False

        path = str(parsed_cmd.get_arg(0))

        # Get optional flags
        verbose = parsed_cmd.get_kwarg("verbose", None)
        progress = parsed_cmd.get_kwarg("progress", None)

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

        # Determine if we should show progress/verbose/summary
        # Default: progress=true, verbose=false
        # Both can be overridden explicitly

        # Parse boolean values
        verbose_mode = self._parse_boolean(verbose) if verbose is not None else False
        progress_mode = self._parse_boolean(progress) if progress is not None else True

        # Always show summary if either verbose or progress is enabled
        show_summary = verbose_mode or progress_mode

        # Temporarily set autoconfirm during file execution
        prev_autoconfirm = self._cli_runtime.set_autoconfirm(True)
        try:
            # Use FileExecutor directly with explicit flags
            from cypher_graphdb.cli.file_executor import FileExecutor

            executor = FileExecutor(self._cli_runtime)
            executor.execute(
                path,
                show_progress=progress_mode,
                verbose=verbose_mode,
                show_summary=show_summary,
            )
        finally:
            self._cli_runtime.set_autoconfirm(prev_autoconfirm)
        return True

    def _parse_boolean(self, value) -> bool:
        """Parse a value as boolean.

        Args:
            value: Value to parse (bool, int, str)

        Returns:
            Boolean interpretation of the value
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "y")
        return bool(value)
