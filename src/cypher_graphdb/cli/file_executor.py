"""File execution module for CLI.

This module handles the execution of command files with progress tracking
and statistics reporting.
"""

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, TextIO

import rich
from loguru import logger
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn

from cypher_graphdb.command_reader import CommandReader, FileCommandReader

if TYPE_CHECKING:
    from cypher_graphdb.cli.runtime import CLIRuntime


@dataclass
class ExecutionStats:
    """Statistics from file execution."""

    total: int = 0
    with_results: int = 0
    without_results: int = 0
    errors: int = 0
    elapsed: float = 0.0

    def summary_text(self) -> str:
        """Generate a human-readable summary."""
        parts = []
        if self.with_results > 0:
            parts.append(f"[green]{self.with_results} returned data[/]")
        if self.without_results > 0:
            parts.append(f"[yellow]{self.without_results} no data[/]")
        if self.errors > 0:
            parts.append(f"[red]{self.errors} errors[/]")

        summary = ", ".join(parts) if parts else "no commands"
        return f"[green]✓[/] Completed {self.total} commands ({summary}) in {self.elapsed:.2f}s"


class FileExecutor:
    """Handles execution of command files with progress tracking."""

    def __init__(self, runtime: "CLIRuntime"):
        """Initialize the file executor.

        Args:
            runtime: The CLI runtime instance
        """
        self._runtime = runtime

    def execute(
        self,
        file: str | TextIO,
        show_progress: bool = True,
        verbose: bool = False,
        show_summary: bool = True,
    ) -> ExecutionStats:
        """Execute commands from a file or stream.

        Args:
            file: File path or text stream to execute commands from
            show_progress: Whether to show progress bar
            verbose: Whether to show each command being executed
            show_summary: Whether to show summary at the end

        Returns:
            ExecutionStats: Statistics about the execution
        """
        stats = ExecutionStats()
        start_time = time.time()

        # Temporarily suppress individual command output during file exec
        prev_suppress = self._runtime.renderer.suppress_output
        self._runtime.renderer.suppress_output = True

        filename = file if isinstance(file, str) else "stdin"

        try:
            # Create progress bar if showing feedback
            if show_progress:
                progress = self._create_progress_bar()
                progress.start()
                task = progress.add_task(f"Executing '{filename}'...", total=None)
            else:
                progress = None
                task = None

            # Execute commands
            self._execute_commands(file, stats, progress, task, filename, verbose)

            # Stop progress bar
            if progress:
                progress.stop()

            # Calculate elapsed time
            stats.elapsed = time.time() - start_time

            # Show summary if requested
            if show_summary:
                rich.print(stats.summary_text())

        finally:
            # Restore previous suppress_output state
            self._runtime.renderer.suppress_output = prev_suppress

        return stats

    def _create_progress_bar(self) -> Progress:
        """Create and configure a Rich progress bar.

        Returns:
            Configured Progress instance
        """
        return Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            transient=False,
        )

    def _execute_commands(
        self,
        file: str | TextIO,
        stats: ExecutionStats,
        progress: Progress | None,
        task,
        filename: str,
        verbose: bool = False,
    ) -> None:
        """Execute all commands from the file.

        Args:
            file: File path or stream
            stats: Statistics object to update
            progress: Progress bar instance (or None)
            task: Progress task ID (or None)
            filename: Display name for the file
            verbose: Whether to show each command being executed
        """

        def exec_commands(reader: CommandReader):
            for cmd in reader:
                logger.debug("Exec command: {}", cmd)
                stats.total += 1

                # Show command if verbose mode is enabled
                if verbose:
                    rich.print(f"[dim]→ {cmd}[/]")

                # Store previous last_result to detect if command
                # produced output
                prev_result = self._runtime.graph_data.last_result

                success = self._runtime.parse_and_execute(cmd, True)

                # Determine outcome and update stats
                if not success:
                    stats.errors += 1
                elif self._runtime.graph_data.last_result != prev_result and self._runtime.graph_data.last_result:
                    stats.with_results += 1
                else:
                    stats.without_results += 1

                # Update progress bar
                if progress and task is not None:
                    progress.update(
                        task,
                        completed=stats.total,
                        description=(f"Executing '{filename}' - {stats.total} commands"),
                    )

                if not success:
                    break

        if isinstance(file, str):
            try:
                logger.debug("Try to open command file {}", file)
                with FileCommandReader(file) as reader:
                    exec_commands(reader)
            except FileNotFoundError:
                rich.print(f"[red]File not found: {file}")
        else:
            exec_commands(CommandReader(file))

    def execute_silent(self, file: str | TextIO) -> ExecutionStats:
        """Execute commands from a file without any output.

        Convenience method for silent execution.

        Args:
            file: File path or stream to execute commands from

        Returns:
            ExecutionStats: Statistics about the execution
        """
        return self.execute(file, show_progress=False)
