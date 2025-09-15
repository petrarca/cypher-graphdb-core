"""The cypher-graphdb command line interface (CLI)."""

import os
import subprocess
import sys
from typing import Any, Final, TextIO

import rich
from loguru import logger

# Import commands to trigger registration
from cypher_graphdb.cli import (
    banner,
    command_map,
    commands,  # noqa: F401
)
from cypher_graphdb.cli.command_manager import CommandInstanceManager
from cypher_graphdb.cli.config import CLIConfig
from cypher_graphdb.cli.graphdata import CLIGraphData
from cypher_graphdb.cli.graphdb import CLIGraphDB
from cypher_graphdb.cli.prompt import CommandLinePrompt
from cypher_graphdb.cli.promptparser import (
    PromptParser,
    PromptParserCmd,
    PromptParserResult,
)
from cypher_graphdb.cli.provider import CLIProviders
from cypher_graphdb.cli.renderer import ResultRenderer
from cypher_graphdb.cli.runtime import CLIRuntime
from cypher_graphdb.command_reader import CommandReader, FileCommandReader


class CypherGraphCLI(CLIRuntime):
    """The main class for the cypher graphdb command line interface."""

    def __init__(self, show_banner=True) -> None:
        self._renderer = ResultRenderer()
        self._graph_data = CLIGraphData()
        self._autoconfirm = False
        self._graphdb = CLIGraphDB(self._renderer)
        self.show_banner = show_banner

        self._prompt_parser = self._create_prompt_parser()
        self._last_cypher_cmd = None
        self._with_prompt = True
        self._exec_stats = False

        # add config to the end to allow configuration of sub systems,
        # e.g. renderer
        self._config = CLIConfig()
        self._config.on_property_change = self._config_on_property_change
        # needs after wiring of hooks
        self._config_setup()

        # Initialize command instances using the instance manager
        self._command_manager = CommandInstanceManager(self)

    @property
    def graphdb(self) -> CLIGraphDB:
        """Returns the cypher graphdb manager of the CLI."""
        return self._graphdb

    @property
    def graph_data(self) -> CLIGraphData:
        """Returns the graph data manager of the CLI."""
        return self._graph_data

    @property
    def config(self) -> CLIConfig:
        """Returns the CLI configuration."""
        return self._config

    @property
    def renderer(self) -> ResultRenderer:
        """Returns the result renderer."""
        return self._renderer

    @property
    def prompt_parser(self):
        """Returns the prompt parser."""
        return self._prompt_parser

    def banner(self) -> type["CypherGraphCLI"]:
        """Shows the CLI banner."""
        banner.show_banner()
        return self

    @logger.catch
    def run_catched(self, options: dict[str, Any]) -> None:
        """Main entry point for the CLI. Runtime exceptions are caught by loguru.
        This entry point should only be used for diagnostic purposes.

        Args:
            options (dict[str, Any]): Parsed command line options.

        """
        self.run(options)

    def run(self, options: dict[str, Any]) -> None:
        """Main entry point for the CLI.

        Args:
            options (dict[str, Any]): Parsed command line options.

        """
        self._resolve_cmdline_args(options)
        logger.debug("cmdline_options:\n{}", options)

        if exec_cmd := options.get("execute"):
            exec_cmd = exec_cmd.strip()
        if file_cmd := options.get("file"):
            file_cmd = file_cmd.strip()

        # Convenience: allow -e <file> to behave like -f <file> if the argument
        # is an existing readable file and no explicit -f was provided.
        # This avoids the cryptic parser error when a user mistakenly uses -e with a path.
        if exec_cmd and not file_cmd and os.path.isfile(exec_cmd):
            logger.debug("Detected -e argument refers to a file; switching to file execution mode: {}", exec_cmd)
            file_cmd = exec_cmd
            exec_cmd = None

        self._with_prompt = sys.stdin.isatty() and not (exec_cmd or file_cmd)

        # Set autoconfirm based on input mode (propagated to graphdb)
        self._set_autoconfirm(
            self._determine_autoconfirm(
                exec_cmd,
                file_cmd,
                options,
            )
        )

        # Determine output suppression: suppress when non-interactive and executing from
        # file (-f) or stdin (no -e provided). Single -e one-liners still produce output.
        self._renderer.suppress_output = self._determine_suppress_output(exec_cmd, file_cmd)

        if self._with_prompt and self.show_banner:
            self.banner()

        if not self._graphdb.connect(options):
            return

        # inject db also in graph data
        self._graph_data.db = self._graphdb.db

        if not self._config.get_property("output_format"):
            self._config.set_property(
                "output_format",
                "table" if self._with_prompt else "json",
            )

        if self._with_prompt:
            # wrap all providers, required to get data for command completion
            providers = CLIProviders(
                model_provider=self._graphdb.db.model_provider,
                graphdb_provider=self._graphdb,
                graphdata_provider=self._graph_data,
                var_provider=self._graph_data,
                config_provider=self._config,
            )

            cmdline_prompt = CommandLinePrompt(
                self._prompt_parser,
                providers,
                on_resolve_prompt=lambda: self._graphdb.id,
            )

            cmdline_prompt.runloop(lambda cmdline: self._parse_and_execute(cmdline, False))

            rich.print("[green]Bye!")
        else:
            if exec_cmd:
                self._execute_from_cmdline(exec_cmd)
            elif file_cmd:
                self._execute_from_file(file_cmd)
            else:
                self._execute_from_file(sys.stdin)

    def _execute_from_cmdline(self, cmdline: str):
        parts = cmdline.split(";")

        for cmd in parts:
            cmd = cmd.strip()

            logger.debug("Exec command: {}", cmdline)

            if not self._parse_and_execute(cmd, True):
                break

    def _execute_from_file(self, file: str | TextIO):
        """Execute semicolon-terminated commands from a file or stream.

        Uses the shared CommandReader implementation to support multi-line
        statements, inline semicolon detection, and comment skipping.
        Output is suppressed, beside errors/warnings.
        """

        def exec_commands(reader: CommandReader):
            for cmd in reader:
                logger.debug("Exec command: {}", cmd)
                if not self._parse_and_execute(cmd, True):
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

    def _parse_and_execute(self, cmdline: str, terminate_on_failure: bool) -> bool:
        parse_result: PromptParserResult = self._prompt_parser.parse_prompt(cmdline)

        if parse_result.failed():
            rich.print(f"[red]{parse_result.failed_reason}")
            # continue if from prompt, else terminate execution
            return not terminate_on_failure

        self._prompt_parser.execute_cmds(parse_result, self._execute_cmd, self._pipe_to_shell_cmd)

        return not parse_result.exit_cmd

    def _execute_cmd(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute a command parsed from the command line.

        This method dispatches commands to appropriate handlers using the
        command instance manager directly.

        Args:
            parsed_cmd: The parsed command to execute

        Returns:
            bool: True if the command executed successfully, False otherwise
        """
        if parsed_cmd.require_backend and not self._check_connected_backend():
            # terminate cmd execution
            return False

        action = parsed_cmd.action

        # Check for dynamic prefixes (update_*, delete_*)
        if action.startswith("update"):
            update_command = self._command_manager.get_instance("update_graphobj")
            if update_command:
                return update_command.execute(parsed_cmd)

        if action.startswith("delete"):
            delete_command = self._command_manager.get_instance("delete_graphobj")
            if delete_command:
                return delete_command.execute(parsed_cmd)

        # Look up the command instance directly
        command_instance = self._command_manager.get_instance(action)
        if command_instance:
            return command_instance.execute(parsed_cmd)

        # Unknown command
        raise RuntimeError(f"Action {action} not implemented!")

    def _resolve_cmdline_args(self, options):
        if options.get("json"):
            self._config.set_properties({"output_format": "json"})

        if options.get("table"):
            self._config.set_properties({"output_format": "table"})

    def _pipe_to_shell_cmd(self, parse_result: PromptParserResult):
        process = subprocess.Popen(
            parse_result.shell_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        pipe_result = parse_result.output_from_pipe()

        if pipe_result:
            result_as_json = self._renderer.to_json(pipe_result)

            output, error = process.communicate(result_as_json.encode())

            if process.returncode == 0:
                rich.print(output.decode())
            else:
                rich.print(error.decode())
        else:
            rich.print("[yellow]No input for system command!")

    def _post_processing_cmd(
        self, parsed_cmd: PromptParserCmd, result, set_last_result: bool = True, render_args=None, render_kwargs=None
    ) -> bool:
        if result:
            parsed_cmd.output = result

            if parsed_cmd.is_finalcmd():
                self._renderer.render(result, args=render_args, kwargs=render_kwargs)

                if set_last_result:
                    self._graph_data.last_result = result

            return True
        else:
            return False

    def post_processing_cmd(
        self,
        parsed_cmd: PromptParserCmd,
        result: Any,
        append_to_graph: bool = True,
        render_kwargs: dict[str, Any] | None = None,
    ) -> bool:
        """Post-process command results (CLIRuntime interface implementation)."""
        return self._post_processing_cmd(parsed_cmd, result, append_to_graph, render_kwargs=render_kwargs)

    @property
    def autoconfirm(self) -> bool:
        """Current autoconfirm state (CLIRuntime interface implementation)."""
        return self._autoconfirm

    def set_autoconfirm(self, value: bool) -> bool:
        """Set autoconfirm state and return previous value (CLIRuntime interface implementation)."""
        return self._set_autoconfirm(value)

    def execute_from_file(self, path: str) -> None:
        """Execute commands from a file (CLIRuntime interface implementation)."""
        self._execute_from_file(path)

    @property
    def exec_stats_enabled(self) -> bool:
        """Whether execution statistics are enabled (CLIRuntime interface implementation)."""
        return self._exec_stats

    def set_last_cypher_cmd(self, cmd: str) -> None:
        """Store the last executed cypher command (CLIRuntime interface implementation)."""
        self._last_cypher_cmd = cmd

    def _check_connected_backend(self):
        if not self._graphdb.db:
            rich.print("[red]No backend connected!", file=sys.stderr)
            return False
        else:
            return True

    def _determine_autoconfirm(self, exec_cmd, file_cmd, options) -> bool:
        """Determine auto-confirm behavior.

        Rules:
        * Interactive prompt (tty, no -e/-f): never autoconfirm.
        * File execution (-f) OR stdin (non-tty, no -e): autoconfirm.
        * Single execute command (-e) only autoconfirms with --yes.
        """
        if self._with_prompt:
            return False
        if file_cmd:
            return True
        if not exec_cmd:  # piped stdin
            return True
        return bool(options.get("yes"))

    def _determine_suppress_output(self, exec_cmd, file_cmd) -> bool:
        """Determine whether to suppress result rendering.

        Rules:
        * Interactive prompt: never suppress.
        * Non-interactive file execution (-f): suppress.
        * Non-interactive stdin pipeline (no -e provided): suppress.
        * Single -e one-liner: do not suppress.
        """

        return (not self._with_prompt) and (bool(file_cmd) or not exec_cmd)

    def _set_autoconfirm(self, value: bool) -> bool:
        """Set autoconfirm state for both the CLI and underlying graphdb.

        Returns the previous state to allow caller to restore it easily.

        Args:
            value (bool): New autoconfirm state.

        Returns:
            bool: Previous autoconfirm state.
        """
        prev = self._autoconfirm
        self._autoconfirm = value
        self._graphdb.autoconfirm = value
        return prev

    _CONFIG_PROPS: Final = {
        "autocommit": True,
        "output_format": None,
        "exec_stats": False,
    }

    def _config_setup(self):
        self._config.set_properties(self._CONFIG_PROPS)

    def _config_on_property_change(self, name, value) -> bool:
        match name:
            case "autocommit":
                self._graphdb.autocommit = value
                return True
            case "output_format":
                if value and value.lower() in ("table", "json", "list"):
                    self._renderer.default_format = value
                    return True
            case "exec_stats":
                if isinstance(value, bool):
                    self._exec_stats = value
                    return True
            case _:
                return False

    def _create_prompt_parser(self) -> PromptParser:
        return PromptParser(command_map.CMD_MAP, "[[execute_cypher")
