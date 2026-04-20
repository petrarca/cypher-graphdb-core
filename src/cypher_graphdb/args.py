"""Command-line argument parsing using Typer.

This module provides command-line argument parsing functionality using Typer
for the cypher-graphdb CLI application. CLI args are merged into CLISettings
(and core Settings for connection params) before the CLI app starts.
"""

from importlib.metadata import version
from typing import Annotated

import typer


def _apply_cli_overrides(
    backend: str | None,
    cinfo: str | None,
    graph: str | None,
    read_only: bool,
    log_level: str | None,
    model_path: str | None,
    ignore_model_path: bool,
    execute: str | None,
    file: str | None,
    json_format: bool,
    table_format: bool,
    verbose: bool,
    no_progress: bool,
    yes: bool,
) -> None:
    """Mutate both core Settings and CLISettings with CLI arg overrides.

    CLI args take priority over env vars / .env file. Only non-None / non-False
    values override the defaults already loaded from the environment.
    """
    from cypher_graphdb.cli.settings import get_cli_settings
    from cypher_graphdb.settings import get_settings

    # Core connection settings (shared with library consumers)
    settings = get_settings()
    if backend is not None:
        settings.backend = backend
    if cinfo is not None:
        settings.cinfo = cinfo
    if graph is not None:
        settings.graph = graph
    if read_only:
        settings.read_only = read_only

    # CLI-specific settings
    cli_settings = get_cli_settings()
    if log_level is not None:
        cli_settings.log_level = log_level
    if model_path is not None:
        cli_settings.model_path = model_path

    # Per-invocation flags (always set -- False is a meaningful value here)
    cli_settings.ignore_model_path = ignore_model_path
    cli_settings.execute = execute
    cli_settings.file = file
    cli_settings.json_format = json_format
    cli_settings.table_format = table_format
    cli_settings.verbose = verbose
    cli_settings.no_progress = no_progress
    cli_settings.yes = yes


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        print(version("cypher_graphdb"))
        raise typer.Exit()


def create_main_app() -> typer.Typer:
    """Create the main Typer application for use as the console script entry point."""
    from cypher_graphdb.cli import schema_cmd

    app = typer.Typer(
        name="cypher-graphdb",
        help="CypherGraph CLI - A command-line interface for graph databases",
        add_completion=False,
        rich_markup_mode="markdown",
        invoke_without_command=True,
    )

    # Add schema subcommand
    app.add_typer(schema_cmd.app, name="schema")

    @app.callback(invoke_without_command=True)
    def main(
        ctx: typer.Context,
        version: Annotated[  # noqa: ARG001
            bool | None,
            typer.Option("--version", "-v", callback=version_callback, is_eager=True, help="Show version and exit"),
        ] = None,
        json_format: Annotated[bool, typer.Option("--json", "-j", help="Render output in JSON format.")] = False,
        table_format: Annotated[bool, typer.Option("--table", "-t", help="Render output as a table.")] = False,
        execute: Annotated[
            str | None, typer.Option("--execute", "-e", help="Execute given command, might be split with ';'")
        ] = None,
        file: Annotated[str | None, typer.Option("--file", "-f", help="Execute commands by given file.")] = None,
        backend: Annotated[
            str | None,
            typer.Option("--backend", "-b", help="Backend type. Precedence: CLI args > .env > defaults."),
        ] = None,
        cinfo: Annotated[
            str | None,
            typer.Option("--cinfo", "-c", help="Connection string. CLI args > .env > defaults."),
        ] = None,
        graph: Annotated[
            str | None,
            typer.Option("--graph", "-g", help="Graph name. CLI args > .env > defaults."),
        ] = None,
        read_only: Annotated[
            bool,
            typer.Option("--read-only", "-r", help="Connect in read-only mode (prevents data modifications)."),
        ] = False,
        log_level: Annotated[
            str | None, typer.Option("--log-level", "-l", help="Log level (INFO, DEBUG, TRACE, ...) [default: INFO]")
        ] = None,
        yes: Annotated[
            bool,
            typer.Option("--yes", "-y", help="Automatically confirm all operations."),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", help="Show each command and summary (file mode)."),
        ] = False,
        no_progress: Annotated[
            bool,
            typer.Option("--no-progress", help="Disable progress bar (file mode)."),
        ] = False,
        model_path: Annotated[
            str | None,
            typer.Option(
                "--model-path",
                "-m",
                help="Path to Python model module/directory to auto-load on startup. Overrides CGDB_MODEL_PATH env var.",
            ),
        ] = None,
        ignore_model_path: Annotated[
            bool,
            typer.Option("--ignore-model-path", help="Ignore CGDB_MODEL_PATH env var (suppress auto-loading)."),
        ] = False,
    ) -> None:
        """CypherGraph CLI - A command-line interface for graph databases."""
        # Only run interactive CLI if no subcommand was invoked
        if ctx.invoked_subcommand is not None:
            return

        # Import here to avoid circular imports
        from cypher_graphdb.main import main as main_func

        _apply_cli_overrides(
            backend=backend,
            cinfo=cinfo,
            graph=graph,
            read_only=read_only,
            log_level=log_level,
            model_path=model_path,
            ignore_model_path=ignore_model_path,
            execute=execute,
            file=file,
            json_format=json_format,
            table_format=table_format,
            verbose=verbose,
            no_progress=no_progress,
            yes=yes,
        )

        main_func(show_banner=True)

    return app


# Create the main app instance for the console script
app = create_main_app()


def typer_main() -> None:
    """Entry point for the typer-based console script."""
    app()


if __name__ == "__main__":
    typer_main()
