"""Command-line argument parsing using Typer.

This module provides command-line argument parsing functionality using Typer
for the cypher-graphdb CLI application.
"""

from typing import Annotated

import typer

from cypher_graphdb import __about__


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        print(__about__.__version__)
        raise typer.Exit()


def create_main_app() -> typer.Typer:
    """Create the main Typer application for use as the console script entry point."""
    app = typer.Typer(
        name="cypher-graphdb",
        help="CypherGraph CLI - A command-line interface for graph databases",
        add_completion=False,
        rich_markup_mode="markdown",
    )

    @app.command()
    def main(
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
            typer.Option(
                "--backend",
                "-b",
                help="Backend type. Precedence: CLI args > .env > defaults.",
            ),
        ] = None,
        cinfo: Annotated[
            str | None,
            typer.Option(
                "--cinfo",
                "-c",
                help="Connection string. CLI args > .env > defaults.",
            ),
        ] = None,
        graph: Annotated[
            str | None,
            typer.Option(
                "--graph",
                "-g",
                help="Graph name. CLI args > .env > defaults.",
            ),
        ] = None,
        read_only: Annotated[
            bool,
            typer.Option(
                "--read-only",
                "-r",
                help="Connect in read-only mode (prevents data modifications).",
            ),
        ] = False,
        log_level: Annotated[
            str | None, typer.Option("--log-level", "-l", help="Log level (INFO, DEBUG, TRACE, ...) [default: INFO]")
        ] = "INFO",
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Automatically confirm all operations without prompting.")] = False,
    ) -> None:
        """CypherGraph CLI - A command-line interface for graph databases."""
        # Import here to avoid circular imports
        from cypher_graphdb.main import main as main_func

        # Create options dict
        options = {
            "json": json_format,
            "table": table_format,
            "execute": execute,
            "file": file,
            "backend": backend,
            "cinfo": cinfo,
            "graph": graph,
            "read_only": read_only,
            "log_level": log_level,
            "yes": yes,
        }

        # Filter out None and False values
        filtered_options = {k: v for k, v in options.items() if v is not None and v is not False}

        # Call the main function with parsed options
        main_func(show_banner=True, parsed_options=filtered_options)

    return app


# Create the main app instance for the console script
app = create_main_app()


def typer_main() -> None:
    """Entry point for the typer-based console script."""
    app()


if __name__ == "__main__":
    typer_main()
