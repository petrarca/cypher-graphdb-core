"""Main entry point for the cypher-graphdb CLI application."""

import sys

from loguru import logger

import cypher_graphdb
from cypher_graphdb.cli.app import CypherGraphCLI
from cypher_graphdb.cli.settings import get_cli_settings


def main(show_banner: bool = True) -> None:
    """Main entry point for the cypher-graphdb CLI application.

    Reads all configuration from CLISettings (already populated from CLI args
    by args.py before this function is called).

    Args:
        show_banner: If True, display the banner at the start of the CLI.
    """
    cli_settings = get_cli_settings()

    # Enable logging for cypher_graphdb
    logger.enable(cypher_graphdb.__name__)
    logger.remove()
    logger.add(sys.stderr, level=cli_settings.log_level.upper())

    cli_app = CypherGraphCLI(show_banner=show_banner)

    try:
        if cli_settings.log_level.upper() in ("TRACE", "DEBUG"):
            cli_app.run_catched()
        else:
            cli_app.run()
    except Exception as e:
        logger.error("CLI application failed: {}", e)
        sys.exit(1)


def run(show_banner: bool = True) -> None:
    """Console script entry point for the cypher-graphdb CLI."""
    main(show_banner=show_banner)


if __name__ == "__main__":
    main(show_banner=False)
