"""Main entry point for the cypher-graphdb CLI application.

This module handles application initialization and delegates to the CLI app
with parsed options from the argument parser.
"""

import sys
from typing import Any

from loguru import logger

import cypher_graphdb
from cypher_graphdb.cli.app import CypherGraphCLI


def main(show_banner: bool = True, parsed_options: dict[str, Any] | None = None) -> None:
    """Main entry point for the cypher-graphdb CLI application.

    This function accepts pre-parsed options and passes them to the CLI application.

    Args:
        show_banner (bool): If True, display the banner at the start of the CLI.
        parsed_options (dict[str, Any]): Pre-parsed options from the CLI argument parser.
    """

    # Use the provided parsed options
    options = parsed_options or {}

    # Enable logging for cypher_graphdb
    logger.enable(cypher_graphdb.__name__)

    # Configure log level if specified
    if loglevel := options.get("log_level"):
        logger.remove()
        logger.add(sys.stderr, level=loglevel.upper())

    # Create and run the CLI application
    cli_app = CypherGraphCLI(show_banner=show_banner)

    try:
        if loglevel and loglevel.upper() in ("TRACE", "DEBUG"):
            cli_app.run_catched(options)
        else:
            cli_app.run(options)
    except Exception as e:
        logger.error(f"CLI application failed: {e}")
        sys.exit(1)


def run(show_banner: bool = True) -> None:
    """Console script entry point for the cypher-graphdb CLI.

    This function is called when the 'cypher-graphdb' command is executed.

    Args:
        show_banner (bool): If True, display the banner at the start of the CLI.
    """
    main(show_banner=show_banner)


if __name__ == "__main__":
    main(show_banner=False)
