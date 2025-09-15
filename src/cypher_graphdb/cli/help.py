"""CLI help module: Provides help and documentation for CLI commands.

This module handles the display of help information, command documentation,
and usage examples within the CLI interface.
"""

import os
import sys

import rich
from rich.markdown import Markdown

from cypher_graphdb.cli.prompt import PromptParser


def show_help(args, prompt_parser: PromptParser) -> bool:
    def resolve_help_filename(action, cmd):
        nonlocal prompt_parser

        if action:
            if action == prompt_parser.default_action:
                rich.print(f"[red]Unknown command '{cmd}'. Enter <help> to get an overview!", file=sys.stderr)
                return None

            return f"{action}.md"

        return "_overview.md"

    def load_help_md(help_filename):
        # expand to full path name
        help_filename = f"{os.path.dirname(os.path.abspath(__file__))}/help/{help_filename}"

        if os.path.exists(help_filename):
            with open(help_filename, encoding="UTF-8") as file:
                return file.read()

        return None

    # parse the command, for which help is requested
    cmd = None
    parsed_cmd = prompt_parser.parse_cmd(cmd := " ".join(args)) if args else None

    if not (help_filename := resolve_help_filename(parsed_cmd.action if parsed_cmd else None, cmd)):
        return False

    if (content := load_help_md(help_filename)) is None:
        rich.print(f"[red]Could not find help for {args}!", file=sys.stderr)
        return False

    md = Markdown(content)
    rich.print(md)

    return True
