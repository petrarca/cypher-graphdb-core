"""CLI banner module: Display application banner and branding.

This module provides functionality for displaying the application banner
and logo in the command-line interface.
"""

import rich


def show_banner():
    """_summary_"""
    # for whatever reason, if import the "art-module" is put at the top of the file
    # starting debugging took extremly long in VSCode. This is reproducible in a small test:
    # --
    # import art
    # print "loaded"
    # --
    # Problem occurs when this code is debugged in VSCode. PyCharm works.
    #
    import art

    rich.print("[yellow]" + art.text2art("Cypher GraphDB"))
    rich.print()
    rich.print("[yellow]Enter 'help' or 'h' to get help.")
    rich.print("[yellow]Enter [blue]<Esc>[/blue] and then [blue]<Return>[/blue] to add new line for multiline editing.")
    rich.print()
