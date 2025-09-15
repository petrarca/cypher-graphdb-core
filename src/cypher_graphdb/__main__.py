"""Main entry point for the cypher-graphdb package.

This module allows the package to be executed directly with `python -m cypher_graphdb`.
"""

from cypher_graphdb.args import typer_main

if __name__ == "__main__":
    typer_main()
