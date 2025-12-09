"""CLI import module: Handles data import operations from the CLI.

This module provides functionality for importing graph data from various formats
through the command-line interface.
"""

import sys

import rich
from prompt_toolkit import prompt

import cypher_graphdb.utils as utils
from cypher_graphdb import CypherGraphDB
from cypher_graphdb.tools import CsvImporter, ExcelImporter, JsonImporter, YamlImporter


class GraphImporter:
    """CLI handler for graph data import operations."""

    def __init__(self, db: CypherGraphDB, default_format="excel", autoconfirm=False) -> None:
        self.db = db
        self.defaut_format = default_format
        self.autoconfirm = autoconfirm

    def load(self, args, kwargs):
        def on_import_file(filename, partname, count=None):
            partname = partname or ""
            if count is not None:
                rich.print(" ", f"[blue]Importing from {filename} {partname}, {count} graph object(s).")
            else:
                rich.print(" ", f"[blue]Importing from {filename} {partname}")

        def on_invalid_file(filename):
            rich.print(" ", f"[yellow]Invalid file to import: File {filename} skipped!")

        if len(args) == 0:
            rich.print(
                "[red]Please specify a file or directory where to importfrom, and an optional import format!",
                file=sys.stderr,
            )
            return

        importer = self._resolve_importer(args[0], kwargs)

        if not importer:
            return

        if not self.autoconfirm:
            result = prompt(f"Do you want to import from {args[0]} [yN]? ")
            if result.lower() not in ("y", "yes"):
                return
        else:
            rich.print(f"[blue]Auto-confirming import from {args[0]}")

        # wire up callbacks
        importer.on_import_file = on_import_file
        importer.on_invalid_file = on_invalid_file

        if importer.load(args[0]):
            rich.print("[green]Successfully imported.")
        else:
            rich.print("[yellow]No files found to import!")

    def _resolve_importer(self, file_or_dirname, kwargs):
        # Check if autoconfirm is passed in kwargs
        if "autoconfirm" in kwargs:
            self.autoconfirm = kwargs["autoconfirm"]

        import_format = utils.resolve_fileformat(file_or_dirname)
        if not import_format:
            import_format = kwargs.get("format", self.defaut_format)

        importer = self._create_importer_by_format(import_format)
        if importer is not None:
            importer.on_import_file = self._create_import_file_callback()

        return importer

    def _create_import_file_callback(self):
        """Create callback for import file progress reporting."""

        def on_import_file(filename, partname, count=None):
            if filename is not None:
                partname = partname or ""
                if count is not None:
                    rich.print(" ", f"[blue]Importing from file {filename} {partname}, {count} graph object(s).")
                else:
                    rich.print(" ", f"[blue]Importing from file {filename} {partname}")

        return on_import_file

    def _create_importer_by_format(self, import_format):
        """Create appropriate importer instance based on format."""
        importers = {
            "excel": ExcelImporter,
            "csv": CsvImporter,
            "json": JsonImporter,
            "yaml": YamlImporter,
        }

        importer_class = importers.get(import_format)
        if importer_class:
            return importer_class(self.db)

        rich.print(f"[red]Invalid or unsupported import format: '{import_format}'")
        return None
