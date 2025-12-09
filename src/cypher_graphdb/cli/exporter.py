"""CLI export module: Handles data export operations from the CLI.

This module provides functionality for exporting graph data to various formats
through the command-line interface.
"""

import json
import sys

import rich

from cypher_graphdb import CypherGraphDB, Graph, utils
from cypher_graphdb.models import TreeResult
from cypher_graphdb.tools import CsvExporter, ExcelExporter, FileExporterOptions, HierarchicalExporter


class GraphExporter:
    """CLI handler for graph data export operations."""

    def __init__(self, db: CypherGraphDB) -> None:
        self.db = db

    def _make_callback(self, item_type: str = "graph object"):
        """Create export callback for progress reporting."""

        def on_export_file(rows, dirname, filename, partname):
            if dirname is not None:
                rich.print(" ", f"[blue]Exporting to directory {dirname}, {rows} {item_type}(s).")
            if filename is not None:
                partname = partname or ""
                rich.print(" ", f"[blue]Exporting to file {filename} {partname}, {rows} {item_type}(s).")

        return on_export_file

    def _validate_args(self, args) -> str | None:
        """Validate export arguments and return filename or None if invalid."""
        if len(args) == 0:
            rich.print("[red]Please specify a file where to export!", file=sys.stderr)
            return None
        return args[0]

    def _run_export(self, export_fn, success_msg: str):
        """Run export with standard error handling."""
        try:
            export_fn()
            rich.print(f"[green]{success_msg}")
        except (ValueError, RuntimeError, FileNotFoundError, OSError, json.JSONDecodeError) as e:
            rich.print(f"[red]Export failed: {e}")

    def export(self, graph: Graph, args, kwargs):
        """Export graph to file."""
        if graph.is_empty:
            rich.print("[yellow]Empty graph could not be exported. Add first cypher results to it (with 'add' or 'add graph').")
            return

        if not (filename := self._validate_args(args)):
            return

        dirname, basename, ext = utils.split_path(filename)
        if basename == "*":
            filename = dirname if dirname else "."

        export_format = utils.resolve_fileformat(ext)

        if not (exporter := self._resolve_exporter(export_format, args, kwargs)):
            rich.print(f"[red]Unsupported export format: '{export_format}'", file=sys.stderr)
            return

        exporter.on_export_file = self._make_callback("graph object")
        self._run_export(lambda: exporter.export(graph, filename), "Successfully exported.")

    def export_tree(self, tree_result: TreeResult, args, kwargs):
        """Export TreeResult directly as tree structure (JSON/YAML only)."""
        if not tree_result:
            rich.print("[yellow]Empty tree could not be exported.")
            return

        if not (filename := self._validate_args(args)):
            return

        _, _, ext = utils.split_path(filename)
        export_format = utils.resolve_fileformat(ext)

        if export_format not in ("json", "yaml"):
            rich.print(f"[red]'{export_format.upper()}' does not support tree exports.", file=sys.stderr)
            return

        exporter = HierarchicalExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
        exporter.on_export_file = self._make_callback("root node")
        self._run_export(lambda: exporter.export_tree_result(tree_result, filename), "Successfully exported tree.")

    def _resolve_exporter(self, export_format, args, kwargs):
        match export_format:
            case "excel":
                exporter = ExcelExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case "csv":
                exporter = CsvExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case "json":
                exporter = HierarchicalExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case "yaml":
                exporter = HierarchicalExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case _:
                rich.print(f"[red]Invalid or unsupported export format: '{export_format}'")
                exporter = None

        return exporter
