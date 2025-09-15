"""CLI export module: Handles data export operations from the CLI.

This module provides functionality for exporting graph data to various formats
through the command-line interface.
"""

import sys

import rich

from cypher_graphdb import CypherGraphDB, Graph, utils
from cypher_graphdb.tools import CsvExporter, ExcelExporter, FileExporterOptions


class GraphExporter:
    """CLI handler for graph data export operations."""

    def __init__(self, db: CypherGraphDB) -> None:
        self.db = db

    def export(self, graph: Graph, args, kwargs):
        # rows is a list[dict] produced by RowCollector (formerly a DataFrame); using len(rows)
        def on_export_file(rows, dirname, filename, partname):
            if dirname is not None:
                rich.print(" ", f"[blue]Exporting to directory {dirname}, {len(rows)} graph object(s).")
            if filename is not None:
                partname = partname or ""
                rich.print(" ", f"[blue]Exporting to file {filename} {partname}, {len(rows)} graph object(s).")

        if graph.is_empty:
            rich.print("[yellow]Empty graph could not be exported. Add first cypher results to it (with 'add' or 'add graph').")
            return

        if len(args) == 0:
            rich.print(
                "[red]Please specify a file or directory where to export, and an optional export format!",
                file=sys.stderr,
            )
            return

        filename = args[0]
        dirname, basename, ext = utils.split_path(filename)

        # handle <dir>/*.<ext>
        if basename == "*":
            filename = dirname if dirname else "."

        export_format = utils.resolve_fileformat(ext)

        if not export_format:
            export_format = kwargs.get("format", None)

        if not (exporter := self._resolve_exporter(export_format, args, kwargs)):
            return

        # wire up callbacks
        exporter.on_export_file = on_export_file

        exporter.export(graph, filename)

        rich.print("[green]Successfully exported.")

    def _resolve_exporter(self, export_format, args, kwargs):
        match export_format:
            case "excel":
                exporter = ExcelExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case "csv":
                exporter = CsvExporter(self.db, FileExporterOptions.from_opts(args, kwargs))
            case _:
                rich.print(f"[red]Invalid or unsupported export format '{export_format}'")
                exporter = None

        return exporter
