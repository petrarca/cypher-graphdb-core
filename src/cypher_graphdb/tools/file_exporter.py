"""File exporter module: Base classes for file-based export functionality.

This module provides the base FileExporter class and FileExporterOptions
for exporting graph data to various file formats.
"""

import os
from collections.abc import Callable
from typing import Any

import cypher_graphdb.config as config
from cypher_graphdb import CypherGraphDB, Graph, GraphNode
from cypher_graphdb.options import TypedOptionModel

from .base_exporter import BaseExporter


class FileExporterOptions(TypedOptionModel):
    """Configuration options for file exporters.

    Attributes:
        with_label: Include label column in exported data.
        chunk_size: Number of entities to process in each chunk.
    """

    with_label: bool = True
    chunk_size: int = 50000


class FileExporter(BaseExporter):
    """Base class for file-based data exporters.

    Provides common functionality for exporting graph data to files,
    including file validation and graph building capabilities.
    """

    def __init__(self, db, opts: FileExporterOptions = None):
        """Initialize file exporter.

        Args:
            db: CypherGraphDB instance to export data from.
            opts: Export options configuration.
        """
        if opts is None:
            opts = FileExporterOptions()

        assert isinstance(db, CypherGraphDB)
        self.db = db
        self.opts = opts
        self.valid_file_extensions = set()

        self.on_export_file: Callable = lambda df, dirname, filename, partname: None

    def statistics(self) -> None:
        """Get export statistics.

        Returns:
            Export statistics (None for base implementation).
        """
        return None

    def export(self, source: Graph | str | list[Any] | tuple[Any, ...], file_or_dirname: str):
        """Export graph data to file or directory.

        Args:
            source: Graph data to export (Graph, query string, or list/tuple).
            file_or_dirname: Target file path or directory path.
        """
        if not isinstance(source, Graph):
            source = self._build_graph(source)

        if self.is_valid_file(file_or_dirname):
            self.export_to_file(source, file_or_dirname, False)
        else:
            os.makedirs(file_or_dirname, exist_ok=True)
            self.export_to_dir(source, file_or_dirname)

    def export_to_file(self, graph: Graph, filename: str, validate_filename: bool = True):
        """Export graph data to a single file.

        Args:
            graph: Graph data to export.
            filename: Target file path.
            validate_filename: Whether to validate file extension.
        """
        if validate_filename:
            self.is_valid_file(filename)

        if items := graph.grouped_entities().items():
            self._write_to_file(items, None, filename)

    def export_to_dir(self, graph: Graph, dirname: str):
        """Export graph data to multiple files in a directory.

        Args:
            graph: Graph data to export.
            dirname: Target directory path.
        """
        if items := graph.grouped_entities().items():
            self._write_to_file(items, dirname, None)

    def _write_to_file(self, items, dirname, filename):
        # neds to be implemented by subclasses
        pass

    def _resolve_basename(self, graph_obj, label):
        if isinstance(graph_obj, GraphNode):
            base_name = label
        else:
            base_name = f"_{label}" if config.EDGE_FILE_NAME == "prefix" else f"{label}_"

        return base_name

    def _build_graph(self, source):
        wrapped_source = source if isinstance(source, list | tuple) else [source]

        result = Graph()

        if not wrapped_source:
            return result

        if isinstance(wrapped_source[0], str):
            for cmd in wrapped_source:
                result.merge(self.db.execute(cmd, unnest_result=True))
        else:
            result.merge(wrapped_source)

        return result

    def is_valid_file(self, filename: str) -> bool:
        """Check if filename is valid for export.

        Args:
            filename: File path to validate.

        Returns:
            True if filename is valid for export.

        Raises:
            RuntimeError: If filename is invalid or unsupported.
        """
        if not self.valid_file_extensions:
            raise RuntimeError(f"Missing valid file extensions in {self.__class__.__name__}")

        if any(c in filename for c in ["?", "*"]):
            raise RuntimeError(f"Invalid export file name: {filename}")

        if os.path.isdir(filename):
            return False

        _, ext = os.path.splitext(filename)

        if ext and ext not in self.valid_file_extensions:
            raise RuntimeError(f"Unsupported file extension: {ext}, valid ones are {self.valid_file_extensions}!")

        return bool(ext)
