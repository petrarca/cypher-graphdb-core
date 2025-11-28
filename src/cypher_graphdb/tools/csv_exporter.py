"""CSV exporter module: Export graph data to CSV files.

This module provides the CsvExporter class for exporting graph data
from a CypherGraphDB instance to CSV files.
"""

from typing import Any

from .file_exporter import FileExporter, FileExporterOptions
from .row_collector import RowCollector


class CsvExporter(FileExporter):
    """Export graph data to CSV files.

    A specialized file exporter that writes graph nodes and edges
    to CSV format files.
    """

    def __init__(self, db: Any, opts: FileExporterOptions = None) -> None:
        """Initialize CSV exporter.

        Args:
            db: CypherGraphDB instance to export data from.
            opts: Export options configuration.
        """
        super().__init__(db)
        self.valid_file_extensions = ".csv"

    def _write_to_file(self, items, dirname, filename):
        if not items:
            return

        self._check_filename(items, filename)
        # Reuse one RowCollector so node cache persists across groups
        chunk_size = getattr(self.opts, "chunk_size", 50000)
        collector = RowCollector(self.db, with_label=self.opts.with_label, chunk_size=chunk_size)
        try:
            for label, entities in items:
                basename = self._resolve_basename(entities[0], label)

                target_filename = filename
                if dirname:
                    target_filename = f"{dirname}/{basename}.csv"

                # Always use streaming for simplicity and consistency
                self._write_streaming(collector, entities, target_filename)

        finally:
            collector.close()

    def _write_streaming(self, collector, entities, target_filename):
        """Streaming export for all datasets."""
        import csv

        # Get headers from first chunk
        chunk_generator = collector.collect_streaming(entities)
        first_chunk = next(chunk_generator)
        if not first_chunk:
            return

        all_fieldnames = set()
        for row in first_chunk:
            all_fieldnames.update(row.keys())

        with open(target_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
            writer.writeheader()

            # Write first chunk
            writer.writerows(first_chunk)

            # Write remaining chunks
            for chunk_rows in chunk_generator:
                writer.writerows(chunk_rows)
                f.flush()  # Ensure data is written immediately

        self.on_export_file(None, None, target_filename, None)

    def _check_filename(self, items, filename):
        assert items
        if filename and len(items) > 1:
            raise RuntimeError("Directory is required if more than one graph object type is exported in CSV file format")
