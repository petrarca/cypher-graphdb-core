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
        collector = RowCollector(self.db, with_label=self.opts.with_label)
        try:
            for label, entities in items:
                basename = self._resolve_basename(entities[0], label)

                target_filename = filename
                if dirname:
                    target_filename = f"{dirname}/{basename}.csv"

                rows = collector.collect(entities)

                self.on_export_file(rows, dirname, target_filename, None)

                if rows:
                    import csv

                    with open(target_filename, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                        writer.writeheader()
                        writer.writerows(rows)
        finally:
            collector.close()

    def _check_filename(self, items, filename):
        assert items
        if filename and len(items) > 1:
            raise RuntimeError("Directory is required if more than one graph object type is exported in CSV file format")

            # TODO check if filename starts or ends with a prefix, based on configuation
            # basename = "QQQ"
