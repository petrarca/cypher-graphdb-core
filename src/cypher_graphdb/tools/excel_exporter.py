"""Excel exporter module: Export graph data to Excel files.

This module provides the ExcelExporter class for exporting graph data
from a CypherGraphDB instance to Excel files, with support for multiple
worksheets.
"""

from typing import Any

import openpyxl as xl

from .file_exporter import FileExporter, FileExporterOptions
from .row_collector import RowCollector


class ExcelExporter(FileExporter):
    """Export graph data to Excel files.

    A specialized file exporter that writes graph nodes and edges
    to Excel format files, with each label type in a separate worksheet.
    """

    def __init__(self, db: Any, opts: FileExporterOptions = None) -> None:
        """Initialize Excel exporter.

        Args:
            db: CypherGraphDB instance to export data from.
            opts: Export options configuration.
        """
        super().__init__(db, opts)
        self.valid_file_extensions = (".xlsx", ".xls")

    def _write_to_file(self, items, dirname, filename):
        # Reuse one RowCollector so node cache persists across groups (nodes then edges)
        chunk_size = getattr(self.opts, "chunk_size", 50000)
        collector = RowCollector(self.db, with_label=self.opts.with_label, chunk_size=chunk_size)
        try:
            for label, entities in items:
                sheetname = self._resolve_basename(entities[0], label)

                target_filename = filename
                if dirname:
                    target_filename = f"{dirname}/{sheetname}.xlsx"

                # Always use chunked write-only mode for simplicity and memory efficiency
                self._write_chunked(collector, entities, target_filename, sheetname)

        finally:
            collector.close()

    def _write_chunked(self, collector, entities, target_filename, sheetname):
        """Chunked Excel export using write-only mode for all datasets."""
        # Create new workbook in write-only mode for memory efficiency
        wb = xl.Workbook(write_only=True)
        ws = wb.create_sheet(sheetname)

        # Process entities and write in chunks
        headers_written = False

        for chunk_rows in collector.collect_streaming(entities):
            if not chunk_rows:
                continue

            if not headers_written:
                headers = list(chunk_rows[0].keys())
                ws.append(headers)
                headers_written = True

            # Write chunk rows
            for row_dict in chunk_rows:
                row_data = [row_dict.get(h) for h in headers]
                ws.append(row_data)

        # Save the workbook
        wb.save(target_filename)
        self.on_export_file(None, None, target_filename, f"[{sheetname}]")
