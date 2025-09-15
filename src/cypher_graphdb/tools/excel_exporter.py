"""Excel exporter module: Export graph data to Excel files.

This module provides the ExcelExporter class for exporting graph data
from a CypherGraphDB instance to Excel files, with support for multiple
worksheets.
"""

import os
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
        collector = RowCollector(self.db, with_label=self.opts.with_label)
        try:
            for label, entities in items:
                sheetname = self._resolve_basename(entities[0], label)

                target_filename = filename
                if dirname:
                    target_filename = f"{dirname}/{sheetname}.xlsx"

                if not os.path.exists(target_filename):
                    wb = xl.Workbook()
                    wb.save(target_filename)
                    delete_sheet = wb.sheetnames[0]
                else:
                    delete_sheet = None

                rows = collector.collect(entities)
                self.on_export_file(rows, dirname, target_filename, f"[{sheetname}]")

                if rows:
                    from openpyxl import load_workbook

                    wb2 = load_workbook(target_filename)
                    if sheetname in wb2.sheetnames:
                        ws = wb2[sheetname]
                        wb2.remove(ws)
                    ws = wb2.create_sheet(sheetname)
                    headers = list(rows[0].keys())
                    ws.append(headers)
                    for r in rows:
                        ws.append([r.get(h) for h in headers])
                    wb2.save(target_filename)

                if delete_sheet is not None:
                    wb = xl.load_workbook(target_filename)
                    wb.remove(wb[delete_sheet])
                    wb.save(target_filename)
        finally:
            collector.close()
