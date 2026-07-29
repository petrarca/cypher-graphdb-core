"""Excel format registration (requires the ``excel`` extra)."""

from cypher_graphdb.formatprovider import data_format
from cypher_graphdb.tools.excel_exporter import ExcelExporter
from cypher_graphdb.tools.excel_importer import ExcelImporter


@data_format("excel", extensions=(".xlsx", ".xls"), extra="excel")
class ExcelFormat:
    """Excel workbooks, via openpyxl."""

    importer = ExcelImporter
    exporter = ExcelExporter
