"""CSV format registration (stdlib only)."""

from cypher_graphdb.formatprovider import data_format
from cypher_graphdb.tools.csv_exporter import CsvExporter
from cypher_graphdb.tools.csv_importer import CsvImporter


@data_format("csv", extensions=(".csv",))
class CsvFormat:
    """Comma-separated values, via the standard library."""

    importer = CsvImporter
    exporter = CsvExporter
