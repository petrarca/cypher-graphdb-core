"""CSV file importer using standard library row streaming.

Refactored to remove DuckDB usage. Utilizes CsvSource + TabularImporter
to stream rows in batches without external dependencies.
"""

from cypher_graphdb import CypherGraphDB, utils

from .csv_source import CsvSource
from .file_importer import FileImporter
from .tabular_importer import TabularImporter


class CsvImporter(FileImporter):
    """CSV file importer for graph data (standard library-based)."""

    def __init__(self, db: CypherGraphDB):
        """Initialize CSV importer.

        Args:
            db: CypherGraphDB instance to import data into.

        """
        super().__init__(db)
        self.valid_file_extensions = (".csv",)

    def load_from_file(self, filename: str):
        """Load graph data from a CSV file (streamed)."""
        self.on_import_file(filename, None)

        # Resolve fallback label from file basename
        _, label, _ = utils.split_path(filename)

        source = CsvSource(filename)
        importer = TabularImporter(self.db)
        try:
            importer.load(source, label)
        finally:
            source.close()
