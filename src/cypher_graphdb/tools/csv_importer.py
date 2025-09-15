"""CSV file importer using DuckDB row streaming.

Refactored to remove pandas usage. Utilizes DuckDBSource + TabularImporter
to stream rows in batches.
"""

from cypher_graphdb import CypherGraphDB, utils

from .duckdb_source import DuckDBSource
from .file_importer import FileImporter
from .tabular_importer import TabularImporter


class CsvImporter(FileImporter):
    """CSV file importer for graph data (DuckDB-based)."""

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

        # Escape single quotes for SQL literal
        escaped = filename.replace("'", "''")
        sql = f"SELECT * FROM read_csv_auto('{escaped}', HEADER=TRUE)"
        source = DuckDBSource(sql)
        importer = TabularImporter(self.db)
        try:
            importer.load(source, label)
        finally:
            source.close()
