"""JSON file importer using hierarchical import logic.

Provides a file-specific wrapper for JSON import functionality that integrates
with the existing file importer architecture.
"""

from cypher_graphdb import CypherGraphDB

from .file_importer import FileImporter
from .hierarchical_importer import HierarchicalImporter
from .json_yaml_data_source import JsonYamlDataSource


class JsonImporter(FileImporter):
    """JSON file importer for graph data (hierarchical processing).

    This importer handles JSON files containing nested graph data structures.
    It uses the HierarchicalImporter to process the data with two-phase
    import logic that properly handles node dependencies and relations.
    """

    def __init__(self, db: CypherGraphDB):
        """Initialize JSON importer.

        Args:
            db: CypherGraphDB instance to import data into.
        """
        super().__init__(db)
        self.valid_file_extensions = (".json",)
        self.data_handler = JsonYamlDataSource()
        self._hierarchical_importer = HierarchicalImporter(db)
        self.nodes_created = 0
        self.edges_created = 0

    def load_from_file(self, filename: str):
        """Load graph data from a JSON file.

        Args:
            filename: Path to the JSON file to import.
        """
        try:
            # Load JSON data
            self.data_handler.load_file(filename)

            # Import using hierarchical logic
            self._hierarchical_importer.load(filename)

            # Update statistics from hierarchical importer
            self.nodes_created = self._hierarchical_importer.nodes_created
            self.edges_created = self._hierarchical_importer.edges_created

        except Exception as ex:
            raise RuntimeError(f"Failed to import JSON file {filename}: {ex}") from ex

    def load(self, file_or_dirname: str, recursive: bool = False) -> list[str]:
        """Load JSON data from file(s) or directory.

        Args:
            file_or_dirname: Path to file or directory to import.
            recursive: Whether to scan directories recursively.

        Returns:
            List of processed filenames.
        """
        return self._hierarchical_importer.load(file_or_dirname, recursive)

    def statistics(self) -> dict:
        """Get import statistics.

        Returns:
            Dictionary with import statistics.
        """
        return {
            "nodes_created": getattr(self, "nodes_created", 0),
            "edges_created": getattr(self, "edges_created", 0),
        }


__all__ = [
    "JsonImporter",
]
