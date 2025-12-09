"""file_importer module: Base file import functionality.

Provides FileImporter class as a base for importing graph data from various
file formats with support for directory scanning and batch processing.
"""

import glob
import os
import re
from collections.abc import Callable

import cypher_graphdb.config as config
from cypher_graphdb import CypherGraphDB

from .base_importer import BaseImporter


class FileImporter(BaseImporter):
    """Base class for file-based graph data importers.

    Provides common functionality for importing from files including
    directory scanning, file filtering, and batch processing.
    """

    def __init__(self, db: CypherGraphDB):
        """Initialize file importer.

        Args:
            db: CypherGraphDB instance to import data into.

        """
        assert isinstance(db, CypherGraphDB)
        self.db = db
        self.valid_file_extensions = set()

        self.on_import_file: Callable = lambda filename, partname, count=None: None
        self.on_invalid_file: Callable = lambda filename: None

    def statistics(self) -> None:
        """Get import statistics.

        Returns:
            None (to be implemented by subclasses).

        """
        return None

    def load(self, file_or_dirname: str, recursive: bool = False) -> list[str]:
        """Load data from file(s) or directory.

        Args:
            file_or_dirname: Path to file or directory to import.
            recursive: Whether to scan directories recursively.

        Returns:
            List of processed filenames.

        Raises:
            RuntimeError: If no valid file extensions are configured.

        """
        if not self.valid_file_extensions:
            raise RuntimeError(f"{self.__class__.__name__} has no valid file extension(s)!")

        filenames = self.resolve_filenames(file_or_dirname, recursive=recursive)

        # disable temporarily autocommit to allow batching
        _autocommit = self.db._backend.autocommit
        self.db._backend.autocommit = False

        try:
            for filename in filenames:
                self.load_from_file(filename)
        finally:
            self.db._backend.autocommit = _autocommit

        return filenames

    def load_from_file(self, filename: str):
        """Load data from a specific file.

        Args:
            filename: Path to the file to import.

        Note:
            To be implemented by subclasses.

        """
        raise NotImplementedError("Subclasses must implement load_from_file() method")

    def resolve_filenames(self, file_or_dirname: str, recursive: bool) -> list[str]:
        """Resolve file paths from file or directory specification.

        Args:
            file_or_dirname: Path to file or directory.
            recursive: Whether to scan directories recursively.

        Returns:
            List of resolved file paths, sorted appropriately.

        """

        def match_file(filename):
            if os.path.splitext(filename)[1] in self.valid_file_extensions:
                return filename

            return None

        def sorting_key(filename):
            # sort file names with _ at prefix or postfix, depending on the configuration.
            # By convention those contain edges, which needs to be loaded after nodes.
            _, base_filename = os.path.split(filename)

            if config.EDGE_FILE_NAME == "prefix":
                # Files starting with _ (edges) should be sorted last
                priority = 1 if base_filename.startswith("_") else 0
            else:
                # Files ending with _ (edges) should be sorted last
                priority = 1 if base_filename.endswith("_") else 0

            return (priority, base_filename)

        if os.path.isfile(file_or_dirname):
            return [match_file(file_or_dirname)]
        else:
            result = []

            if re.search(r"[\*\?]", file_or_dirname):
                patterns = [file_or_dirname]
            else:
                patterns = [f"{file_or_dirname}/*" + ext for ext in self.valid_file_extensions]

            for pattern in patterns:
                for filename in glob.iglob(pattern, recursive=recursive):
                    if match_file(filename):
                        result.append(filename)
                    else:
                        self.on_invalid_file(filename)

        result = sorted(result, key=sorting_key) if len(result) > 1 else result

        return result
