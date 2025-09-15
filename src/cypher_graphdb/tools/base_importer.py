"""base_importer module: Abstract base class for data importers.

Provides the BaseImporter abstract class that defines the interface
for importing graph data from external sources.
"""

from abc import ABC, abstractmethod


class BaseImporter(ABC):
    """Abstract base class for all data importers.

    Defines the common interface that all importer implementations
    must follow for loading graph data from external sources.
    """

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the importer instance."""
        pass

    @abstractmethod
    def statistics(self):
        """Get import statistics.

        Returns:
            Statistics about the import operation.

        """
        pass
