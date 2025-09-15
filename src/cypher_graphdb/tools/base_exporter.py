"""base_exporter module: Abstract base class for data exporters.

Provides the BaseExporter abstract class that defines the interface
for exporting graph data to external formats.
"""

from abc import ABC, abstractmethod


class BaseExporter(ABC):
    """Abstract base class for all data exporters.

    Defines the common interface that all exporter implementations
    must follow for saving graph data to external formats.
    """

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the exporter instance."""
        pass

    @abstractmethod
    def statistics(self):
        """Get export statistics.

        Returns:
            Statistics about the export operation.

        """
        pass
