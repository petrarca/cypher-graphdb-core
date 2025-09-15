"""Row source abstraction for tabular data access.

Provides the RowSource abstract base class for streaming row-oriented data.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class RowSource(ABC):
    """Abstract row-oriented tabular data source.

    Implementations must provide column names and batched iteration of rows
    as dictionaries (column->value).
    """

    @abstractmethod
    def columns(self) -> list[str]:  # noqa: D401 - obvious
        return []

    @abstractmethod
    def iter_batches(self, batch_size: int) -> Iterator[list[dict[str, Any]]]:  # noqa: D401 - obvious
        yield []
