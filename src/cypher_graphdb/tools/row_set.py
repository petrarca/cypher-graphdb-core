"""Row source abstractions for DuckDB-based import/export.

Provides the RowSet class used by exporters. The RowSource base class and its
implementations are in separate files.
"""

from __future__ import annotations


class RowSet:
    """Lightweight summary object passed to exporter callbacks.

    Provides only length and (optionally) column names to avoid exposing heavy
    DataFrame-like interfaces.
    """

    def __init__(self, count: int, columns: list[str] | None = None):
        self._count = count
        self.columns = columns or []

    def __len__(self) -> int:  # noqa: D401 - obvious
        return self._count
