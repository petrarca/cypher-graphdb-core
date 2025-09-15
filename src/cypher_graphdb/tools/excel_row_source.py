"""Excel row source implementation.

Provides ExcelRowSource class for streaming rows from Excel worksheets.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from .row_source import RowSource


class ExcelRowSource(RowSource):
    """Streaming row reader for Excel worksheets via openpyxl.

    This is a minimal implementation to remove pandas dependency. Cells are
    read row-wise and converted to dicts using header row as column names.
    """

    def __init__(self, worksheet):
        self._ws = worksheet
        self._header: list[str] | None = None

    def columns(self) -> list[str]:
        if self._header is None:
            self._resolve_header()
        return list(self._header or [])

    def _resolve_header(self):
        for row in self._ws.iter_rows(min_row=1, max_row=1, values_only=True):
            self._header = [str(c) if c is not None else "" for c in row]
            break
        if self._header is None:
            self._header = []

    def iter_batches(self, batch_size: int) -> Iterator[list[dict[str, Any]]]:
        if self._header is None:
            self._resolve_header()
        batch: list[dict[str, Any]] = []
        for row in self._ws.iter_rows(min_row=2, values_only=True):
            record = {self._header[i]: row[i] for i in range(min(len(self._header), len(row)))}
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
