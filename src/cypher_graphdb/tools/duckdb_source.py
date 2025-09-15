"""DuckDB row source implementation.

Provides DuckDBSource class for streaming rows from DuckDB queries.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import suppress
from typing import Any

import duckdb  # type: ignore

from .row_source import RowSource

try:
    import pyarrow as pa  # type: ignore
except Exception:  # pragma: no cover - pyarrow is optional at runtime
    pa = None  # type: ignore


class DuckDBSource(RowSource):
    """RowSource backed by a DuckDB relation.

    Parameters:
        sql: SQL text yielding the tabular data (SELECT ...)
        params: Optional parameters for the SQL (currently unused; reserved).

    Note: Call close() when done to release DuckDB resources, or use as a context manager.
    """

    def __init__(self, sql: str, params: Iterable[Any] | None = None):
        self._con = duckdb.connect(database=":memory:")
        self._sql = sql
        self._params = params or []
        # Evaluate once to capture schema; keep relation handle for iteration.
        self._rel = self._con.execute(self._sql, self._params).fetch_arrow_table()
        if pa is None:
            # Fallback: convert to Python list-of-dicts immediately (less efficient).
            _cols = self._rel.column_names
            _py_arrays = [col.to_pylist() for col in self._rel.columns]
            self._rows_cache = [dict(zip(_cols, row, strict=False)) for row in zip(*_py_arrays, strict=False)]
        else:
            self._rows_cache = []  # delay materialization when using Arrow streaming

    def columns(self) -> list[str]:
        return list(self._rel.column_names)

    def iter_batches(self, batch_size: int) -> Iterator[list[dict[str, Any]]]:
        if pa is None:
            # Use precomputed cache
            for i in range(0, len(self._rows_cache), batch_size):
                yield self._rows_cache[i : i + batch_size]
            return
        # Arrow path: slice without full materialization
        total = self._rel.num_rows
        cols = self._rel.column_names
        for offset in range(0, total, batch_size):
            slice_tbl = self._rel.slice(offset, min(batch_size, total - offset))
            batch_rows: list[dict[str, Any]] = []
            # Convert row-wise; could be optimized later by column-wise iteration.
            arrays = [slice_tbl.column(i).to_pylist() for i in range(len(cols))]
            for row_values in zip(*arrays, strict=False):
                batch_rows.append(dict(zip(cols, row_values, strict=False)))
            yield batch_rows

    def close(self):  # noqa: D401 - obvious
        """Close the DuckDB connection and release resources."""
        if hasattr(self, "_con") and self._con is not None:
            with suppress(Exception):
                self._con.close()
                self._con = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
