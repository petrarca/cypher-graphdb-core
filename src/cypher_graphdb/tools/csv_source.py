"""CSV row source implementation using standard library.

Provides CsvSource class for streaming rows from CSV files without external dependencies.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from contextlib import suppress
from typing import Any

from .row_source import RowSource


class CsvSource(RowSource):
    """RowSource backed by a CSV file using Python's standard csv module.

    Parameters:
        filename: Path to the CSV file to read.
        delimiter: CSV delimiter character (auto-detected if None).
        encoding: File encoding (default: utf-8).

    Note: Call close() when done to release file resources, or use as a context manager.
    """

    def __init__(
        self,
        filename: str,
        delimiter: str | None = None,
        encoding: str = "utf-8",
    ):
        self._filename = filename
        self._delimiter = delimiter
        self._encoding = encoding
        self._file = None
        self._reader = None
        self._columns = []

        # Open file and initialize reader
        self._file = open(self._filename, newline="", encoding=self._encoding)  # noqa: SIM115

        # Detect delimiter if not specified
        if delimiter is None:
            # Read a sample to detect delimiter
            sample = self._file.read(1024)
            self._file.seek(0)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                self._reader = csv.DictReader(self._file, dialect=dialect)
            except csv.Error:
                # Fallback to default dialect if detection fails
                self._reader = csv.DictReader(self._file)
        else:
            self._reader = csv.DictReader(self._file, delimiter=delimiter)

        self._columns = list(self._reader.fieldnames) if self._reader.fieldnames else []

    def columns(self) -> list[str]:
        return self._columns

    def iter_batches(self, batch_size: int) -> Iterator[list[dict[str, Any]]]:
        if not self._reader:
            return

        batch = []
        for row in self._reader:
            # Convert empty strings to None for consistency with standard CSV behavior
            processed_row = {}
            for key, value in row.items():
                if value == "":
                    processed_row[key] = None
                else:
                    processed_row[key] = value
            batch.append(processed_row)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        # Yield any remaining rows
        if batch:
            yield batch

    def close(self):
        """Close the CSV file and release resources."""
        if hasattr(self, "_file") and self._file is not None:
            with suppress(Exception):
                self._file.close()
                self._file = None
                self._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
