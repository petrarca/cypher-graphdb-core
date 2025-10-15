"""AGE type handling module: Custom loader for Apache AGE agtype data.

This module provides a custom psycopg loader for handling Apache AGE's
agtype data format, allowing proper conversion of graph data types.
"""

from typing import Any

from age import newResultHandler
from psycopg.abc import AdaptContext
from psycopg.adapt import Loader


class AgTypeLoader(Loader):
    """Custom loader for Apache AGE agtype data format."""

    def __init__(self, oid: int, context: AdaptContext | None = None):
        super().__init__(oid, context)

        self._result_handler = newResultHandler()

    def load(self, data: bytes | bytearray | memoryview) -> Any | None:
        # decode agtype result into vertex, edge, path etc...
        # psycopg3 returns memoryview - convert to bytes first
        if isinstance(data, memoryview):
            data = bytes(data)
        return self._result_handler.parse(data.decode("utf-8"))
