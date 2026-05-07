"""AGE type handling module: Custom psycopg loader for Apache AGE agtype data.

Uses our own JSON-based agtype parser (agtype_parser.py) instead of the
age package's ANTLR parser, which has a string unescaping bug.
"""

from typing import Any

from psycopg.abc import AdaptContext
from psycopg.adapt import Loader

from .agtype_parser import parse_agtype


class AgTypeLoader(Loader):
    """Custom psycopg loader for Apache AGE agtype data format."""

    def __init__(self, oid: int, context: AdaptContext | None = None):
        super().__init__(oid, context)

    def load(self, data: bytes | bytearray | memoryview) -> Any | None:
        if isinstance(data, memoryview):
            data = bytes(data)
        return parse_agtype(data.decode("utf-8"))
