"""Exceptions for the cypher_graphdb library.

This module defines custom exceptions used throughout the library.
Backend-specific exceptions (AGEExecutionError, etc.) live in their
respective backend modules; this module holds backend-agnostic
exceptions that consumers can catch without knowing the backend.
"""


class ReadOnlyModeError(Exception):
    """Raised when attempting write operations in read-only mode.

    This exception is raised when a user attempts to execute a write operation
    (CREATE, DELETE, SET, MERGE, REMOVE) or call write methods
    (create_or_merge, delete, execute_sql) while the connection is in
    read-only mode.

    Examples:
        >>> db = CypherGraphDB()
        >>> db.connect(read_only=True)
        >>> db.execute("CREATE (n:Node)")
        ReadOnlyModeError: Write operation not allowed in read-only mode...
    """


class LabelNotFoundError(Exception):
    """Raised when a query references a node or edge label that does not exist.

    In Apache AGE, labels are backed by PostgreSQL tables that are created
    lazily on first CREATE. A MATCH on a label that has never been written
    fails with ``UndefinedTable``. This exception wraps that backend-specific
    error so consumers can catch it without importing psycopg.

    Common scenario: running a cleanup/delete query on a fresh graph where
    no nodes of a given type have ever been created. The correct response
    is usually to skip the operation (nothing to delete).
    """


class CypherQueryError(Exception):
    """Raised when a fluent ``CypherQuery`` is built or used incorrectly.

    Covers builder-time errors such as building a query with no ``MATCH``
    clause, or supplying values that cannot be bound. This is distinct from
    ``QueryExecutionError`` which wraps backend execution failures.
    """


class QueryExecutionError(Exception):
    """Raised when a query fails to execute against the backend.

    This is the backend-agnostic wrapper for execution-time failures such as
    Cypher syntax errors or other database errors raised by the underlying
    driver (e.g. Memgraph's ``neo4j.exceptions.Neo4jError`` or AGE's
    ``AGEExecutionError``). The facade wraps those backend-specific exceptions
    so consumers can catch a single type without importing any backend driver.

    The original backend exception is preserved as the ``__cause__`` (via
    ``raise ... from``). The message contains the backend's error text, which
    is suitable for surfacing to a user (e.g. as an HTTP 400 detail).

    Examples:
        >>> try:
        ...     cdb.execute_with_stats("MATCH (n RETURN n")
        ... except QueryExecutionError as e:
        ...     print(e)  # "Error on line 1 position 10. ..."
    """


class QueryTimeoutError(QueryExecutionError):
    """Raised when the backend cancels a query for running longer than its timeout.

    The timeout is the connection's default (``query_timeout_s``) or a statement's own
    (``execute(..., timeout_s=)``, ``with cdb.query_timeout(...)``). A subclass of
    ``QueryExecutionError``: callers catching that one still catch it.
    """


class PathLengthExceededError(Exception):
    """Raised, before it runs, for a query whose patterns can match more relationships than the caller allows.

    The limit is the caller's (``execute(..., max_path_length=)``); the query's longest path is
    ``ParsedCypherQuery.max_path_length`` (None: unbounded). The message says how to bound it.
    """

    def __init__(self, max_path_length: int | None, limit: int):
        self.max_path_length = max_path_length
        self.limit = limit
        found = (
            "an unbounded variable-length relationship"
            if max_path_length is None
            else f"a path of up to {max_path_length} relationships"
        )
        hint = f"bound its variable-length relationships, e.g. [:REL*1..{limit}]"
        super().__init__(f"Query rejected: it has {found}, the limit is {limit}; {hint}")


def check_path_length(max_path_length: int | None, limit: int | None) -> None:
    """Raise PathLengthExceededError when a query's longest path (None: unbounded) exceeds ``limit`` (None: no limit)."""
    if limit is not None and (max_path_length is None or max_path_length > limit):
        raise PathLengthExceededError(max_path_length, limit)
