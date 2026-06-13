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


class QueryExecutionError(Exception):
    """Raised when a query fails to execute against the backend.

    This is the backend-agnostic wrapper for execution-time failures such as
    Cypher syntax errors or other database errors raised by the underlying
    driver (e.g. Memgraph's ``mgclient.DatabaseError`` or AGE's
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
