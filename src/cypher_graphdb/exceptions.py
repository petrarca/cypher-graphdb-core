"""Exceptions for the cypher_graphdb library.

This module defines custom exceptions used throughout the library.
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
