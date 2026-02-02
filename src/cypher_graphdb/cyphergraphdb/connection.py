"""Connection management mixin for CypherGraphDB."""

import contextlib
from typing import Any


class ConnectionMixin:
    """Mixin providing connection management methods for CypherGraphDB."""

    def __enter__(self):
        """Enter the context manager for automatic connection management.

        The context manager pattern is the recommended way to use CypherGraphDB as it ensures
        proper cleanup of resources by automatically disconnecting when exiting the context.

        Returns self without implicit connection. You still need to call connect() explicitly
        unless you provided connection parameters during initialization for auto-connection.

        Returns:
            CypherGraphDB instance for method chaining.

        Example:
            ```python
            # Basic context manager usage (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()
                result = cdb.execute("MATCH (n) RETURN count(n)")
                # No need to call disconnect() - handled automatically

            # Auto-connection with parameters
            with CypherGraphDB(connect_url="bolt://localhost:7687") as cdb:
                # No need to call connect() - already connected
                result = cdb.execute("MATCH (n) RETURN count(n)")
                # No need to call disconnect() - handled automatically
            ```
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager with automatic cleanup.

        Automatically disconnects from the database when exiting the context,
        ensuring proper resource cleanup. This is why the context manager pattern
        is recommended - you don't need to remember to call disconnect() explicitly.

        Any disconnect errors are suppressed, but original exceptions from the
        context are preserved and propagated.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False to propagate any exceptions that occurred in the context
        """
        with contextlib.suppress(Exception):
            self.disconnect()
        return False  # Don't suppress exceptions

    def connect(self, connect_url: str | None = None, *args: Any, **kwargs: Any) -> ConnectionMixin:
        """Establish a connection to the configured graph database backend.

        Args:
            connect_url: Optional connection URL (e.g., "bolt://localhost:7687")
            *args: Additional positional arguments passed to backend
            **kwargs: Additional keyword arguments passed to backend

        Returns:
            Self for method chaining

        Examples:
            ```python
            # Approach 1: Direct connection with explicit URL
            db = CypherGraphDB("memgraph").connect("bolt://localhost:7687")
            # Remember to call db.disconnect() when done

            # Approach 2: Direct connection with parameters
            db = CypherGraphDB("memgraph").connect(
                host="localhost",
                port=7687
                # username and password if needed
            )
            # Remember to call db.disconnect() when done

            # Approach 3: Context manager (recommended)
            # Automatically disconnects when exiting the context
            with CypherGraphDB() as cdb:
                cdb.connect()
                # Perform operations here
                result = cdb.execute("MATCH (n) RETURN count(n)")
                # No need to call disconnect() - handled automatically
            ```
        """
        assert self._backend

        if connect_url is not None:
            self._backend.connect(cinfo=connect_url, **kwargs)
        else:
            # Pass settings as fallbacks - explicit params take precedence
            merged_kwargs = {}

            # Only add cinfo from settings if no explicit connection params
            # (host, port, etc.) are provided
            has_explicit_conn_params = any(k in kwargs for k in ["host", "port", "dbname", "user", "password"])

            # Add settings as defaults if not already provided
            if "cinfo" not in kwargs and self.settings.cinfo and not has_explicit_conn_params:
                merged_kwargs["cinfo"] = self.settings.cinfo
            if "graph_name" not in kwargs and self.settings.graph:
                merged_kwargs["graph_name"] = self.settings.graph
            if "read_only" not in kwargs:
                merged_kwargs["read_only"] = self.settings.read_only
            if "create_graph" not in kwargs:
                merged_kwargs["create_graph"] = self.settings.create_graph

            # Explicit kwargs override settings
            merged_kwargs.update(kwargs)

            self._backend.connect(*args, **merged_kwargs)

        return self

    def disconnect(self):
        """Close the connection to the graph database.

        Gracefully closes the database connection and cleans up resources.
        Safe to call multiple times.

        Note: When using the context manager approach (with statement),
        disconnect() is called automatically when exiting the context,
        so you don't need to call it explicitly.

        Example:
            ```python
            # Approach 1: Direct connection (requires explicit disconnect)
            db = CypherGraphDB("memgraph").connect()
            try:
                # ... database operations
                result = db.execute("MATCH (n) RETURN count(n)")
            finally:
                db.disconnect()  # Explicit cleanup required

            # Approach 2: Context manager (recommended, automatic disconnect)
            with CypherGraphDB() as cdb:
                cdb.connect()
                # ... database operations
                result = cdb.execute("MATCH (n) RETURN count(n)")
                # No need to call disconnect() - handled automatically
            ```
        """
        assert self._backend
        self._backend.disconnect()

    def commit(self):
        """Commit pending transactions to the graph database.

        Commits all pending changes in the current transaction to the database.
        Only applies to backends that support explicit transaction management.

        Example:
            ```python
            from cypher_graphdb import CypherGraphDB
            # Import the model classes defined above or from your own module
            from my_models import Product, Technology, UsesTechnology

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Create product and technology
                product = Product(name="CypherGraph", version="1.0")
                technology = Technology(name="Python", category="Programming Language")
                cdb.create_or_merge(product)
                cdb.create_or_merge(technology)

                # Create the relationship
                uses = UsesTechnology(
                    start_id_=product.id_,
                    end_id_=technology.id_,
                    since=2023
                )
                db.create_or_merge(uses)

                # Commit all changes at once
                db.commit()
                # Disconnect happens automatically when exiting the context
            ```
        """
        assert self._backend
        self._backend.commit()

    def rollback(self):
        """Rollback pending transactions, discarding all uncommitted changes.

        Discards all changes made in the current transaction without applying them
        to the database. Only applies to backends that support explicit transaction management.

        Example:
            ```python
            from cypher_graphdb import CypherGraphDB
            # Import the model classes defined above or from your own module
            from my_models import Product

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                try:
                    # Perform operations that might fail
                    product = Product(name="CypherGraph", version="1.0-beta")
                    cdb.create_or_merge(product)

                    # Risky operation that might fail
                    cdb.execute("MATCH (p:Product) SET p.validated = true")
                    cdb.commit()
                except Exception:
                    # Something went wrong, discard changes
                    cdb.rollback()
                    raise
                # Disconnect happens automatically when exiting the context
            ```
        """
        assert self._backend
        self._backend.rollback()
