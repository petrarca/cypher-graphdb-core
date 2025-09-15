"""Memgraph backend implementation for CypherGraphDB.

This module provides the main interface for connecting to and querying Memgraph
databases. Memgraph is a native graph database that supports the Cypher query language.

Classes:
    MemgraphDB: Main backend class for Memgraph database operations.
"""

import contextlib
import os
from typing import Any

import mgclient
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Import directly from the main package
from cypher_graphdb import config
from cypher_graphdb.backend import BackendCapability, CypherBackend, ExecStatistics
from cypher_graphdb.cypherparser import ParsedCypherQuery
from cypher_graphdb.models import GraphObject, GraphObjectType
from cypher_graphdb.statistics import LabelStatistics


class MemgraphDB(CypherBackend):
    """Memgraph backend for graph database operations.

    This class provides a complete interface for connecting to Memgraph databases,
    enabling graph operations using Cypher queries. Memgraph is a native graph database
    that fully supports the Cypher query language.

    Attributes:
        _id: Backend identifier set to "MEMGRAPH".
        _connection: Current database connection.
        _graph_name: Name of the active graph.
        autocommit: Whether to automatically commit transactions.

    Example:
        >>> from cypher_graphdb import MemgraphDB
        >>> db = MemgraphDB("host=localhost port=7687", "my_graph")
        >>> result = db.execute_cypher("MATCH (n) RETURN n LIMIT 5")
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize Memgraph GraphDB backend.

        Args:
            *args: Positional arguments passed to connect().
            **kwargs: Keyword arguments passed to connect().
        """
        super().__init__(args, **kwargs)

        # required for reconnect
        self._cinfo = None
        self._ckwargs = None

        if args or kwargs:
            self.connect(*args, **kwargs)

    def connect(
        self,
        cinfo: str | None = None,
        graph_name: str | None = "memgraph",
        **kwargs: Any,
    ) -> "MemgraphDB":
        """Connect to Memgraph database.

        Args:
            cinfo: Connection string or parameters. If None, loads from env.
            graph_name: Name of the graph to use. Default is "memgraph".
            **kwargs: Additional connection parameters including host, port, username, password.

        Returns:
            Self for method chaining.
        """
        if self._connection is not None:
            return self

        if not cinfo:
            logger.debug("Try to load cinfo from env")
            cinfo = os.getenv(config.CGDB_CINFO)

        self.autocommit = kwargs.pop("autocommit", self.autocommit)

        # Parse connection info
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 7687)
        username = kwargs.get("username", "")
        password = kwargs.get("password", "")

        # Store connection parameters for reconnect
        self._cinfo = cinfo
        self._ckwargs = kwargs
        self._graph_name = "memgraph"

        logger.debug(f"{host=}, {port=}, {username=}, {self.autocommit=}")

        try:
            # Connect to Memgraph with retry logic
            self._connection = self._connect_with_retry(host, port, username, password)

            logger.debug(f"Successfully connected to Memgraph with autocommit={self.autocommit}")
        except Exception as e:
            logger.error(f"Failed to connect to Memgraph: {e}")
            self._connection = None
            raise

        return self

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((mgclient.OperationalError, ConnectionError)),
        reraise=True,
    )
    def _connect_with_retry(self, host: str, port: int, username: str, password: str):
        """Connect to Memgraph with retry logic for transient failures."""
        logger.debug(f"Attempting connection to Memgraph at {host}:{port}")
        return mgclient.connect(
            host=host,
            port=port,
            username=username,
            password=password,
        )

    def execute_cypher(
        self,
        cypher_query: ParsedCypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics]:
        """Execute a Cypher query against the graph database.

        Args:
            cypher_query: Parsed Cypher query to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without processing.

        Returns:
            Tuple of (query results, execution statistics).
        """
        assert isinstance(cypher_query, ParsedCypherQuery)

        # Get the query string
        query = cypher_query.parsed_query

        return self._execute_query(query, fetch_one, raw_data)

    def fulltext_search(
        self, cypher_query: ParsedCypherQuery, fts_query: str, language: str = None
    ) -> tuple[list[tuple[GraphObject, ...]], ExecStatistics]:
        """Perform full-text search on graph data.

        Args:
            cypher_query: Base Cypher query to modify for full-text search.
            fts_query: Full-text search query string.
            language: Language for text search.

        Returns:
            Tuple of (search results, execution statistics).
        """
        # Implementation will be added later
        return ([], ExecStatistics())

    def graphs(self) -> list[str]:
        """Get list of all graphs in the database.

        Returns:
            List of graph names.
        """
        # Implementation will be added later
        return ["memgraph"]

    def labels(self) -> list[LabelStatistics]:
        """Get statistics for all labels in the current graph.

        Returns:
            List of label statistics including counts and types.
        """

        labels = []

        # Single efficient query to get both node labels and relationship types
        combined_query = """
        MATCH (n)
        UNWIND labels(n) AS label
        RETURN 'node' AS entity_type, label AS name, count(*) AS cnt
        UNION ALL
        MATCH ()-[r]->()
        RETURN 'relationship' AS entity_type, type(r) AS name, count(r) AS cnt
        ORDER BY entity_type, name
        """

        try:
            # Execute combined query for better performance
            result, _ = self._execute_query(combined_query, raw_data=True)

            for row in result:
                # Query returns exactly 3 columns: entity_type, name, count
                assert len(row) == 3, f"Expected 3 columns, got {len(row)}"
                entity_type, label_name, count = row[0], row[1], row[2]
                if label_name:  # Skip empty labels/types
                    # Determine type based on the first column
                    obj_type = GraphObjectType.NODE if entity_type == "node" else GraphObjectType.EDGE

                    label_statistics = LabelStatistics(
                        graph_name=self.graph_name,
                        label_=label_name,
                        type_=obj_type,
                        count=count,
                    )
                    labels.append(label_statistics)

        except Exception as e:
            logger.error(f"Failed to get label statistics: {e}")
            # Return empty list on error
            return []

        return labels

    def disconnect(self):
        """Close the database connection and clear connection info."""
        if self._connection:
            try:
                self._connection.close()
                logger.debug("Disconnected from Memgraph")
            except mgclient.DatabaseError as e:
                logger.error(f"Database error disconnecting from Memgraph: {e}")
            except ConnectionError as e:
                logger.error(f"Connection error disconnecting from Memgraph: {e}")
            except Exception as e:
                logger.error(f"Unexpected error disconnecting from Memgraph: {e}")
            finally:
                self._connection = None

        self._cinfo = None
        self._ckwargs = None

    def reconnect(self):
        """Reconnect to the database using stored connection info."""
        assert self._cinfo is not None, "Can only reconnect if already successfully connected!"

        logger.debug("Try to reconnect to Memgraph")

        # Close existing connection if any
        if self._connection:
            with contextlib.suppress(Exception):
                self._connection.close()
            self._connection = None

        # Reconnect using stored parameters
        self.connect(**self._ckwargs)

    def _require_connection(self):
        """Ensure database connection is available, reconnecting if needed."""
        if self._connection is None and self._cinfo is not None:
            self.reconnect()

        if self._connection is None:
            raise RuntimeError("Not connected to Memgraph database")

    def commit(self):
        """Commit the current transaction.

        Commits the current transaction to make changes persistent.
        """
        self._require_connection()
        try:
            self._connection.commit()
            logger.debug("Transaction committed successfully")
        except mgclient.DatabaseError as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during commit: {e}")
            raise

    def rollback(self):
        """Rollback the current transaction.

        Rolls back the current transaction, discarding any uncommitted changes.
        """
        self._require_connection()
        try:
            self._connection.rollback()
            logger.debug("Transaction rolled back successfully")
        except mgclient.DatabaseError as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during rollback: {e}")
            raise

    def create_graph(self, graph_name: str | None = None) -> None:
        """Create a new graph in the database.

        Args:
            graph_name: Name of the graph to create. Uses current graph if None.

        Raises:
            NotImplementedError: Memgraph doesn't support multiple graphs.
        """
        raise NotImplementedError("Memgraph doesn't support multiple graphs, only the default 'memgraph' graph is available.")

    def drop_graph(self, graph_name: str | None = None) -> None:
        """Drop a graph from the database.

        Args:
            graph_name: Name of the graph to drop. Uses current graph if None.

        Raises:
            NotImplementedError: Memgraph doesn't support multiple graphs.
        """
        raise NotImplementedError("Memgraph doesn't support dropping graphs, only the default 'memgraph' graph is available.")

    def graph_exists(self, graph_name: str = None) -> bool:
        """Check if a graph exists in the database.

        Args:
            graph_name: Name of the graph to check. Uses current graph if None.

        Returns:
            True if the graph exists, False otherwise.
        """
        # Only the default graph "memgraph" exists
        return graph_name == "memgraph"

    def get_capability(self, capability: BackendCapability) -> Any:
        """Get the value of a backend capability.

        Args:
            capability: The capability to query.

        Returns:
            String value representing the capability status.

        Raises:
            ValueError: If the capability is not supported.
        """
        match capability:
            case BackendCapability.LABEL_FUNCTION:
                # Pattern to retrieve single label - use with .format(node_var)
                return "labels({$node})[0]"
            case BackendCapability.SUPPORT_MULTIPLE_LABELS:
                # Memgraph support multiple labels per node
                return True
            case _:
                # Delegate unknown capabilities to superclass
                return super().get_capability(capability)

    def _execute_query(
        self, query: str, fetch_one: bool = False, raw_data: bool = False
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics]:
        """Execute a Cypher query and process the results.

        Args:
            query: Cypher query string to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without processing.

        Returns:
            Tuple of (query results, execution statistics).
        """
        import time

        from .memgraphrowfactories import memgraph_row_factory

        self._require_connection()

        start_time = time.perf_counter()
        logger.debug(f"Memgraph execute: {query}")

        exec_stats = ExecStatistics()
        result = []

        try:
            # Get cursor from connection
            cursor = self._connection.cursor()

            # Execute the query
            cursor.execute(query)

            # Process results
            if not raw_data:
                # Apply row factory to convert results
                row_factory = memgraph_row_factory(exec_stats, self._model_provider)

                if fetch_one:
                    row = cursor.fetchone()
                    if row:
                        result = [row_factory(row)]
                else:
                    for row in cursor.fetchall():
                        result.append(row_factory(row))
            else:
                # Return raw results
                result = [cursor.fetchone()] if fetch_one else cursor.fetchall()

            # Close the cursor
            cursor.close()

            # Handle autocommit like AGE does
            if self.autocommit:
                self._connection.commit()

        except mgclient.DatabaseError as e:
            logger.error(f"Memgraph database error: {e}")
            raise
        except ValueError as e:
            logger.error(f"Value error in Memgraph query: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing Memgraph query: {e}")
            raise

        # Record execution time
        exec_stats.exec_time = time.perf_counter() - start_time
        logger.debug(f"exec_time={exec_stats.exec_time:.4f}s")

        return (result, exec_stats)
