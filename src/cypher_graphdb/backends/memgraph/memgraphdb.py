"""Memgraph backend implementation for CypherGraphDB.

This module provides the main interface for connecting to and querying Memgraph
databases. Memgraph is a native graph database that supports the Cypher query language.

Classes:
    MemgraphDB: Main backend class for Memgraph database operations.
"""

import contextlib
from collections.abc import Generator
from typing import Any

import mgclient
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Import directly from the main package
from cypher_graphdb.backend import BackendCapability, CypherBackend, ExecStatistics
from cypher_graphdb.cypherparser import ParsedCypherQuery
from cypher_graphdb.models import GraphObject, GraphObjectType, TabularResult
from cypher_graphdb.statistics import IndexInfo, IndexType, LabelStatistics
from cypher_graphdb.utils import chunk_list, parse_connection_uri, validate_protocol


class MemgraphDB(CypherBackend):
    """Memgraph backend for graph database operations.

    This class provides a complete interface for connecting to Memgraph databases,
    enabling graph operations using Cypher queries. Memgraph is a native graph database
    that fully supports the Cypher query language.

    Attributes:
        name: Backend identifier set to "MEMGRAPH".
        _connection: Current database connection.
        _graph_name: Name of the active graph.
        autocommit: Whether to automatically commit transactions.

    Example:
        >>> from cypher_graphdb import MemgraphDB
        >>> db = MemgraphDB("host=localhost port=7687", "my_graph")
        >>> result = db.execute_cypher("MATCH (n) RETURN n LIMIT 5")
    """

    name = "MEMGRAPH"

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
    ) -> MemgraphDB:
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

        # Values now come from CypherGraphDB settings via connect() parameters
        # No need to check environment variables directly

        self.autocommit = kwargs.pop("autocommit", self.autocommit)
        self._read_only = kwargs.pop("read_only", self._read_only)

        # Parse connection info from cinfo string and merge with kwargs
        cinfo_params = {}
        if cinfo:
            cinfo_params = self._parse_cinfo(cinfo)

        # Merge parameters: kwargs take precedence over cinfo
        host = kwargs.get("host") or cinfo_params.get("host", "127.0.0.1")
        port = kwargs.get("port") or cinfo_params.get("port", 7687)
        username = kwargs.get("username") or cinfo_params.get("username", "")
        password = kwargs.get("password") or cinfo_params.get("password", "")

        # Store connection parameters for reconnect
        self._cinfo = cinfo
        self._ckwargs = kwargs
        self._graph_name = "memgraph"

        logger.debug(
            "host=%s, port=%s, username_masked=%s, autocommit=%s", host, port, "***MASKED***" if username else "", self.autocommit
        )

        try:
            # Connect to Memgraph with retry logic
            self._connection = self._connect_with_retry(host, port, username, password)

            logger.debug(f"Successfully connected to Memgraph with autocommit={self.autocommit}")
        except Exception as e:
            logger.error(f"Failed to connect to Memgraph: {e}")
            self._connection = None
            raise

        return self

    def _parse_cinfo(self, cinfo: str) -> dict[str, Any]:
        """Parse Memgraph connection info string.

        Supports formats:
        - "host=localhost port=7687 username=user password=pass"
        - "bolt://username:password@localhost:7687"

        Args:
            cinfo: Connection info string to parse

        Returns:
            Dictionary of parsed connection parameters

        Raises:
            ValueError: If protocol is not 'bolt' or parsing fails
        """
        if not cinfo:
            return {}

        # Parse using the common utility
        params = parse_connection_uri(cinfo)

        # Validate protocol if specified (only bolt is supported for Memgraph)
        if "protocol" in params:
            validate_protocol(params, ["bolt"])

        return params

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
        params: dict | None = None,
    ) -> tuple[TabularResult, ExecStatistics]:
        """Execute a Cypher query against the graph database.

        Args:
            cypher_query: Parsed Cypher query to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without processing.
            params: Optional dictionary of parameter values to bind (e.g., {"key": "value"}).

        Returns:
            Tuple of (query results, execution statistics).

        Raises:
            ReadOnlyModeError: If query contains write operations in
                read-only mode.
        """
        assert isinstance(cypher_query, ParsedCypherQuery)

        # Validate read-only mode FIRST
        self._validate_read_only(cypher_query)

        # Get the query string
        query = cypher_query.parsed_query

        return self._execute_query(query, fetch_one, raw_data, params)

    def fulltext_search(
        self,
        cypher_query: ParsedCypherQuery,
        fts_query: str,
        language: str | None = None,
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics]:
        """Perform full-text search using Cypher queries.

        Args:
            cypher_query: Parsed Cypher query to execute.
            fts_query: Full-text search query string.
            language: Language for text search.

        Returns:
            Tuple of (search results, execution statistics).
        """
        # Implementation will be added later
        return ([], ExecStatistics())

    def execute_cypher_stream(
        self,
        cypher_query: ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> Generator[list[list[Any]]]:
        """Execute a Cypher query and yield results in chunks.

        Args:
            cypher_query: Parsed Cypher query to execute.
            chunk_size: Number of rows to fetch per chunk.
            raw_data: If True, return raw data without processing.
            params: Optional dictionary of parameter values to bind (e.g., {"key": "value"}).

        Yields:
            list[list[Any]]: Lists of result rows (chunks).
        """
        assert isinstance(cypher_query, ParsedCypherQuery)

        # Validate read-only mode
        self._validate_read_only(cypher_query)

        query = cypher_query.parsed_query
        self._require_connection()

        logger.debug("Memgraph execute stream (chunk_size={}): {}", chunk_size, query)

        from .memgraphrowfactories import memgraph_row_factory

        cursor = self._connection.cursor()
        try:
            # Execute the query with optional params
            cursor.execute(query, params or {})

            # Set up row factory if not raw data
            exec_stats = ExecStatistics()
            row_factory = None
            if not raw_data:
                row_factory = memgraph_row_factory(exec_stats, self._model_provider)

            # Stream results in chunks
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break

                if raw_data:
                    yield chunk
                else:
                    # Apply row factory to each row in chunk
                    processed_chunk = [row_factory(row) for row in chunk]
                    yield processed_chunk

        finally:
            # Always close cursor
            cursor.close()

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

    # ── Index management ─────────────────────────────────────────────────

    def create_property_index(self, label: str, *property_names: str) -> None:
        """Create property indexes on the given label.

        Memgraph creates one index per property. If no property_names are
        given, creates a label-only index.

        Args:
            label: Node label to index.
            *property_names: Property names to index. Each gets its own index.
        """
        self._require_connection()
        if property_names:
            for prop in property_names:
                self._run_ddl(f"CREATE INDEX ON :{label}({prop})")
        else:
            self._run_ddl(f"CREATE INDEX ON :{label}")
        logger.debug("Property index(es) created for label '{}' in Memgraph", label)

    def drop_index(self, label: str, *property_names: str) -> None:
        """Drop property indexes on the given label.

        Args:
            label: Node label whose index to drop.
            *property_names: Property names to drop. Drops label index if empty.
        """
        self._require_connection()
        if property_names:
            for prop in property_names:
                self._run_ddl(f"DROP INDEX ON :{label}({prop})")
        else:
            self._run_ddl(f"DROP INDEX ON :{label}")
        logger.debug("Index(es) dropped for label '{}' in Memgraph", label)

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:
        """List all indexes in Memgraph.

        Args:
            include_internal: Unused for Memgraph -- all indexes are user-created.

        Returns:
            List of IndexInfo objects.
        """
        self._require_connection()
        with self._autocommit_mode():
            cursor = self._connection.cursor()
            cursor.execute("SHOW INDEX INFO")
            rows = cursor.fetchall()
            cursor.close()

        indexes = []
        for row in rows:
            # Memgraph SHOW INDEX INFO returns: (index_type_str, label, property_list_or_none, count)
            # property is a Python list (e.g. ['id']) or None for label-only indexes
            index_type_str, label, prop_list, _ = row
            idx_type = IndexType.UNIQUE if "unique" in str(index_type_str).lower() else IndexType.PROPERTY
            # prop_list is already a list or None
            prop_names = list(prop_list) if prop_list else None
            name = f"{label}_{'_'.join(prop_list)}" if prop_list else label
            indexes.append(
                IndexInfo(
                    label=label,
                    property_names=prop_names,
                    index_type=idx_type,
                    index_name=name,
                )
            )
        return indexes

    # ── Bulk write operations ─────────────────────────────────────────────

    def bulk_create_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        """Create nodes in batches using parameterized UNWIND.

        Memgraph supports standard $params in UNWIND, so we use the clean
        parameterized path.

        Args:
            label: Node label for all created nodes.
            rows: List of property dicts, one per node.
            batch_size: Number of nodes per UNWIND batch.

        Returns:
            Total number of nodes created.
        """
        self._require_connection()
        if not rows:
            return 0

        total = 0
        for batch in chunk_list(rows, batch_size):
            cursor = self._connection.cursor()
            cursor.execute(
                f"UNWIND $rows AS props CREATE (n:{label}) SET n = props",
                {"rows": batch},
            )
            cursor.close()
            if self.autocommit:
                self._connection.commit()
            total += len(batch)

        return total

    def bulk_create_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str = "",
        dst_label: str = "",
        src_key: str = "id",
        dst_key: str = "id",
        batch_size: int = 500,
    ) -> int:
        """Create edges in batches using parameterized UNWIND.

        Each dict in edges must have "src" and "dst" keys. Additional keys
        are set as edge properties.

        Args:
            label: Edge label for all created edges.
            edges: List of dicts with "src", "dst", and optional edge properties.
            src_label: Label of source nodes (empty string for any label).
            dst_label: Label of destination nodes (empty string for any label).
            src_key: Property name on source nodes to match against "src".
            dst_key: Property name on destination nodes to match against "dst".
            batch_size: Number of edges per UNWIND batch.

        Returns:
            Total number of edges created.
        """
        self._require_connection()
        if not edges:
            return 0

        # Build MATCH patterns from label/key names (not user values -- safe to inline)
        src_pat = f"(a:{src_label} {{{src_key}: e.src}})" if src_label else f"(a {{{src_key}: e.src}})"
        dst_pat = f"(b:{dst_label} {{{dst_key}: e.dst}})" if dst_label else f"(b {{{dst_key}: e.dst}})"

        # Collect edge property keys (beyond src/dst) from first row
        edge_prop_keys = [k for k in edges[0] if k not in ("src", "dst")]
        if edge_prop_keys:
            set_clause = " SET " + ", ".join(f"r.{k} = e.{k}" for k in edge_prop_keys)
        else:
            set_clause = ""

        cypher = f"UNWIND $edges AS e MATCH {src_pat} MATCH {dst_pat} CREATE (a)-[r:{label}]->(b){set_clause}"

        total = 0
        for batch in chunk_list(edges, batch_size):
            cursor = self._connection.cursor()
            cursor.execute(cypher, {"edges": batch})
            cursor.close()
            if self.autocommit:
                self._connection.commit()
            total += len(batch)

        return total

    @contextlib.contextmanager
    def _autocommit_mode(self):
        """Temporarily enable autocommit on the connection.

        Memgraph requires DDL (CREATE/DROP INDEX) and storage info queries
        (SHOW INDEX INFO) to run in autocommit (implicit transaction) mode.
        """
        prev = self._connection.autocommit
        self._connection.autocommit = True
        try:
            yield
        finally:
            self._connection.autocommit = prev

    def _run_ddl(self, statement: str) -> None:
        """Execute a DDL statement (CREATE/DROP INDEX etc.) on Memgraph."""
        with self._autocommit_mode():
            cursor = self._connection.cursor()
            cursor.execute(statement)
            cursor.close()

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
            case BackendCapability.STREAMING_SUPPORT:
                # Memgraph supports native streaming via fetchmany()
                return True
            case BackendCapability.PROPERTY_INDEX:
                # Memgraph supports CREATE/DROP INDEX ON :Label(prop)
                return True
            case _:
                # Delegate unknown capabilities to superclass
                return super().get_capability(capability)

    def _execute_query(
        self, query: str, fetch_one: bool = False, raw_data: bool = False, params: dict | None = None
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics]:
        """Execute a Cypher query and process the results.

        Args:
            query: Cypher query string to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without processing.
            params: Optional dictionary of parameter values to bind.

        Returns:
            Tuple of (query results, execution statistics).
        """
        import time

        from .memgraphrowfactories import memgraph_row_factory

        self._require_connection()

        start_time = time.perf_counter()
        logger.debug("Memgraph execute: {}", query)

        exec_stats = ExecStatistics()
        result = []

        try:
            # Get cursor from connection
            cursor = self._connection.cursor()

            # Execute the query with optional params
            cursor.execute(query, params or {})

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
            logger.error("Memgraph database error: {}", e)
            raise
        except ValueError as e:
            logger.error("Value error in Memgraph query: {}", e)
            raise
        except Exception as e:
            logger.error("Unexpected error executing Memgraph query: {}", e)
            raise

        # Record execution time
        exec_stats.exec_time = time.perf_counter() - start_time
        logger.debug("exec_time={:.4f}s", exec_stats.exec_time)

        # Calculate column count from the result
        # If result is not empty, use the length of the first row as column count
        if result and len(result) > 0 and isinstance(result[0], tuple):
            exec_stats.col_count = len(result[0])

        return (result, exec_stats)
