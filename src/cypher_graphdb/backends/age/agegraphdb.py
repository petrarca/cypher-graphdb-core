"""Apache AGE (A Graph Extension) backend implementation for PostgreSQL.

This module provides the main interface for connecting to and querying PostgreSQL
databases with the Apache AGE graph extension. AGE transforms PostgreSQL into a
graph database that supports Cypher query language.

Classes:
    AGEGraphDB: Main backend class for Apache AGE database operations.
"""

import hashlib
import json
import time
from collections.abc import Iterator
from typing import Any

import psycopg
import psycopg.conninfo as conninfo
from loguru import logger
from psycopg.client_cursor import ClientCursor
from psycopg.sql import SQL, Identifier
from psycopg.types import TypeInfo
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Import directly from the main package
from cypher_graphdb.backend import BackendCapability, CypherBackend, ExecStatistics, SqlStatistics
from cypher_graphdb.cypherparser import ParsedCypherQuery
from cypher_graphdb.models import GraphObject, GraphObjectType, TabularResult
from cypher_graphdb.statistics import LabelStatistics
from cypher_graphdb.utils import sanitize_connection_params_for_logging, sanitize_connection_string_for_logging

from .agerowfactories import age_row_factory
from .agesearch import convert_to_fts_query
from .agesqlbuilder import SQLBuilder
from .agtype import AgTypeLoader


class AGEExecutionError(Exception):
    """Exception raised for errors during AGE query execution."""


class AGEGraphDB(CypherBackend):
    """PostgreSQL Apache AGE backend for graph database operations.

    This class provides a complete interface for connecting to PostgreSQL databases
    with the Apache AGE extension, enabling graph operations using Cypher queries.
    AGE transforms PostgreSQL into a graph database by adding graph data types
    and query capabilities.

    Attributes:
        name: Backend identifier set to "AGE".
        _connection: Current database connection.
        _graph_name: Name of the active graph.
        autocommit: Whether to automatically commit transactions.

    Example:
        >>> from cypher_graphdb import AGEGraphDB
        >>> db = AGEGraphDB("host=localhost dbname=postgres", "my_graph")
        >>> result = db.execute_cypher("MATCH (n) RETURN n LIMIT 5")

    """

    name = "AGE"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize AGE GraphDB backend.

        Args:
            *args: Positional arguments passed to connect().
            **kwargs: Keyword arguments passed to connect().

        """
        super().__init__(args, **kwargs)

        # required for reconnect
        self._cinfo = None
        self._ckwargs = None
        self._cursor_factory = None
        self._set_graph_if_not_exists = True

        # prepared statement cache for parameterized queries
        self._prepared_statements = {}  # query_hash -> stmt_name
        self._max_cached_statements = 10

        if args or kwargs:
            self.connect(*args, **kwargs)

    def connect(
        self,
        cinfo: str | None = None,
        *args: Any,
        graph_name: str | None = None,
        create_graph: bool = False,
        cursor_factory: Any = ClientCursor,
        check_graph_exists: bool = True,
        **kwargs: Any,
    ) -> "AGEGraphDB":
        """Connect to PostgreSQL database with Apache AGE extension.

        Args:
            cinfo: Connection string or parameters. If None, loads from env.
            graph_name: Name of the graph to use. If None, loads from env.
            create_graph: Whether to create the graph if it doesn't exist.
            cursor_factory: Factory class for creating database cursors.
            check_graph_exists: Whether to verify graph existence.
            **kwargs: Additional connection parameters (including autocommit, read_only).

        Returns:
            Self for method chaining.

        """
        if self._connection is not None:
            return self

        # Values now come from CypherGraphDB settings via connect() parameters
        # No need to check environment variables directly

        self._set_graph_if_not_exists = kwargs.pop("set_graph_if_not_exists", True)
        self.autocommit = kwargs.pop("autocommit", self.autocommit)
        self._read_only = kwargs.pop("read_only", self._read_only)

        logger.debug(
            "cinfo=%s, graph_name=%s, autocommit=%s",
            sanitize_connection_string_for_logging("" if cinfo is None else cinfo),
            graph_name,
            self.autocommit,
        )

        self._cinfo = conninfo.make_conninfo("" if cinfo is None else cinfo, **kwargs)
        logger.debug(
            "make_conninfo (sensitive values masked)=%s",
            sanitize_connection_params_for_logging(dict(conninfo.conninfo_to_dict(self._cinfo))),
        )

        self._ckwargs = kwargs
        self._cursor_factory = cursor_factory
        self._graph_name = graph_name

        self.connect_to_db()

        if check_graph_exists and graph_name and not self.graph_exists(graph_name):
            if create_graph:
                self.create_graph(graph_name)
            else:
                logger.warning("Graph {} does not exist!", graph_name)

                if not self._set_graph_if_not_exists:
                    self._graph_name = None

        return self

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
            raw_data: If True, return raw data without AGE processing.

        Returns:
            Tuple of (query results, execution statistics).

        Raises:
            ReadOnlyModeError: If query contains write operations in
                read-only mode.
        """
        assert isinstance(cypher_query, ParsedCypherQuery)

        # Validate read-only mode FIRST
        self._validate_read_only(cypher_query)

        if params:
            # Use prepared statement for parameterized queries
            (result, execute_stats, _) = self._execute_prepared(cypher_query, fetch_one, raw_data, params)
        else:
            # Use regular execution for non-parameterized queries
            sql, sql_params = SQLBuilder.create_cypher_sql(self._graph_name, cypher_query, params)
            (result, execute_stats, _) = self._execute_sql(sql, fetch_one, raw_data, sql_params)

        return (result, execute_stats)

    def _get_query_hash(self, query: str) -> str:
        """Generate consistent hash for query string."""
        return hashlib.md5(query.encode()).hexdigest()[:8]

    def _get_or_prepare_statement(self, cypher_sql, query: str) -> str:
        """Get existing prepared statement or create a new one."""
        query_hash = self._get_query_hash(query)

        if query_hash not in self._prepared_statements:
            logger.trace("Cache miss for query hash {}: creating new prepared statement", query_hash)

            # Remove oldest statement if cache is full
            if len(self._prepared_statements) >= self._max_cached_statements:
                oldest_hash = next(iter(self._prepared_statements))
                oldest_stmt = self._prepared_statements[oldest_hash]

                logger.trace("Cache full: deallocating oldest statement {} (hash {})", oldest_stmt, oldest_hash)

                # Deallocate old statement
                with self._fetch_cursor(row_factory=None) as cursor:
                    cursor.execute(SQL("DEALLOCATE {}").format(Identifier(oldest_stmt)))

                del self._prepared_statements[oldest_hash]

            # Create new prepared statement
            stmt_name = f"cypher_stmt_{query_hash}"
            logger.trace("Creating new prepared statement {} for query hash {}", stmt_name, query_hash)

            with self._fetch_cursor(row_factory=None) as cursor:
                prepare_sql = SQL("PREPARE {} AS {}").format(Identifier(stmt_name), cypher_sql)
                cursor.execute(prepare_sql)

            self._prepared_statements[query_hash] = stmt_name
            logger.trace("Prepared statement cached. Total cached: {}", len(self._prepared_statements))
        else:
            stmt_name = self._prepared_statements[query_hash]
            logger.trace("Cache hit for query hash {}: using prepared statement {}", query_hash, stmt_name)

        return self._prepared_statements[query_hash]

    def _execute_prepared(
        self, cypher_query: ParsedCypherQuery, fetch_one: bool = False, raw_data: bool = False, params: dict | None = None
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics, SqlStatistics]:
        """Execute a parameterized Cypher query using PostgreSQL prepared statements."""
        start_time = time.perf_counter()
        logger.debug("AGE execute prepared with params: {}", params)

        exec_stats = ExecStatistics()
        sql_stats = None
        result = None

        # Build the cypher SQL with parameter placeholder
        cypher_sql, _ = SQLBuilder.create_cypher_sql(self._graph_name, cypher_query, params)

        # Get or create cached prepared statement
        stmt_name = self._get_or_prepare_statement(cypher_sql, cypher_query.parsed_query)

        # Convert params to agtype JSON string
        params_json = json.dumps(params)

        # Execute the prepared statement
        execute_sql = SQL("EXECUTE {} (%s)").format(Identifier(stmt_name))

        try:
            with self._fetch_cursor(
                row_factory=age_row_factory(exec_stats, self._model_provider) if not raw_data else None
            ) as exec_cursor:
                exec_cursor.execute(execute_sql, (params_json,))

                if fetch_one:
                    row = exec_cursor.fetchone()
                    result = [row] if row is not None else []
                else:
                    result = exec_cursor.fetchall()

                col_names = [col.name for col in exec_cursor.description] if exec_cursor.description else []
                sql_stats = SqlStatistics(sql_stmt=cypher_sql.as_string(), col_names=col_names)

            if self.autocommit:
                self._connection.commit()

        except Exception as e:
            error_details = f"AGE query execution failed: {e}"
            self._connection.close()
            self._connection = None
            raise AGEExecutionError(error_details) from e

        exec_stats.exec_time = time.perf_counter() - start_time
        logger.debug("AGE exec_time={:.4f}s", exec_stats.exec_time)

        return (result, exec_stats, sql_stats)

    def _cleanup_prepared_statements(self):
        """Deallocate all cached prepared statements."""
        if not self._connection or not self._prepared_statements:
            return

        try:
            with self._fetch_cursor(row_factory=None) as cursor:
                for stmt_name in self._prepared_statements.values():
                    logger.trace("Deallocating prepared statement {} on disconnect", stmt_name)
                    cursor.execute(SQL("DEALLOCATE {}").format(Identifier(stmt_name)))
            self._prepared_statements.clear()
            logger.debug("Deallocated all cached prepared statements")
        except (psycopg.errors.DatabaseError, psycopg.errors.OperationalError) as e:
            logger.warning("Error deallocating prepared statements: {}", e)
            self._prepared_statements.clear()

    def execute_cypher_stream(
        self,
        cypher_query: ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> Iterator[list[Any]]:
        """Execute a Cypher query and yield results in chunks.

        AGE does not support native streaming. The StreamMixin handles
        fallback execution for backends that don't support streaming.

        Args:
            cypher_query: Parsed Cypher query to execute.
            chunk_size: Number of rows to fetch per chunk.
            raw_data: If True, return raw data without processing.

        Raises:
            NotImplementedError: AGE does not support native streaming.
        """
        raise NotImplementedError("AGE backend does not support native streaming. Use StreamMixin for fallback.")

    def fulltext_search(
        self, cypher_query: ParsedCypherQuery, fts_query: str, language: str = None
    ) -> tuple[TabularResult, ExecStatistics]:
        """Perform full-text search on graph data.

        Args:
            cypher_query: Base Cypher query to modify for full-text search.
            fts_query: Full-text search query string.
            language: Language for text search (default: "english").

        Returns:
            Tuple of (search results, execution statistics).

        """
        fts_cypher_query = convert_to_fts_query(cypher_query)

        # use default language if not overridden
        language = language or "english"

        sql = SQLBuilder.create_fts_sql(self._graph_name, fts_cypher_query, fts_query, language)

        result, execute_stats, _ = self._execute_sql(sql, False, False)

        return (result, execute_stats)

    def execute_sql(
        self,
        sql_str: str,
        fetch_one: bool = False,
        raw_data: bool = False,
    ) -> tuple[TabularResult, ExecStatistics, SqlStatistics]:
        """Execute raw SQL statement against the database.

        Args:
            sql_str: SQL statement to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without AGE processing.

        Returns:
            Tuple of (results, execution statistics, SQL statistics).

        """
        sql = SQL(sql_str)
        return self._execute_sql(sql, fetch_one, raw_data)

    def graphs(self) -> list[str]:
        """Get list of all graphs in the database.

        Returns:
            List of graph names.

        """
        sql = SQLBuilder.resolve_graphs()

        (result,), _, _ = self._execute_sql(sql, raw_data=True)

        return list(result)

    def labels(self) -> list[LabelStatistics]:
        """Get statistics for all labels in the current graph.

        Returns:
            List of label statistics including counts and types.

        """
        sql = SQLBuilder.resolve_labels(self._graph_name)
        labels = []

        result, _, _ = self._execute_sql(sql, raw_data=True)

        for row in result:
            if row[0].startswith("_ag_label_"):
                continue

            (count,), _, _ = self._execute_sql(SQLBuilder.resolve_label_count(row[2]), True, True)

            label_statistics = LabelStatistics(
                graph_name=self.graph_name,
                label_=row[0],
                type_=GraphObjectType.NODE if row[1] == "v" else GraphObjectType.EDGE,
                count=count,
            )
            labels.append(label_statistics)

        return labels

    def disconnect(self):
        """Close the database connection and clear connection info."""
        if self._connection:
            # Deallocate all cached prepared statements
            self._cleanup_prepared_statements()
            self._connection.close()
            self._connection = None
        self._cinfo = None
        self._ckwargs = None

    def reconnect(self):
        """Reconnect to the database using stored connection info."""
        assert self._cinfo is not None, "Can only reconnect if already successfully connected!"

        logger.debug("Try to reconnect")
        self.connect_to_db()

    def commit(self):
        """Commit the current transaction."""
        self._require_connection()
        self._connection.commit()

    def rollback(self):
        """Rollback the current transaction."""
        self._require_connection()
        self._connection.rollback()

    def create_graph(self, graph_name: str | None = None) -> None:
        """Create a new graph in the database.

        Args:
            graph_name: Name of the graph to create. Uses current graph if None.

        """
        self._require_connection()

        graph_name = graph_name or self._graph_name

        assert graph_name is not None, "Graph name is required"

        with self._fetch_cursor() as cursor:
            cursor.execute(SQLBuilder.create_graph(graph_name))
            self._connection.commit()

    def drop_graph(self, graph_name: str | None = None) -> None:
        """Drop a graph from the database.

        Args:
            graph_name: Name of the graph to drop. Uses current graph if None.

        """
        self._require_connection()

        graph_name = graph_name or self._graph_name

        assert graph_name is not None, "Graph name is required"

        with self._fetch_cursor() as cursor:
            cursor.execute(SQLBuilder.drop_graph(graph_name))
            self._connection.commit()

        # when drop current graph
        if graph_name == self._graph_name and not self._set_graph_if_not_exists:
            self._graph_name = None

    def graph_exists(self, graph_name: str = None) -> bool:
        """Check if a graph exists in the database.

        Args:
            graph_name: Name of the graph to check. Uses current graph if None.

        Returns:
            True if the graph exists, False otherwise.

        """
        self._require_connection()

        graph_name = graph_name or self._graph_name

        with self._fetch_cursor() as cursor:
            cursor.execute(SQLBuilder.graph_exists(graph_name))
            return cursor.fetchone()[0] == 1

    def get_capability(self, capability: BackendCapability) -> Any:
        """Get the value of a backend capability.

        Args:
            capability: The capability to query.

        Returns:
            The capability value.

        Raises:
            ValueError: If the capability is not supported.
        """
        match capability:
            case BackendCapability.LABEL_FUNCTION:
                # Pattern to retrieve single label
                return "label({node})"
            case BackendCapability.SUPPORT_MULTIPLE_LABELS:
                # AGE does not support multiple labels per node
                return False
            case BackendCapability.STREAMING_SUPPORT:
                # AGE does not support native streaming
                return False
            case _:
                # Delegate unknown capabilities to superclass
                return super().get_capability(capability)

    def create_vlabel(self, graph_name: str, vlabel: Any) -> None:
        """Create vertex label(s) in the graph.

        Args:
            graph_name: Name of the graph.
            vlabel: Vertex label name or iterable of label names.

        """
        self._require_connection()

        graph_name = graph_name or self._graph_name

        vlabels = tuple(vlabel) if isinstance(vlabel, str) else vlabel

        with self._fetch_cursor() as cursor:
            for val in vlabels:
                cursor.execute(SQLBuilder.create_vlabel(graph_name, val))
            self._connection.commit()

    def create_elabel(self, graph_name: str, elabel: str):
        """Create edge label(s) in the graph.

        Args:
            graph_name: Name of the graph.
            elabel: Edge label name or iterable of label names.

        """
        self._require_connection()

        graph_name = graph_name or self._graph_name

        elabels = tuple(elabel) if isinstance(elabel, str) else elabel

        with self._fetch_cursor() as cursor:
            for val in elabels:
                cursor.execute(SQLBuilder.create_elabel(graph_name, val))
            self._connection.commit()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((psycopg.OperationalError, ConnectionError)),
        reraise=True,
    )
    def _connect_with_retry(self):
        """Connect to PostgreSQL with retry logic for transient failures."""
        logger.debug("Attempting connection to PostgreSQL/AGE")
        connection = psycopg.connect(self._cinfo, cursor_factory=self._cursor_factory, **self._ckwargs)
        self._setup_age(connection)
        return connection

    def connect_to_db(self):
        """Establish connection to the PostgreSQL database.

        Sets up the database connection with AGE extension configuration.
        Uses retry logic to handle transient connection failures.
        """
        assert self._connection is None

        try:
            self._connection = self._connect_with_retry()
            logger.debug("Successfully connected to PostgreSQL/AGE")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL/AGE: {e}")
            self._connection = None
            raise

    def _execute_sql(
        self, sql: SQL, fetch_one: bool = False, raw_data: bool = False, params: tuple | None = None
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics, SqlStatistics]:
        """Execute SQL statement and return results with statistics.

        Args:
            sql: SQL statement to execute.
            fetch_one: If True, fetch only one result row.
            raw_data: If True, return raw data without AGE processing.

        Returns:
            Tuple of (results, execution statistics, SQL statistics).

        """
        start_time = time.perf_counter()
        logger.debug("AGE execute:\n{}", sql)

        exec_stats = ExecStatistics()
        sql_stats = None

        result = None

        with self._fetch_cursor(
            row_factory=age_row_factory(exec_stats, self._model_provider) if not raw_data else None
        ) as cursor:
            try:
                if fetch_one:
                    row = cursor.execute(sql, params).fetchone()
                    # Wrap single row in a list to match Memgraph's return format
                    result = [row] if row is not None else []
                else:
                    result = cursor.execute(sql, params).fetchall()

                col_names = [col.name for col in cursor.description]
                sql_stats = SqlStatistics(sql_stmt=sql.as_string(), col_names=col_names)

                if self.autocommit:
                    self._connection.commit()
            except (psycopg.errors.ProgrammingError, psycopg.errors.DataError) as e:
                # Handling errors like syntax errors in the statement. In that case, connection get unusable.
                # In that case as reconnect is forced
                self._connection.close()
                self._connection = None
                raise AGEExecutionError(f"AGE query execution failed: {e}") from e

        # measure executing time
        exec_stats.exec_time = time.perf_counter() - start_time
        logger.debug("exec_time={:.4f}s", exec_stats.exec_time)

        return (result, exec_stats, sql_stats)

    def _fetch_cursor(self, row_factory=None) -> psycopg.Cursor:
        """Get a database cursor with optional row factory.

        Args:
            row_factory: Optional factory for processing row results.

        Returns:
            Database cursor instance.

        """
        # reconnect if necessary
        self._require_connection()

        return self._connection.cursor(row_factory=row_factory)

    def _require_connection(self):
        """Ensure database connection is available, reconnecting if needed."""
        if self._connection is None and self._cinfo is not None:
            self.reconnect()

    def _setup_age(self, connection):
        """Set up Apache AGE extension on the database connection.

        Args:
            connection: Database connection to configure.

        """
        with connection.cursor() as cursor:
            logger.trace("AGE: Load extension")
            cursor.execute(SQLBuilder.load_age())
            logger.trace("AGE: Set search path")
            cursor.execute(SQLBuilder.set_search_path())

            aginfo = TypeInfo.fetch(connection, "agtype")
            logger.trace(f"{aginfo=}")

            if not aginfo:
                raise RuntimeError("Missing agtype information!")

        connection.adapters.register_loader(aginfo.oid, AgTypeLoader)
        connection.adapters.register_loader(aginfo.array_oid, AgTypeLoader)
