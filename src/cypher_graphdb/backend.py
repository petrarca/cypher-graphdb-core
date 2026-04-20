"""backend module: Defines execution and SQL statistics and the abstract CypherBackend interface.

ExecStatistics: Extends GraphStatistics with execution time.
SqlStatistics: Captures SQL statement and column names.
CypherBackend: Abstract base class for implementing graph database backends.
"""

import abc
from enum import Enum, auto
from typing import Any

from loguru import logger
from pydantic import BaseModel

from . import modelprovider, utils
from .cypherparser import ParsedCypherQuery, parse_cypher_query
from .modelprovider import ModelProvider
from .models import TabularResult
from .statistics import GraphStatistics, IndexInfo, LabelStatistics


class BackendCapability(Enum):
    """Enumeration of backend capabilities for feature detection."""

    LABEL_FUNCTION = auto()  # Function to get node labels
    SUPPORT_MULTIPLE_LABELS = auto()  # Support for multiple labels per node
    STREAMING_SUPPORT = auto()  # Native streaming support via server-side cursors
    PROPERTY_INDEX = auto()  # Supports create_property_index / drop_index / list_indexes
    UNIQUE_CONSTRAINT = auto()  # Supports create_unique_constraint
    FULLTEXT_INDEX = auto()  # Supports create_fulltext_index
    VECTOR_INDEX = auto()  # Supports create_vector_index


class ExecStatistics(GraphStatistics):
    """GraphStatistics with execution timing information."""

    exec_time: float = 0.0

    def __str__(self) -> str:
        return f"exec_time={self.exec_time:.3f}s, " + super().__str__()


class SqlStatistics(BaseModel):
    """SQL execution statistics, including statement and returned column names."""

    sql_stmt: str = None
    col_names: tuple[str, ...] = ()


class CypherBackend(abc.ABC):
    """Abstract base class for graph database backend implementations."""

    # Backend name - should be overridden by subclasses
    name: str = None

    def __init__(self, _, **kwargs):
        self.autocommit = kwargs.get("autocommit", True)
        self._graph_name = kwargs.get("graph_name")
        self._read_only = kwargs.get("read_only", False)

        # use global model provider if not injected in constructor
        self._model_provider = kwargs.get("model_provider", modelprovider.model_provider)

        self._connection = None

    @property
    def connection(self):
        return self._connection

    @property
    def graph_name(self) -> str:
        return self._graph_name

    @graph_name.setter
    def graph_name(self, graph_name: str):
        if self._graph_name == graph_name:
            return

        if graph_name:
            if self.graph_exists(graph_name):
                self._graph_name = graph_name
        else:
            self._graph_name = None

    @property
    def read_only(self) -> bool:
        """Return True if backend is in read-only mode."""
        return self._read_only

    @read_only.setter
    def read_only(self, value: bool):
        """Set read-only mode for the backend.

        Args:
            value: True to enable read-only mode, False to disable
        """
        self._read_only = value

    @property
    def model_provider(self) -> ModelProvider:
        return self._model_provider

    @model_provider.setter
    def model_provider(self, model_provider: ModelProvider):
        self._model_provider = model_provider

    def change_graph_name(self, graph_name: str) -> bool:
        if graph_name == self._graph_name:
            return True

        current_name = self._graph_name
        self.graph_name = graph_name

        return self._graph_name != current_name

    def get_capability(self, capability: BackendCapability) -> Any:
        """Get the value of a backend capability.

        Args:
            capability: The capability to query.

        Returns:
            The capability value.

        Raises:
            NotImplementedError: If the capability is not supported.
        """
        raise NotImplementedError(f"Backend does not support capability: {capability.value}")

    def has_capability(self, capability: BackendCapability) -> bool:
        """Check if the backend supports a specific capability.

        Args:
            capability: The capability to check.

        Returns:
            True if the capability is supported, False otherwise.
        """
        try:
            self.get_capability(capability)
            return True
        except NotImplementedError:
            return False

    @abc.abstractmethod
    def connect(self, *args, **kwargs):
        """Establish a connection to the graph database."""
        pass

    @abc.abstractmethod
    def disconnect(self):
        """Close the connection to the graph database."""
        pass

    @property
    def connected(self) -> bool:
        """Return True if currently connected to the backend."""
        return self._connection is not None

    @abc.abstractmethod
    def create_graph(self, graph_name=None):
        """Create a new graph or namespace in the backend."""
        pass

    @abc.abstractmethod
    def drop_graph(self, graph_name=None):
        """Drop an existing graph or namespace in the backend."""
        pass

    @abc.abstractmethod
    def graph_exists(self, graph_name: str = None) -> bool:
        """Check if a graph or namespace exists in the backend."""
        pass

    def parse_cypher(self, cypher_cmd: str) -> ParsedCypherQuery:
        """Parse a Cypher command string into a ParsedCypherQuery."""
        parsed_query = parse_cypher_query(cypher_cmd)
        logger.trace(parsed_query.model_dump_json(indent=2))

        return parsed_query

    def _validate_read_only(self, parsed_query: ParsedCypherQuery):
        """Validate query is allowed in read-only mode.

        Args:
            parsed_query: Parsed Cypher query to validate.

        Raises:
            ReadOnlyModeError: If query contains write operations in
                read-only mode.
        """
        if self._read_only and parsed_query.has_updating_clause():
            from .exceptions import ReadOnlyModeError

            raise ReadOnlyModeError(
                f"Write operation not allowed in read-only mode. Query contains updating clause: {parsed_query.parsed_query}"
            )

    @abc.abstractmethod
    def execute_cypher(
        self,
        cypher_query: ParsedCypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> tuple[TabularResult, ExecStatistics]:
        """Execute a parsed Cypher query and return results and execution stats.

        Args:
            cypher_query: Parsed Cypher query to execute.
            fetch_one: If True, return only the first result row.
            raw_data: If True, return raw data without processing.
            params: Optional dictionary of parameter values to bind (e.g., {"key": "value"}).
        """
        pass

    @abc.abstractmethod
    def execute_cypher_stream(
        self,
        cypher_query: ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
        params: dict | None = None,
    ):
        """Execute a parsed Cypher query and yield results in chunks.

        Args:
            cypher_query: Parsed Cypher query to execute
            chunk_size: Number of rows to fetch per chunk
            raw_data: If True, return raw data without processing
            params: Optional dictionary of parameter values to bind (e.g., {"key": "value"}).

        Yields:
            Lists of result rows (TabularResult chunks)
        """
        pass

    @abc.abstractmethod
    def fulltext_search(
        self,
        cypher_query: ParsedCypherQuery,
        fts_query: str,
        language: str = None,
    ) -> tuple[TabularResult, ExecStatistics]:
        """Perform a full-text search via a Cypher query and return results and execution stats."""
        pass

    def execute_sql(
        self,
        sql_str: str,
        fetch_one: bool = False,
        raw_data: bool = False,
    ) -> tuple[TabularResult, ExecStatistics, SqlStatistics]:
        """Execute a SQL statement if supported, else raise an error.

        Raises:
            ReadOnlyModeError: If in read-only mode.
            RuntimeError: If backend doesn't support SQL execution.
        """
        if self._read_only:
            from .exceptions import ReadOnlyModeError

            raise ReadOnlyModeError("Direct SQL execution not allowed in read-only mode")
        raise RuntimeError("Backend does not support SQL execution!")

    @abc.abstractmethod
    def labels(self) -> list[LabelStatistics]:
        """Return statistics per label across all nodes in the graph."""
        return []

    @abc.abstractmethod
    def graphs(self) -> list[LabelStatistics]:
        """Return statistics per graph/namespace available in the backend."""
        return []

    @abc.abstractmethod
    def commit(self):
        """Commit pending transactions to the backend."""
        pass

    @abc.abstractmethod
    def rollback(self):
        """Rollback pending transactions, discarding changes."""
        pass

    # ── Index management (opt-in, not abstract) ────────────────────────────

    def create_property_index(self, label: str, *property_names: str) -> None:
        """Create a property index on the given label.

        For backends like AGE where a single GIN index covers all properties,
        the property_names parameter may be ignored. For backends like Neo4j,
        Memgraph, and FalkorDB, each property gets its own index.

        Consumers should always pass property_names for portability.

        Args:
            label: Node label to index (e.g. "Method").
            *property_names: Property names to index. Ignored by some backends.

        Raises:
            NotImplementedError: If the backend does not support property indexes.
        """
        raise NotImplementedError(f"Backend {self.name} does not support create_property_index")

    def drop_index(self, label: str, *property_names: str) -> None:
        """Drop a property index on the given label.

        Args:
            label: Node label whose index to drop.
            *property_names: Property names of the index. Ignored by some backends.

        Raises:
            NotImplementedError: If the backend does not support drop_index.
        """
        raise NotImplementedError(f"Backend {self.name} does not support drop_index")

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:
        """List all indexes on the current graph.

        Args:
            include_internal: If True, also return backend-internal indexes
                (e.g. AGE's _pkey, _start_id_idx, _end_id_idx). Default False.

        Returns:
            List of IndexInfo objects describing each index.

        Raises:
            NotImplementedError: If the backend does not support list_indexes.
        """
        raise NotImplementedError(f"Backend {self.name} does not support list_indexes")

    # ── Bulk write operations (opt-in, not abstract) ──────────────────────

    def bulk_create_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        """Create nodes in batches using UNWIND.

        For AGE, rows are serialized as inline Cypher literals (AGE does not
        support $params in UNWIND). For other backends, parameterized UNWIND
        is used.

        Args:
            label: Node label for all created nodes.
            rows: List of property dicts, one per node.
            batch_size: Number of nodes per UNWIND batch.

        Returns:
            Total number of nodes created.

        Raises:
            NotImplementedError: If the backend does not support bulk_create_nodes.
        """
        raise NotImplementedError(f"Backend {self.name} does not support bulk_create_nodes")

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
        """Create edges in batches by matching src/dst nodes on a key property.

        Each dict in edges must have "src" and "dst" keys whose values match
        the src_key/dst_key properties on source/destination nodes. Any
        additional keys are set as properties on the created edge.

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

        Raises:
            NotImplementedError: If the backend does not support bulk_create_edges.
        """
        raise NotImplementedError(f"Backend {self.name} does not support bulk_create_edges")

    @property
    def __dict__(self):
        """Return backend state as a dictionary for introspection."""
        return utils.to_collection(
            {
                "name": self.name,
                "graph_name": self.graph_name,
                "autocommit": self.autocommit,
                "connection": self._connection,
            }
        )
