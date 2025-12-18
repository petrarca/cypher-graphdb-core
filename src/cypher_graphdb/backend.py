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
from .statistics import GraphStatistics, LabelStatistics


class BackendCapability(Enum):
    """Enumeration of backend capabilities for feature detection."""

    LABEL_FUNCTION = auto()  # Function to get node labels
    SUPPORT_MULTIPLE_LABELS = auto()  # Support for multiple labels per node


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
    ) -> tuple[TabularResult, ExecStatistics]:
        """Execute a parsed Cypher query and return results and execution stats."""
        pass

    @abc.abstractmethod
    def execute_cypher_stream(
        self,
        cypher_query: ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
    ):
        """Execute a parsed Cypher query and yield results in chunks.

        Args:
            cypher_query: Parsed Cypher query to execute
            chunk_size: Number of rows to fetch per chunk
            raw_data: If True, return raw data without processing

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
