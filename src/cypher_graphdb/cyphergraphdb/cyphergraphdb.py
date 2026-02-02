"""cyphergraphdb module: Core client for connecting to and querying a Cypher-based graph database.

Provides CypherGraphDB for managing connections, executing queries, and CRUD operations on graph data.
The examples in this documentation focus on using Memgraph as the backend.

The library supports two approaches for working with graph data:

1. Untyped approach: Work directly with dictionaries and raw data structures.
   Simple but lacks type safety and validation.

2. Typed (ORM-like) approach: Define Pydantic models for nodes and edges.
   Provides type safety, validation, and better IDE support.

Exemplary Data Model:

Typed approach with Product and Technology entities:

    # Node model with relation definition
    @node()
    @relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
    class Product(GraphNode):
        name: str
        version: str | None = None
        description: str | None = None

        def is_stable(self) -> bool:
            # Check if product is stable
            return self.version and not self.version.endswith('-beta')

    # Simple node model
    @node(label="Technology")  # Custom label (otherwise class name is used)
    class Technology(GraphNode):
        name: str
        category: str | None = None

        def is_database(self) -> bool:
            # Check if this is a database technology
            return self.category == "Database"

    # Edge model connecting Product to Technology
    @edge(label="USES_TECHNOLOGY")
    class UsesTechnology(GraphEdge):
        since: int | None = None  # Year when technology was adopted
        version: str | None = None

With this typed approach:
- Objects are returned as instances of your model classes
- Field validation is performed automatically
- Custom methods can be added to models
- IDE autocompletion works with your custom fields
- Relationships can be defined and validated

The same operations can be performed with the untyped approach, but without
the benefits of type safety and validation.
"""

from collections.abc import Callable
from typing import Any, Literal

from loguru import logger

from .. import config, modelprovider, utils
from ..backend import CypherBackend
from ..backendprovider import backend_provider
from ..cypherbuilder import CypherBuilder
from ..cypherparser import ParsedCypherQuery
from ..exceptions import ReadOnlyModeError
from ..models import Graph, GraphEdge, GraphNode, GraphObject, TabularResult
from .batch import BatchMixin
from .connection import ConnectionMixin
from .criteria import MatchCriteria, MatchEdgeCriteria, MatchNodeCriteria
from .result import QueryResult
from .schema import SchemaMixin
from .search import SearchMixin
from .sql import SqlMixin
from .stream_mixin import StreamMixin


class CypherGraphDB(ConnectionMixin, BatchMixin, SchemaMixin, SearchMixin, SqlMixin, StreamMixin):
    """Primary client for connecting to and managing Cypher-based graph databases.

    CypherGraphDB provides a high-level interface for graph database operations
    including connection management, CRUD operations, query execution, and
    transaction handling. It supports multiple backends (Neo4j, Memgraph,
    Apache AGE) through a unified API.

    Key Features:
        - Multiple backend support with automatic detection
        - Type-safe graph models with Pydantic integration
        - Flexible query execution with result formatting
        - Transaction management and statistics tracking
        - Full-text search capabilities
        - Connection pooling and context manager support

    Basic Usage:
        ```python
        from cypher_graphdb import CypherGraphDB
        # Import the model classes defined above or from your own module
        from my_models import Product, Technology, UsesTechnology

        # Connection parameters can be resolved from multiple sources, here we use the
        # Approach 1: Direct connection (remember to disconnect when done).
        cdb = CypherGraphDB().connect()
        try:
            # Execute raw Cypher
            result = cdb.execute("MATCH (n) RETURN count(n)")

            # Fetch nodes by criteria
            products = cdb.fetch_nodes({"label_": "Product", "name": "CypherGraph"})

            # Create/merge graph objects
            product = Product(name="CypherGraph", version="1.0")
            cdb.create_or_merge(product)
        finally:
            # Always disconnect when done
            cdb.disconnect()

        # Approach 2: Context manager (recommended - automatically disconnects)
        with CypherGraphDB() as cdb:
            # Connect to database
            cdb.connect()

            # Execute operations
            result = cdb.execute("MATCH (n) RETURN count(n)")
            product = Product(name="CypherGraph", version="1.0")
            cdb.create_or_merge(product)

            # No need to call disconnect() - handled automatically
        ```

    Args:
        backend: Backend type (supported: "memgraph", "age") or CypherBackend instance.
            If not provided, will use CGDB_BACKEND environment variable or .env file.
            Examples: CypherGraphDB("memgraph"), CypherGraphDB("age"), CypherGraphDB()
        connect_url: Optional connection URL for auto-connection
        connect_params: Optional connection parameters for auto-connection

    Attributes:
        on_before_execute: Callback executed before each query
            (for security/validation)
        on_after_execute: Callback executed after each query
            (for logging/monitoring)
    """

    def _resolve_backend(self, backend: CypherBackend | str | None) -> CypherBackend:
        """Resolve backend from parameter or settings.

        Args:
            backend: Backend parameter (can be None)

        Returns:
            Resolved CypherBackend instance

        Raises:
            RuntimeError: If no backend can be resolved
        """
        # If backend is explicitly provided, use it
        if backend is not None:
            return backend_provider.check_and_resolve(backend, True)

        # Try to get backend from settings
        settings_backend = self.get_settings().backend
        if settings_backend:
            logger.debug(f"Using backend from settings: {settings_backend}")
            return backend_provider.check_and_resolve(settings_backend, True)

        # No backend available - raise error with helpful message
        raise RuntimeError(
            "No backend specified! Please provide either:\n"
            "1. backend parameter: CypherGraphDB('memgraph')\n"
            "2. CGDB_BACKEND environment variable\n"
            "3. .env file with CGDB_BACKEND=<backend_name>"
        )

    def __init__(
        self,
        backend: CypherBackend | str | None = None,
        connect_url: str | None = None,
        connect_params: dict | None = None,
    ):
        backend = self._resolve_backend(backend)
        assert backend

        self._backend = backend
        self._model_provider = modelprovider.model_provider
        # inject into the backend
        self._backend.model_provider = self._model_provider

        # will be called before executing the query for e.g. security checks
        self.on_before_execute: Callable = lambda parsed_query: True
        # will be called after executing the query
        self.on_after_execute: Callable = lambda result, parsed_query: None

        # Log current settings values for debugging
        settings = self.get_settings()
        logger.debug(f"Current settings: backend={settings.backend} cinfo={settings.cinfo_sanitized} graph={settings.graph}")

        # Auto-connect if connection parameters are provided
        self._auto_connect_if_params(connect_url, connect_params)

    @property
    def id(self) -> str:
        """Get the unique identifier for this database connection.

        Returns:
            String identifier combining backend name and graph name.
        """
        if self._backend:
            if self._backend.graph_name:
                return f"{self._backend.name}:{self._backend.graph_name}"
            else:
                return self._backend.name
        else:
            return ""

    @property
    def graph_name(self) -> str | None:
        """Get the name of the current graph.

        Returns:
            Graph name string, or None if no backend connected.
        """
        return self._backend.graph_name if self._backend else None

    @property
    def backend(self) -> Any:
        """Get the backend database interface.

        Returns:
            Backend interface instance.
        """
        return self._backend

    @property
    def model_provider(self) -> Any:
        """Get the model provider for graph object type management.

        Returns:
            ModelProvider instance for managing graph model classes.
        """
        return self._model_provider

    @staticmethod
    def get_settings() -> config.Settings:
        """Get the singleton settings instance.

        Returns:
            Settings: The cached settings instance with environment variables.
        """
        from ..settings import get_settings

        return get_settings()

    # Property for convenience access
    settings = property(lambda self: self.get_settings())

    @property
    def read_only(self) -> bool:
        """Check if the connection is in read-only mode.

        Returns:
            True if the connection is in read-only mode, False otherwise

        Example:
            ```python
            with CypherGraphDB() as cdb:
                cdb.connect(read_only=True)
                print(cdb.read_only)  # True
            ```
        """
        return self._backend.read_only if self._backend else False

    @read_only.setter
    def read_only(self, value: bool):
        """Set the read-only mode for the connection.

        Args:
            value: True to enable read-only mode, False to disable

        Raises:
            RuntimeError: If backend is not initialized

        Example:
            ```python
            with CypherGraphDB() as cdb:
                cdb.connect()
                cdb.read_only = True  # Enable read-only mode
                # Now write operations will be blocked
                cdb.read_only = False  # Disable read-only mode
                # Write operations are allowed again
            ```
        """
        if not self._backend:
            raise RuntimeError("Cannot set read-only mode: backend not initialized")
        self._backend.read_only = value

    def fetch(
        self,
        criteria: MatchCriteria,
        unnest_result: str | bool = None,
        fetch_one: bool | None = None,
    ) -> Any:
        """Universal fetch method that dispatches to nodes or edges based on criteria type.

        Convenience method that automatically routes to fetch_nodes or fetch_edges
        based on the type of matching criteria provided.

        Args:
            criteria: MatchNodeCriteria for nodes or MatchEdgeCriteria for edges
            unnest_result: Result formatting (same as execute method)
            fetch_one: Override automatic single-result detection

        Returns:
            Matching nodes or edges formatted according to unnest_result parameter

        Examples:
            ```python
            from cypher_graphdb import MatchNodeCriteria, MatchEdgeCriteria

            with CypherGraphDB() as cdb:
                cdb.connect()

                # Fetch products (automatically routes to fetch_nodes)
                product_criteria = MatchNodeCriteria(
                    label_="Product",
                    properties_={"name": "CypherGraph"}
                )
                products = cdb.fetch(product_criteria)

                # Fetch technology relationships (automatically routes to fetch_edges)
                edge_criteria = MatchEdgeCriteria(
                    label_="USES_TECHNOLOGY",
                    start_criteria_=product_criteria,
                    end_criteria_=MatchNodeCriteria(
                        label_="Technology",
                        properties_={"name": "Python"}
                    ),
                    fetch_nodes_=True
                )
                relations = cdb.fetch(edge_criteria)
            ```
        """
        assert self._backend

        if isinstance(criteria, MatchNodeCriteria):
            return self.fetch_nodes(criteria, unnest_result, fetch_one)
        if isinstance(criteria, MatchEdgeCriteria):
            return self.fetch_edges(criteria, unnest_result, fetch_one)

        return None

    def fetch_nodes(
        self,
        criteria: MatchCriteria | int | str | dict[str, Any],
        unnest_result: str | bool = None,
        fetch_one: bool | None = None,
    ) -> Any:
        """Fetch GraphNode instances from the database using flexible criteria.

        Provides multiple ways to specify node matching criteria with intelligent
        result formatting. Automatically determines fetch_one behavior based on criteria.

        Args:
            criteria: Node matching specification:
                - int: Match by database ID
                - str: Match by GID (global identifier) property
                - dict: Match by properties and/or metadata
                - MatchCriteria: Advanced matching with labels, projections, etc.
            unnest_result: Result formatting (same as execute method)
            fetch_one: Override automatic single-result detection

        Returns:
            Matching nodes formatted according to unnest_result parameter

        Examples:
            ```python
            from cypher_graphdb import MatchNodeCriteria

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Fetch by database ID
                node = cdb.fetch_nodes(12345, unnest_result=True)
                # Returns: Product(id_=12345, name='CypherGraph', ...)

                # Fetch by GID (string identifier)
                node = cdb.fetch_nodes("6f628f1e7tFZHfis", unnest_result=True)
                # Returns: Product with name='CypherGraph'

                # Fetch by simple properties
                products = cdb.fetch_nodes({"label_": "Product", "name": "CypherGraph"})
            # Returns: [Product(...), ...]

            # Advanced criteria with labels and projections
            criteria = MatchNodeCriteria(
                label_="Technology",  # Single label
                properties_={"name": "Python"},
                projection_=["n.name", "n.category", "id(n)"]  # Custom fields
            )
            python_tech = cdb.fetch_nodes(criteria)

            # Fetch all nodes of a type
            all_technologies = cdb.fetch_nodes({"label_": "Technology"})

            # Get first match only
            database_tech = cdb.fetch_nodes(
                {"label_": "Technology", "name": "PostgreSQL"},
                fetch_one=True,
                unnest_result=True
            )

            # Using model classes for type safety
            from my_models import Technology
            memgraph = cdb.fetch_nodes({
                "label_": Technology,  # Use class instead of string
                "name": "Memgraph"
            }, unnest_result=True)
            ```
        """
        assert self._backend

        if isinstance(criteria, int):
            criteria = MatchNodeCriteria(id_=criteria)
        elif isinstance(criteria, str):
            criteria = MatchNodeCriteria(properties_={config.PROP_GID: criteria})
        elif isinstance(criteria, dict):
            if criteria:
                fields, props = utils.slice_model_properties(MatchCriteria, criteria)
                if props:
                    fields.update({"properties_": props})

                criteria = MatchNodeCriteria(**fields)
        else:
            assert isinstance(criteria, MatchCriteria)

        criteria.resolve()

        fetch_one = fetch_one if fetch_one is not None else criteria.has_unique_ids

        return self._fetch_node_by_criteria(criteria, unnest_result, fetch_one)

    def fetch_edges(
        self,
        criteria: MatchCriteria | int | str | dict[str, Any],
        unnest_result: str | bool = None,
        fetch_one: bool | None = None,
    ) -> Any:
        """Fetch GraphEdge instances from the database using flexible criteria.

        Similar to fetch_nodes but for relationships/edges. Supports additional
        filtering on start/end nodes and optional node fetching.

        Args:
            criteria: Edge matching specification (same formats as fetch_nodes)
            unnest_result: Result formatting (same as execute method)
            fetch_one: Override automatic single-result detection

        Returns:
            Matching edges formatted according to unnest_result parameter

        Examples:
            ```python
            from cypher_graphdb import MatchEdgeCriteria, MatchNodeCriteria

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Fetch by database ID
                edge = cdb.fetch_edges(67890, unnest_result=True)

                # Fetch by edge properties
                tech_relations = cdb.fetch_edges({
                    "label_": "USES_TECHNOLOGY",
                    "since": 2023
                })

            # Advanced criteria with start/end node filtering
            criteria = MatchEdgeCriteria(
                label_="USES_TECHNOLOGY",
                start_criteria_=MatchNodeCriteria(
                    label_="Product",
                    properties_={"name": "CypherGraph"}
                ),
                end_criteria_=MatchNodeCriteria(
                    label_="Technology",
                    properties_={"name": "Python"}
                ),
                fetch_nodes_=True  # Include connected nodes in results
            )
            python_usage = cdb.fetch_edges(criteria)

            # Get all edges of a specific type
            all_tech_relations = cdb.fetch_edges({"label_": "USES_TECHNOLOGY"})

            # Fetch with node information included
            criteria = MatchEdgeCriteria(
                label_="USES_TECHNOLOGY",
                start_criteria_=MatchNodeCriteria(
                    label_="Product"
                ),
                fetch_nodes_=True  # Include start and end nodes
            )
            product_technologies = cdb.fetch_edges(criteria)
            ```
        """
        assert self._backend

        if isinstance(criteria, int):
            criteria = MatchEdgeCriteria(id_=criteria)
        elif isinstance(criteria, str):
            criteria = MatchEdgeCriteria(properties_={config.PROP_GID: criteria})
        elif isinstance(criteria, dict):
            if criteria:
                fields, props = utils.slice_model_properties(MatchCriteria, criteria)
                if props:
                    fields.update({"properties_": props})

                criteria = MatchNodeCriteria(**fields)
        else:
            assert isinstance(criteria, MatchCriteria)

        criteria.resolve()

        fetch_one = fetch_one if fetch_one is not None else criteria.has_unique_ids

        return self._fetch_edge_by_criteria(criteria, unnest_result, fetch_one)

    def fetch_nodes_by_ids(self, node_ids: list[int]) -> list[GraphNode]:
        """Fetch nodes by their database IDs.

        Args:
            node_ids: List of database IDs to fetch.

        Returns:
            List of graph nodes matching the IDs.
        """
        assert self._backend

        result = self._parse_and_execute(CypherBuilder.fetch_nodes_by_ids(node_ids))

        # Transform list of tuples [(node1,), (node2,), ...] to list of nodes [node1, node2, ...]
        return [node[0] for node in result] if result else []

    def create_or_merge(
        self,
        obj: GraphNode | GraphEdge | Graph,
        strategy: Literal["merge", "force_create"] = config.CREATE_OR_MERGE_STRATEGY[0],
    ) -> GraphNode | GraphEdge | Graph:
        """Create or merge graph objects (nodes, edges, or entire graphs) into the database.

        Intelligently handles object persistence based on the chosen strategy.
        For nodes and edges, automatically assigns database IDs and maintains object state.

        Args:
            obj: Graph object to persist (GraphNode, GraphEdge, or Graph)
            strategy: Creation strategy - "merge" (default) or "force_create"
                - "merge": Update existing objects or create if not found
                - "force_create": Always create new objects, ignore existing ones

        Returns:
            The same object with updated database ID and properties

        Examples:
            ```python
            from cypher_graphdb import CypherGraphDB
            from cypher_graphdb.models import Graph
            # Import the model classes defined above or from your own module
            from my_models import Product, Technology, UsesTechnology

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Create/merge individual nodes
                product = Product(name="CypherGraph", version="1.0", description="Graph database client")
                product = cdb.create_or_merge(product)  # Returns product with ID assigned
                print(f"Product ID: {product.id_}")

                technology = Technology(name="Python", category="Programming Language")
                technology = cdb.create_or_merge(technology, strategy="merge")

            # Create/merge edges between nodes
            uses = UsesTechnology(
                start_id_=product.id_,
                end_id_=technology.id_,
                since=2023,
                version="3.10"
            )
            uses = cdb.create_or_merge(uses)

            # Force create (always creates new, ignores existing)
            duplicate_product = Product(name="CypherGraph", version="1.1")
            new_product = cdb.create_or_merge(duplicate_product, strategy="force_create")
            # This creates a new product even if one exists

            # Create/merge entire graphs at once
            graph = Graph(nodes=[product, technology], edges=[uses])
            cdb.create_or_merge(graph)
            ```

        Raises:
            ReadOnlyModeError: If the connection is in read-only mode
        """
        assert self._backend

        # Validate read-only mode
        if self.read_only:
            raise ReadOnlyModeError("Cannot execute CREATE or MERGE in read-only mode")

        assert strategy in config.CREATE_OR_MERGE_STRATEGY, f"Invalid strategy {strategy}!"

        if isinstance(obj, GraphNode):
            return self._create_or_merge_node(obj, strategy)
        if isinstance(obj, GraphEdge):
            return self._create_or_merge_edge(obj, strategy)
        if isinstance(obj, Graph):
            return self._create_or_merge_graph(obj, strategy)

        raise RuntimeError(f"Unsupported objectobject type to merge/create: {type(obj)} with strategy {strategy}")

    def delete(self, obj: GraphNode | GraphEdge | MatchCriteria, detach: bool = False) -> int | Any:
        """Delete graph entities from the database.

        Supports deletion of individual nodes/edges by object reference or
        bulk deletion using match criteria. For nodes, optionally detaches
        all relationships before deletion.

        Args:
            obj: Object to delete - GraphNode, GraphEdge, or MatchCriteria
            detach: For nodes, whether to detach relationships before deletion
                   (prevents constraint violation errors)

        Returns:
            Number of entities deleted or deletion result

        Examples:
            ```python
            from cypher_graphdb import MatchNodeCriteria, MatchEdgeCriteria

            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Delete specific node by object (must have ID)
                product = cdb.fetch_nodes({"label_": "Product", "name": "CypherGraph"}, fetch_one=True)
                cdb.delete(product, detach=True)  # Detach all relationships first

                # Delete specific edge by object
                edge = cdb.fetch_edges({"label_": "USES_TECHNOLOGY"}, fetch_one=True)
                cdb.delete(edge)

            # Bulk delete nodes by criteria
            criteria = MatchNodeCriteria(
                label_="Technology",
                properties_={"name": "Python"}
            )
            deleted_count = cdb.delete(criteria, detach=True)
            print(f"Deleted {deleted_count} Python technology nodes")

            # Bulk delete edges by criteria
            edge_criteria = MatchEdgeCriteria(
                label_="USES_TECHNOLOGY",
                start_criteria_=MatchNodeCriteria(
                    label_="Product",
                    properties_={"name": "CypherGraph"}
                )
            )
            cdb.delete(edge_criteria)
            ```

        Warning:
            Deleting nodes without detach=True will fail if the node has
            relationships. Always use detach=True for nodes unless you're
            certain they have no relationships.

        Raises:
            ReadOnlyModeError: If the connection is in read-only mode
        """
        assert self._backend

        # Validate read-only mode
        if self.read_only:
            raise ReadOnlyModeError("Cannot execute DELETE in read-only mode")

        if isinstance(obj, GraphNode):
            return self._delete_node_by_id(obj, detach)
        if isinstance(obj, GraphEdge):
            return self._delete_edge_by_id(obj)
        if isinstance(obj, MatchNodeCriteria):
            return self._delete_node_by_criteria(obj, detach)
        if isinstance(obj, MatchEdgeCriteria):
            return self._delete_edge_by_criteria(obj)

        raise RuntimeError(f"Unsupported objectobject type to delete: {type(obj)}")

    def parse(self, cypher_cmd: str) -> ParsedCypherQuery:
        """Parse a Cypher command into an internal query representation.

        Analyzes the Cypher query structure for optimization, security validation,
        and execution planning. The parsed query can be reused for multiple executions.

        Args:
            cypher_cmd: Raw Cypher query string to parse

        Returns:
            ParsedCypherQuery object containing query analysis and metadata

        Examples:
            ```python
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Parse a query for analysis
                query = "MATCH (p:Product {name: $name}) RETURN p"
                parsed = cdb.parse(query)

                # Access parsing results
                print(f"Query type: {parsed.query_type}")
                print(f"Parameters: {parsed.parameters}")
                print(f"Labels used: {parsed.labels}")

                # Reuse parsed query for multiple executions
                result1 = db.execute(parsed, {"name": "CypherGraph"})
                result2 = db.execute(parsed, {"name": "AnotherProduct"})

                # Parse complex analytical query
                analytical = db.parse('''
                    MATCH (p:Product)-[:USES_TECHNOLOGY]->(t:Technology)
                    WHERE t.category = $category
                    RETURN t.name, count(p) AS product_count
                    ORDER BY product_count DESC
                    LIMIT 10
                ''')
                print(f"Estimated complexity: {analytical.complexity}")
            ```
        """
        return self._parse_cypher(cypher_cmd)

    def execute(
        self,
        cypher_cmd: str | ParsedCypherQuery,
        unnest_result: str | bool = None,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> Any | TabularResult:
        """Execute a Cypher command and return results with flexible formatting options.

        Provides the primary interface for executing raw Cypher queries with extensive
        result formatting and optimization options.

        Args:
            cypher_cmd: Cypher query string or pre-parsed ParsedCypherQuery
            unnest_result: Result formatting option:
                - None: Return list of tuples/objects as-is
                - True/"r": Return single result if only one row
                - "c": Return first column if only one column
                - "rc": Return single value if single row and column
            fetch_one: Optimize for single result (stops after first match)
            raw_data: Return raw database results without object transformation

        Returns:
            Query results formatted according to unnest_result parameter

        Examples:
                ```python
                # Using context manager (recommended approach)
                with CypherGraphDB() as cdb:
                    cdb.connect()

                    # Basic query execution
                    result = cdb.execute("MATCH (n:Product) RETURN n.name, n.gid_ LIMIT 3")
                    # Returns: [('CypherGraph', '6f628f1e7tFZHfis'), ('PostgreSQL', '940652f10K2xQobe'), ...]

                    # Get single result
                    count = cdb.execute(
                        "MATCH (n:Product) RETURN count(n)",
                        unnest_result="rc"  # single row, single column -> scalar
                    )
                    # Returns: 265 (just the count number)

                    # Get first column only
                    names = cdb.execute(
                        "MATCH (n:Technology) RETURN n.name, n.gid_ ORDER BY n.name LIMIT 3",
                        unnest_result="c"
            )
            # Returns: ['Python', 'PostgreSQL', 'Memgraph'] (just names)

            # Optimized single result fetch
            product = cdb.execute(
                "MATCH (n:Product {name: 'CypherGraph'}) RETURN n",
                fetch_one=True,
                unnest_result="rc"
            )
            # Returns: Product object (stops after first match)

            # Complex analytical query
            stats = cdb.execute('''
                MATCH (p:Product)-[r:USES_TECHNOLOGY]->(t:Technology)
                RETURN t.name AS technology,
                       count(p) AS product_count,
                       collect(p.name)[..3] AS example_products
                ORDER BY product_count DESC
                LIMIT 3
            ''')
            # Returns: [('Python', 101, ['CypherGraph', 'ProductA', 'ProductB']), ...]

            # Raw database results (no object conversion)
            raw = cdb.execute(
                "MATCH (n:Technology) RETURN n",
                raw_data=True
            )
            # Returns raw database node objects
            ```
        """
        assert self._backend

        logger.debug("Execute cypher (unnest_result={}, fetch_one={}): {}", unnest_result, fetch_one, cypher_cmd)
        result = self._parse_and_execute(cypher_cmd, fetch_one, raw_data, params)

        return utils.unnest_result(result, unnest_result)

    def execute_with_stats(
        self,
        cypher_cmd: str | ParsedCypherQuery,
        unnest_result: str | bool = None,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> QueryResult:
        """Execute a Cypher command and return immutable QueryResult with statistics.

        Provides complete query execution information without mutable state,
        ensuring thread safety and clean state ownership.

        Args:
            cypher_cmd: Cypher query string or pre-parsed ParsedCypherQuery
            unnest_result: Result formatting option (same as execute method)
            fetch_one: Optimize for single result (stops after first match)
            raw_data: Return raw database results without object transformation

        Returns:
            QueryResult containing data, execution statistics, and parsed query

        Examples:
            ```python
            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Full query information
                result = cdb.execute_with_stats("MATCH (p:Product) RETURN p.name", unnest_result="c")
                data = result.data  # ['Product1', 'Product2', ...]
                stats = result.exec_statistics  # Execution metrics
                parsed = result.parsed_query  # Query analysis

                # Performance monitoring
                if result.exec_statistics.exec_time > 1.0:
                    print(f"Slow query: {result.exec_statistics.exec_time}s")

                # Check data types
                if result.has_graph_data():
                    print(f"Graph result with {result.exec_statistics.nodes} nodes")
                elif result.has_tabular_data():
                    print(f"Tabular result with {result.exec_statistics.row_count} rows")
            ```
        """
        assert self._backend

        logger.debug(f"Execute cypher with stats {unnest_result=}, {fetch_one=}: \n{cypher_cmd}")

        # Parse the query to get the parsed query object
        parsed_query = self._parse_cypher(cypher_cmd)

        # Execute the query and get statistics
        result, exec_stats = self._backend.execute_cypher(parsed_query, fetch_one=fetch_one, raw_data=raw_data, params=params)

        # Create immutable QueryResult
        return QueryResult(
            data=utils.unnest_result(result, unnest_result),
            exec_statistics=exec_stats,
            sql_statistics=None,  # Only populated for SQL queries
            parsed_query=parsed_query,
        )

    def _auto_connect_if_params(self, connect_url: str | None, connect_params: dict | None):
        """Auto-connect if connection parameters are provided."""
        if connect_url is not None:
            logger.debug(f"Auto-connecting with URL={connect_url}, params={connect_params}")
            self.connect(connect_url=connect_url, **(connect_params or {}))
        elif connect_params is not None:
            logger.debug(f"Auto-connecting with params={connect_params}")
            self.connect(**connect_params)

    def _create_or_merge_node(self, obj, strategy) -> GraphNode:
        obj.resolve()

        match strategy:
            case "merge":
                if self._resolve_obj_id(obj, self._fetch_node_by_criteria):
                    return self._merge_node(obj)
                else:
                    return self._create_node(obj)
            case "force_create":
                return self._create_node(obj)
            case _:
                raise RuntimeError(f"Invalid strategy {strategy}")

    def _create_node(self, obj) -> GraphNode:
        obj.create_gid_if_missing()

        cypher_cmd = CypherBuilder.create_node(obj.label_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # only the object id needs to be updated
        # result is [(id,)], extract the integer from the tuple
        obj.bind_id(result[0][0])

        return obj

    def _merge_node(self, obj) -> GraphNode:
        cypher_cmd = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # merge results back from the retrieved one, could be modified in the meantime
        # result is [(node_obj,)], extract the node object from the tuple
        updated_node = result[0][0]
        obj.__dict__.update(updated_node.__dict__)

        return obj

    def _fetch_node_by_criteria(self, criteria: MatchCriteria, unnest_result: str, fetch_one: bool):
        cypher_cmd = CypherBuilder.fetch_node_by_criteria(criteria)

        result = self._parse_and_execute(cypher_cmd, fetch_one)

        return utils.unnest_result(result, unnest_result)

    def _delete_node_by_id(self, obj, detach) -> int:
        assert obj.id_ is not None

        criteria = MatchNodeCriteria(id_=obj.id_)
        cypher_cmd = CypherBuilder.delete_node_by_criteria(criteria, detach)

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # mark node as not persisted
        obj.unbind_id()

        return result[0]

    def _delete_node_by_criteria(self, criteria: MatchCriteria, detach: bool):
        cypher_cmd = CypherBuilder.delete_node_by_criteria(criteria, detach)

        return self._parse_and_execute(cypher_cmd)

    def _create_or_merge_edge(self, obj, strategy: str) -> int:
        obj.resolve()

        match strategy:
            case "merge":
                if self._resolve_obj_id(obj, self._fetch_edge_by_criteria):
                    return self._merge_edge(obj)
                else:
                    return self._create_edge(obj)
            case "force_create":
                return self._create_edge(obj)
            case _:
                raise RuntimeError(f"Invalid strategy {strategy}")

    def _create_edge(self, obj) -> GraphEdge:
        obj.create_gid_if_missing()

        cypher_cmd = CypherBuilder.create_edge(obj.label_, obj.start_id_, obj.end_id_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        # result is [(id,)], extract the integer from the tuple
        obj.bind_id(result[0][0])

        return obj

    def _merge_edge(self, obj) -> GraphEdge:
        cypher_cmd = CypherBuilder.merge_edge_by_id(obj.id_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # merge results back from the retrieved one, could be modified in the meantime
        # result is [(edge_obj,)], extract the edge object from the tuple
        updated_edge = result[0][0]
        obj.__dict__.update(updated_edge.__dict__)

        return obj

    def _delete_edge_by_id(self, obj) -> int:
        assert obj.id_ is not None

        criteria = MatchEdgeCriteria(id_=obj.id_)
        cypher_cmd = CypherBuilder.delete_edge_by_criteria(criteria)

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # mark edge as not persisted
        obj.unbind_id()

        return result[0]

    def _delete_edge_by_criteria(self, criteria: MatchCriteria):
        cypher_cmd = CypherBuilder.delete_edge_by_criteria(criteria)

        return self._parse_and_execute(cypher_cmd)

    def _fetch_edge_by_criteria(self, criteria: MatchCriteria, unnest_result: str | bool, fetch_one: bool):
        cypher_cmd = CypherBuilder.fetch_edge_by_criteria(criteria)

        result = self._parse_and_execute(cypher_cmd, fetch_one)

        return utils.unnest_result(result, unnest_result)

    def _create_or_merge_graph(self, obj, strategy: str) -> int:
        for node in obj.nodes:
            self._create_or_merge_node(node, strategy)

        for edge in obj.edges:
            self._create_or_merge_edge(edge, strategy)

        return obj

    def _parse_cypher(self, cmd: str) -> ParsedCypherQuery:
        return self._backend.parse_cypher(cmd)

    def _parse_and_execute(
        self,
        cypher_cmd: str | ParsedCypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> TabularResult | None:
        if isinstance(cypher_cmd, str):
            if not (parsed_query := self._parse_cypher(cypher_cmd)):
                return None
        else:
            assert isinstance(cypher_cmd, ParsedCypherQuery)
            parsed_query = cypher_cmd

        if not self._before_execute(parsed_query):
            logger.debug("Cancelled execution due failure of before_execute hook!")
            return None

        result, exec_stats = self._backend.execute_cypher(parsed_query, fetch_one=fetch_one, raw_data=raw_data, params=params)

        self._after_execute(result, parsed_query)

        return result

    def _before_execute(self, parsed_query: ParsedCypherQuery):
        result = self.on_before_execute(parsed_query) if isinstance(self.on_before_execute, Callable) else True

        return result

    def _after_execute(self, result: list[tuple[GraphObject]], parsed_query: ParsedCypherQuery):
        if isinstance(self.on_after_execute, Callable):
            self.on_after_execute(result, parsed_query)

    def _resolve_obj_id(self, obj: GraphObject, fetch_func: Callable) -> bool:
        if obj.has_id:
            return True

        if obj.has_gid:
            criteria = MatchCriteria(
                prefix_="n",
                properties_={config.PROP_GID: obj.properties_[config.PROP_GID]},
                projection_=["id(n)"],
            )

            if id_ := fetch_func(criteria, True, True):
                obj.id_ = id_
                return True

        # For edges, look up by pattern (start_id + end_id + edge_type)
        # Note: This adds an extra DB query - only triggered when edge has no ID but has start/end node IDs
        if isinstance(obj, GraphEdge) and hasattr(obj, "start_id_") and hasattr(obj, "end_id_"):
            criteria = MatchEdgeCriteria(
                prefix_="r",
                label_=obj.label_,
                start_criteria_=MatchNodeCriteria(id_=obj.start_id_),
                end_criteria_=MatchNodeCriteria(id_=obj.end_id_),
                projection_=["id(r)"],
            )

            # fetch_func(criteria, unnest_result, fetch_one)
            # - unnest_result=True: returns scalar (1 match), list (>1), or None (0)
            # - fetch_one=False: fetch all to detect multiple edges of same type between same nodes
            result = fetch_func(criteria, True, False)
            if result is not None:
                if not isinstance(result, list):
                    # Exactly one match - safe to resolve
                    obj.id_ = result
                    return True
                else:
                    # Multiple matches - ambiguous, cannot resolve to single edge
                    logger.warning(
                        f"Found {len(result)} edges of type '{obj.label_}' between "
                        f"nodes {obj.start_id_} and {obj.end_id_}. Cannot resolve unique edge ID."
                    )
            # If None (0 matches) or list (>1 matches), don't resolve - not found or ambiguous

        return False
