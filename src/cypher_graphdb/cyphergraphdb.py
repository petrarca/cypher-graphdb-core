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

    # Node model with metadata and relation definition
    @node(metadata={"category": "software"})
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

import contextlib
from collections.abc import Callable
from typing import Any

from loguru import logger
from pydantic import BaseModel

from . import config, modelprovider, utils
from . import graphops as gops
from .backend import CypherBackend, ExecStatistics, SqlStatistics
from .backendprovider import backend_provider
from .cypherbuilder import CypherBuilder
from .cypherparser import ParsedCypherQuery
from .models import Graph, GraphEdge, GraphNode, GraphObject, TabularResult
from .statistics import LabelStatistics


class MatchCriteria(BaseModel):
    """Base criteria for matching nodes or edges in the graph database.

    Defines id, labels, and property filters for queries.
    """

    id_: int | None = None
    label_: str | type[GraphNode | GraphEdge] | list[str | type[GraphNode | GraphEdge]] | None = None
    properties_: dict[str, Any] | None = None
    prefix_: str | None = None
    projection_: list[str] | None = None

    @property
    def has_id(self):
        return self.id_ is not None

    @property
    def has_gid(self):
        return self.properties_.get(config.PROP_GID, False) if isinstance(self.properties_, dict) else False

    @property
    def has_unique_ids(self):
        return self.has_id or self.has_gid

    @property
    def has_properties(self):
        return self.properties_

    @property
    def has_labels(self):
        return isinstance(self.label_, list | tuple)

    @property
    def has_projection(self):
        return self.projection_

    def get_prefix(self, default_prefix=None):
        return self.prefix_ if self.prefix_ is not None else default_prefix

    def resolve(self):
        if isinstance(self.properties_, dict):
            if "id_" in self.properties_:
                self.id_ = self.properties_.get("id_") if self.id_ is None else self.id_
                self.properties_.pop("id_")

            if "label_" in self.properties_:
                self.label_ = self.properties_.get("label_") if self.label_ is None else self.label_
                self.properties_.pop("label_")

        self._resolve_label()

    def _resolve_label(self) -> str:
        def label_to_literal(lbl) -> str:
            if isinstance(lbl, str):
                return lbl

            if issubclass(lbl, GraphNode | GraphEdge):
                return lbl.graph_info_.label_

            raise RuntimeError("Label must either be 'str|GraphNode|GraphEdge'")

        if self.label_ is None:
            return None

        if isinstance(self.label_, list):
            self.label_ = [label_to_literal(lbl) for lbl in self.label_]
        else:
            self.label_ = label_to_literal(self.label_)


class MatchNodeCriteria(MatchCriteria):
    """Criteria for matching graph nodes (inherits MatchCriteria)."""

    pass


class MatchNodeById(MatchNodeCriteria):
    """Criteria for matching a node by its numeric ID."""

    def __init__(self, id):
        """Initialize with the given node ID."""
        self.id_ = id


class MatchEdgeCriteria(MatchCriteria):
    """Criteria for matching graph edges, including optional start/end filters and fetch behavior."""

    start_criteria_: MatchCriteria = None
    end_criteria_: MatchCriteria = None
    fetch_nodes_: bool = False

    def resolve(self):
        super().resolve()
        if self.start_criteria_ is not None:
            assert isinstance(self.start_criteria_, MatchCriteria)
            self.start_criteria_.resolve()

        if self.end_criteria_ is not None:
            assert isinstance(self.end_criteria_, MatchCriteria)
            self.end_criteria_.resolve()


class MatchEdgeById(MatchEdgeCriteria):
    """Criteria for matching an edge by its numeric ID."""

    def __init__(self, id):
        """Initialize with the given edge ID."""
        self.id_ = id


class CypherGraphDB:
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

        self._exec_statistics = ExecStatistics()
        self._sql_statistics = SqlStatistics()
        self._last_parsed_query = None

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
            String identifier combining backend ID and graph name.
        """
        if self._backend:
            if self._backend.graph_name:
                return f"{self._backend.id}:{self._backend.graph_name}"
            else:
                return self._backend.id
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
    def last_parsed_query(self) -> ParsedCypherQuery:
        """Get the most recently parsed Cypher query.

        Returns:
            ParsedCypherQuery object containing query analysis.
        """
        return self._last_parsed_query

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
    def get_settings():
        """Get the singleton settings instance.

        Returns:
            Settings: The cached settings instance with environment variables.
        """
        from .settings import get_settings

        return get_settings()

    # Property for convenience access
    settings = property(lambda self: self.get_settings())

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

    def connect(self, connect_url: str | None = None, *args, **kwargs):
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

            # Add settings as defaults if not already provided
            if "cinfo" not in kwargs and self.settings.cinfo:
                merged_kwargs["cinfo"] = self.settings.cinfo
            if "graph_name" not in kwargs and self.settings.graph:
                merged_kwargs["graph_name"] = self.settings.graph

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

    def fetch(
        self,
        criteria: MatchCriteria,
        unnest_result: str | bool = None,
        fetch_one=None,
    ):
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
        fetch_one=None,
    ):
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
        fetch_one=None,
    ):
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
        strategy=config.CREATE_OR_MERGE_STRAGEY[0],
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
        """
        assert self._backend

        assert strategy in config.CREATE_OR_MERGE_STRAGEY, f"Invalid strategy {strategy}!"

        if isinstance(obj, GraphNode):
            return self._create_or_merge_node(obj, strategy)
        if isinstance(obj, GraphEdge):
            return self._create_or_merge_edge(obj, strategy)
        if isinstance(obj, Graph):
            return self._create_or_merge_graph(obj, strategy)

        raise RuntimeError(f"Unsupported objectobject type to merge/create: {type(obj)} with strategy {strategy}")

    def delete(self, obj: GraphNode | GraphEdge | MatchCriteria, detach=False):
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
        """
        assert self._backend

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
        fetch_one=False,
        raw_data=False,
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

        logger.debug(f"Execute cypher {unnest_result=}, {fetch_one=}): \n{cypher_cmd}")
        result = self._parse_and_execute(cypher_cmd, fetch_one, raw_data)

        return utils.unnest_result(result, unnest_result)

    def execute_sql(
        self,
        sql_str: str,
        unnest_result: str | bool = None,
        fetch_one=False,
        raw_data=False,
    ) -> Any | TabularResult:
        """Execute a raw SQL command and return results, with optional unnesting."""
        assert self._backend

        logger.debug(f"Execute SQL {unnest_result=}, {fetch_one=}): \n{sql_str}")
        result = self._execute_sql(sql_str, fetch_one, raw_data)

        return utils.unnest_result(result, unnest_result)

    def search(
        self, parsed_query: ParsedCypherQuery, fts_query: str, language: str = None, unnest_result: str | bool = None
    ) -> Any | TabularResult:
        """Perform full-text search on graph data using backend-specific search capabilities.

        Executes full-text search queries against indexed graph content. The search
        behavior depends on the backend's search implementation and indexing configuration.

        Args:
            parsed_query: Parsed Cypher query providing search context and filters
            fts_query: Full-text search query string with search terms
            language: Optional language hint for search optimization (e.g., "english")
            unnest_result: Result formatting (same as execute method)

        Returns:
            Search results formatted according to unnest_result parameter

        Examples:
            ```python
            # Using context manager (recommended)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Basic text search in product descriptions
                context_query = cdb.parse("MATCH (p:Product) RETURN p")
            results = cdb.search(
                context_query,
                "graph database client",
                language="english"
            )

            # Search with specific language
            spanish_results = cdb.search(
                context_query,
                "base de datos grafo",
                language="spanish",
                unnest_result="c"  # Just the matching content
            )

            # Technology search with filters
            tech_query = cdb.parse('''
                MATCH (t:Technology)
                WHERE t.category = "Database"
                RETURN t
            ''')
            database_techs = cdb.search(
                tech_query,
                "graph database nosql",
                unnest_result=True
            )
            ```

        Note:
            Full-text search requires proper indexing configuration on the
            backend. Search capabilities and syntax vary by database backend
            (Neo4j, Memgraph, etc.).
        """
        assert self._backend
        logger.debug(f"Search fts_query={fts_query} unnest_result={unnest_result}\ncypher_query={parsed_query.submitted_query}")

        result, self._exec_statistics = self._backend.fulltext_search(
            parsed_query,
            fts_query,
            language,
        )

        return utils.unnest_result(result, unnest_result)

    def exec_statistics(self) -> ExecStatistics:
        """Get execution statistics from the most recent database operation.

        Provides detailed metrics about the last executed query including
        timing, resource usage, and result statistics. Useful for performance
        monitoring
            and query optimization.

            Returns:
                ExecStatistics object with comprehensive operation metrics

            Examples:
                ```python
                # Using context manager (recommended)
                with CypherGraphDB() as cdb:
                    cdb.connect()

                    # Execute a query
                    result = cdb.execute("MATCH (n:Product) RETURN count(n)")

                    # Get execution statistics
                    stats = cdb.exec_statistics()
                    print(f"Execution time: {stats.execution_time_ms}ms")
                    print(f"Nodes examined: {stats.nodes_examined}")
                    print(
                        "Relationships examined: "
                        f"{stats.relationships_examined}"
                    )
                    print(f"Records produced: {stats.records_produced}")
                    print(f"Memory usage: {stats.memory_used_bytes} bytes")

                    # Monitor complex query performance
                    complex_result = cdb.execute('''
                        MATCH (p:Product)-[:USES_TECHNOLOGY*1..2]->(t:Technology)
                        WHERE t.name = "Python"
                        RETURN p.name, t.name
                        ORDER BY p.name
                        LIMIT 100
                    ''')

                    perf_stats = cdb.exec_statistics()
                    if perf_stats.execution_time_ms > 1000:  # Slow query
                        print(f"Query took {perf_stats.execution_time_ms}ms")
                        print(f"Consider adding indexes or optimizing")
                ```
        """
        return self._exec_statistics

    def sql_statistics(self) -> SqlStatistics:
        """Get SQL statistics from the last operation.

        Returns:
            SqlStatistics object with SQL execution metrics.
        """
        return self._sql_statistics

    def graphs(self) -> tuple[str]:
        """Get list of available graphs in the database backend.

        Returns all graph databases available on the connected backend
        instance.
            Useful for multi-tenant applications or database exploration.

            Returns:
                Sorted tuple of graph database names

            Examples:
                ```python
                db = CypherGraphDB("memgraph").connect()

                # List all available graphs
                available_graphs = db.graphs()
                print(f"Available graphs: {available_graphs}")
                # Output: ('main', 'analytics', 'test_graph')

                # Switch to a different graph if backend supports it
                if "analytics" in available_graphs:
                    # Reconnect to different graph
                    db.disconnect()
                    db.backend.graph_name = "analytics"
                    db.connect()
                ```
        """
        assert self._backend
        result = self._backend.graphs()

        return sorted(result)

    def labels(self) -> list[LabelStatistics]:
        """Get statistics for all labels (node types and relationship types)
        in the current graph.

        Provides comprehensive metadata about the graph schema including node
        and edge label usage statistics. Useful for schema discovery and data
        profiling.

        Returns:
            List of LabelStatistics objects sorted by type (nodes first) then
            by name

        Examples:
            ```python
            # Using context manager (recommended)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Get all label statistics
                label_stats = cdb.labels()

                for label_stat in label_stats:
                    print(f"Label: {label_stat.label_}")
                    print(f"Type: {label_stat.type_}")  # node or relationship
                    print(f"Count: {label_stat.count_}")
                    print(f"Properties: {label_stat.property_names}")
                    print("---")

                # Filter for just node labels
                node_labels = [
                    stat for stat in label_stats
                    if stat.type_.value == "node"
                ]
                print(f"Node types: {[stat.label_ for stat in node_labels]}")

                # Filter for specific node types
                product_tech_labels = [
                    stat for stat in label_stats
                    if stat.label_ in ["Product", "Technology"]
                ]
                for stat in product_tech_labels:
                    print(f"{stat.label_}: {stat.count_} instances")

            # Find the most common relationship type
            rel_labels = [
                stat for stat in label_stats
                if stat.type_.value == "relationship"
            ]
            if rel_labels:
                most_common_rel = max(rel_labels, key=lambda x: x.count_)
                print(
                    "Most common relationship: "
                    f"{most_common_rel.label_} with {most_common_rel.count_} instances"
                )

            # Get schema overview for Product and Technology
            schema = {}
            for stat in label_stats:
                if stat.label_ in ["Product", "Technology", "USES_TECHNOLOGY"]:
                    schema[stat.label_] = {
                        'type': stat.type_.value,
                        'count': stat.count_,
                        'properties': stat.property_names or []
                    }
            print(f"Schema subset: {schema}")
            ```
        """
        assert self._backend

        result = self._backend.labels()

        # sort by type and then by label name
        return sorted(result, key=lambda x: (x.type_.value, x.label_))

    def nest_result(self, result: Any) -> Any:
        """Convert query results to consistent nested tuple format for
        internal processing.

        Normalizes different result formats into a consistent list-of-tuples
        structure used internally by the library. Primarily used by result
        processing utilities.

        Args:
            result: Query result in any format (single value, tuple, list,
                etc.)

        Returns:
            Normalized result as list of tuples, or None if input is None

        Examples:
            ```python
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Single value -> [(value,)]
                nested = cdb.nest_result(42)
                # Returns: [(42,)]

                # Tuple -> [tuple]
                nested = cdb.nest_result(("Alice", 30))
                # Returns: [("Alice", 30)]

                # List is returned as-is
                nested = cdb.nest_result([("Alice", 30), ("Bob", 25)])
                # Returns: [("Alice", 30), ("Bob", 25)]

                # None -> None
                nested = cdb.nest_result(None)
                # Returns: None
            ```

        Note:
            This method is primarily for internal use by result processing
            utilities. Most users should use the unnest_result parameter in
            execute/fetch methods instead.
        """
        if result is None:
            return None

        if isinstance(result, list):
            return result
        if isinstance(result, tuple):
            return [result]

        return [(result,)]

    def resolve_edges(self, graph: Graph) -> set[int] | None:
        if graph is None:
            return None

        if not (missing_nodes := gops.missing_nodes(graph)):
            return missing_nodes

        for batch in utils.chunk_list(list(missing_nodes), 50):
            graph.merge(self._parse_and_execute(CypherBuilder.fetch_nodes_by_ids(batch)))

        return missing_nodes

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
        obj.bind_id(result[0])

        return obj

    def _merge_node(self, obj) -> GraphNode:
        cypher_cmd = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # merge results back from the retrieved one, could be modified in the meantime
        obj.__dict__.update(result[0])

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

        obj.bind_id(result[0])

        return obj

    def _merge_edge(self, obj) -> GraphEdge:
        cypher_cmd = CypherBuilder.merge_edge_by_id(obj.id_, obj.flatten_properties())

        result = self._parse_and_execute(cypher_cmd, True)

        if result is None:
            return -1

        # merge results back from the retrieved one, could be modified in the meantime
        obj.__dict__.update(result[0])

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
        self._last_parsed_query = self._backend.parse_cypher(cmd)
        return self._last_parsed_query

    def _parse_and_execute(
        self,
        cypher_cmd: str | ParsedCypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
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

        self._last_parsed_query = parsed_query

        result, self._exec_statistics = self._backend.execute_cypher(parsed_query, fetch_one=fetch_one, raw_data=raw_data)

        self._after_execute(result, parsed_query)

        return result

    def _before_execute(self, parsed_query: ParsedCypherQuery):
        result = self.on_before_execute(parsed_query) if isinstance(self.on_before_execute, Callable) else True

        return result

    def _after_execute(self, result: list[tuple[GraphObject]], parsed_query: ParsedCypherQuery):
        if isinstance(self.on_after_execute, Callable):
            self.on_after_execute(result, parsed_query)

    def _execute_sql(self, sql_str: str, fetch_one: bool, raw_data: bool) -> TabularResult:
        result, self._exec_statistics, self._sql_statistics = self._backend.execute_sql(sql_str, fetch_one, raw_data)

        return result

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

        return False
