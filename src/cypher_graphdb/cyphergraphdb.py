"""cyphergraphdb module: Core client for connecting to and querying a Cypher-based graph database.

Provides CypherGraphDB for managing connections, executing queries, and CRUD operations on graph data.
"""

import contextlib
from collections.abc import Callable
from typing import Any

import dotenv
from loguru import logger
from pydantic import BaseModel

from . import config, modelprovider, utils
from . import graphops as gops
from .backend import CypherBackend, ExecStatistics, SqlStatistics
from .backendprovider import backend_provider
from .cypherbuilder import CypherBuilder
from .cypherparser import ParsedCypherQuery
from .models import Graph, GraphEdge, GraphNode, GraphObject
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
    """Manage connection and operations against a Cypher graph database backend.

    Provides methods to connect, fetch, create, merge, delete, and execute queries.
    """

    def __init__(self, backend: CypherBackend | str, load_dotenv: bool = False, connect_params: dict | None = None):
        backend = backend_provider.check_and_resolve(backend, True)
        assert backend

        self._backend = backend
        self._model_provider = modelprovider.model_provider
        # inject into the backend
        self._backend.model_provider = self._model_provider

        self._exec_statistics = ExecStatistics()
        self._sql_statistics = SqlStatistics()
        self._last_parsed_query = None

        if load_dotenv:
            logger.debug("Load dotenv")
            dotenv.load_dotenv()

        # will be called before executing the query for e.g. security checks
        self.on_before_execute: Callable = lambda parsed_query: True
        # will be called after executing the query
        self.on_after_execute: Callable = lambda result, parsed_query: None

        utils.log_env(config.CGDB_BACKEND)
        utils.log_env(config.CGDB_CINFO)
        utils.log_env(config.CGDB_GRAPH)

        # Auto-connect if connection parameters are provided
        self._auto_connect_if_params(connect_params)

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

    def __enter__(self):
        """Enter the context manager - returns self without implicit connection."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager - disconnect if connected."""
        with contextlib.suppress(Exception):
            self.disconnect()
        return False  # Don't suppress exceptions

    def connect(self, *args, **kwargs):
        """Establish a connection to the configured graph database."""
        assert self._backend
        self._backend.connect(*args, **kwargs)

        return self

    def disconnect(self):
        """Close the connection to the graph database."""
        assert self._backend
        self._backend.disconnect()

    def commit(self):
        """Commit pending transactions to the graph database."""
        assert self._backend
        self._backend.commit()

    def rollback(self):
        """Rollback pending transactions, discarding changes."""
        assert self._backend
        self._backend.rollback()

    def fetch(
        self,
        criteria: MatchCriteria,
        unnest_result: str | bool = None,
        fetch_one=None,
    ):
        """Dispatch fetch to nodes or edges based on the criteria type."""
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
        """Fetch GraphNode instances matching the given criteria."""
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
        """Fetch GraphEdge instances matching the given criteria."""
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
        """Create or merge the given graph object(s) into the database."""
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
        """Delete graph entities matching the given object or criteria."""
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
        """Parse a Cypher command into an internal query representation."""
        return self._parse_cypher(cypher_cmd)

    def execute(
        self,
        cypher_cmd: str | ParsedCypherQuery,
        unnest_result: str | bool = None,
        fetch_one=False,
        raw_data=False,
    ) -> Any | list[tuple[GraphObject, ...]]:
        """Execute a Cypher command and return results, with optional unnesting."""
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
    ) -> Any | list[tuple[GraphObject, ...]]:
        """Execute a raw SQL command and return results, with optional unnesting."""
        assert self._backend

        logger.debug(f"Execute SQL {unnest_result=}, {fetch_one=}): \n{sql_str}")
        result = self._execute_sql(sql_str, fetch_one, raw_data)

        return utils.unnest_result(result, unnest_result)

    def search(
        self, parsed_query: ParsedCypherQuery, fts_query: str, language: str = None, unnest_result: str | bool = None
    ) -> Any | list[tuple[GraphObject, ...]]:
        """Perform full-text search on graph data.

        Args:
            parsed_query: Parsed Cypher query for search context.
            fts_query: Full-text search query string.
            language: Language for search (optional).
            unnest_result: How to format results (optional).

        Returns:
            Search results in specified format.
        """
        assert self._backend
        logger.debug(f"Search {fts_query=} {unnest_result=} \ncypher_query={parsed_query.submitted_query}")

        result, self._exec_statistics = self._backend.fulltext_search(parsed_query, fts_query, language)

        return utils.unnest_result(result, unnest_result)

    def exec_statistics(self) -> ExecStatistics:
        """Get execution statistics from the last operation.

        Returns:
            ExecStatistics object with operation metrics.
        """
        return self._exec_statistics

    def sql_statistics(self) -> SqlStatistics:
        """Get SQL statistics from the last operation.

        Returns:
            SqlStatistics object with SQL execution metrics.
        """
        return self._sql_statistics

    def graphs(self) -> tuple[str]:
        """Get list of available graphs in the database.

        Returns:
            Sorted tuple of graph names.
        """
        assert self._backend
        result = self._backend.graphs()

        return sorted(result)

    def labels(self) -> list[LabelStatistics]:
        """Get statistics for all labels in the current graph.

        Returns:
            List of LabelStatistics objects sorted by type and name.
        """
        assert self._backend

        result = self._backend.labels()

        # sort by type and then by label name
        return sorted(result, key=lambda x: (x.type_.value, x.label_))

    def nest_result(self, result: Any) -> Any:
        """Nest query results into tuples for consistent return format.

        Args:
            result: Query result to format.

        Returns:
            Nested result in tuple format.
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

    def _auto_connect_if_params(self, connect_params: dict | None):
        """Auto-connect if connection parameters are provided."""
        if connect_params is not None:
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
    ) -> list[tuple[GraphObject, ...]] | None:
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

    def _execute_sql(self, sql_str: str, fetch_one: bool, raw_data: bool) -> list[tuple[GraphObject, ...]]:
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
