"""CypherGraphDB: A powerful Python library for graph database operations.

This package provides a high-level interface for working with graph databases
using Cypher query language, with support for multiple backends including
Apache AGE (Age Graph Extension) for PostgreSQL.
"""

from .backend import BackendCapability
from .backendprovider import backend_provider
from .cardinality import Cardinality
from .connection_guard import (
    ConnectionGuardError,
    clear_connection_guard,
    install_connection_guard,
)
from .cyphergraphdb import (
    CypherGraphDB,
    MatchCriteria,
    MatchEdgeById,
    MatchEdgeCriteria,
    MatchNodeById,
    MatchNodeCriteria,
    QueryResult,
)
from .cyphergraphdb.expand import DEFAULT_EXPAND_LIMIT, ExpandDirection
from .cyphergraphdb.pagination import Page
from .cypherjson import GraphJSONEncoder
from .cypherparser import ParsedCypherQuery, parse_cypher_query
from .dbpool import CypherGraphDBPool
from .exceptions import LabelNotFoundError, QueryExecutionError, ReadOnlyModeError
from .formatprovider import FormatProvider, FormatSpec, data_format, format_provider
from .modelinfo import GraphEdgeInfo, GraphNodeInfo, GraphRelationInfo
from .modelprovider import ModelProvider, validate_node_properties
from .modelrelations import RelationSpec
from .models import ExpandResult, Graph, GraphEdge, GraphNode, GraphObject, GraphPath, TabularResult
from .settings import Settings, get_settings
from .statistics import GraphStatistics, IndexInfo, IndexType

__all__ = [
    "Graph",
    "GraphNode",
    "GraphEdge",
    "GraphPath",
    "GraphObject",
    "TabularResult",
    "GraphJSONEncoder",
    "ModelProvider",
    "RelationSpec",
    "validate_node_properties",
    "GraphNodeInfo",
    "GraphEdgeInfo",
    "GraphRelationInfo",
    "GraphStatistics",
    "IndexInfo",
    "IndexType",
    "CypherGraphDB",
    "QueryResult",
    "ExpandResult",
    "ExpandDirection",
    "DEFAULT_EXPAND_LIMIT",
    "Page",
    "MatchCriteria",
    "MatchEdgeCriteria",
    "MatchEdgeById",
    "MatchNodeCriteria",
    "MatchNodeById",
    "CypherGraphDBPool",
    "Settings",
    "get_settings",
    "ReadOnlyModeError",
    "LabelNotFoundError",
    "QueryExecutionError",
    "ConnectionGuardError",
    "install_connection_guard",
    "clear_connection_guard",
    "backend",
    "BackendCapability",
    "backend_provider",
    "FormatProvider",
    "FormatSpec",
    "data_format",
    "format_provider",
    "Cardinality",
    "graphops",
    "backends",
    "ParsedCypherQuery",
    "parse_cypher_query",
]

# logger.disable(__name__)
