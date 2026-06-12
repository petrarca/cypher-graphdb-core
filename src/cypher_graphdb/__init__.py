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
from .cypherjson import GraphJSONEncoder
from .cypherparser import ParsedCypherQuery, parse_cypher_query
from .dbpool import CypherGraphDBPool
from .decorators import edge, extend_relation, extend_relations, node, relation
from .exceptions import LabelNotFoundError, ReadOnlyModeError
from .modelinfo import GraphEdgeInfo, GraphNodeInfo
from .modelprovider import ModelProvider, model_provider, validate_node_properties
from .models import Graph, GraphEdge, GraphNode, GraphObject, GraphPath, TabularResult
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
    "model_provider",
    "validate_node_properties",
    "GraphNodeInfo",
    "GraphEdgeInfo",
    "GraphStatistics",
    "IndexInfo",
    "IndexType",
    "CypherGraphDB",
    "QueryResult",
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
    "ConnectionGuardError",
    "install_connection_guard",
    "clear_connection_guard",
    "backend",
    "BackendCapability",
    "backend_provider",
    "node",
    "edge",
    "relation",
    "extend_relations",
    "extend_relation",
    "Cardinality",
    "graphops",
    "backends",
    "ParsedCypherQuery",
    "parse_cypher_query",
]

# logger.disable(__name__)

# Apply decorators to base classes - not needed anymore
#
# node("GraphNode")(GraphNode)
# edge("GraphEdge")(GraphEdge)
