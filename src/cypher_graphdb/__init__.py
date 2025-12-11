"""CypherGraphDB: A powerful Python library for graph database operations.

This package provides a high-level interface for working with graph databases
using Cypher query language, with support for multiple backends including
Apache AGE (Age Graph Extension) for PostgreSQL.
"""

from .backendprovider import backend_provider
from .cardinality import Cardinality
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
from .dbpool import CypherGraphDBPool
from .decorators import edge, node, relation
from .exceptions import ReadOnlyModeError
from .modelinfo import GraphEdgeInfo, GraphNodeInfo
from .modelprovider import ModelProvider, model_provider
from .models import Graph, GraphEdge, GraphNode, GraphObject, GraphPath, TabularResult
from .settings import Settings, get_settings
from .statistics import GraphStatistics

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
    "GraphNodeInfo",
    "GraphEdgeInfo",
    "GraphStatistics",
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
    "backend",
    "backend_provider",
    "node",
    "edge",
    "relation",
    "Cardinality",
    "graphops",
    "backends",
]

# logger.disable(__name__)

# Apply decorators to base classes - not needed anymore
#
# node("GraphNode")(GraphNode)
# edge("GraphEdge")(GraphEdge)
