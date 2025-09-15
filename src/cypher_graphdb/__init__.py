"""CypherGraphDB: A powerful Python library for graph database operations.

This package provides a high-level interface for working with graph databases
using Cypher query language, with support for multiple backends including
Apache AGE (Age Graph Extension) for PostgreSQL.
"""

from .backendprovider import backend_provider
from .cyphergraphdb import (
    CypherGraphDB,
    MatchCriteria,
    MatchEdgeById,
    MatchEdgeCriteria,
    MatchNodeById,
    MatchNodeCriteria,
)
from .cypherjson import GraphJSONEncoder
from .dbpool import CypherGraphDBPool
from .decorators import edge, node, relation
from .modelinfo import GraphEdgeInfo, GraphNodeInfo
from .modelprovider import ModelProvider, model_provider
from .models import Graph, GraphEdge, GraphNode, GraphObject, GraphPath
from .statistics import GraphStatistics

__all__ = [
    "Graph",
    "GraphNode",
    "GraphEdge",
    "GraphPath",
    "GraphObject",
    "GraphJSONEncoder",
    "ModelProvider",
    "model_provider",
    "GraphNodeInfo",
    "GraphEdgeInfo",
    "GraphStatistics",
    "CypherGraphDB",
    "MatchCriteria",
    "MatchEdgeCriteria",
    "MatchEdgeById",
    "MatchNodeCriteria",
    "MatchNodeById",
    "CypherGraphDBPool",
    "backend",
    "backend_provider",
    "node",
    "edge",
    "relation",
    "graphops",
    "backends",
]

# logger.disable(__name__)

# Apply decorators to base classes - not needed anymore
#
# node("GraphNode")(GraphNode)
# edge("GraphEdge")(GraphEdge)
