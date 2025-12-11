"""CypherGraphDB subpackage - Core client for graph database operations."""

from .criteria import MatchCriteria, MatchEdgeById, MatchEdgeCriteria, MatchNodeById, MatchNodeCriteria
from .cyphergraphdb import CypherGraphDB
from .result import QueryResult

__all__ = [
    "CypherGraphDB",
    "MatchCriteria",
    "MatchEdgeById",
    "MatchEdgeCriteria",
    "MatchNodeById",
    "MatchNodeCriteria",
    "QueryResult",
]
