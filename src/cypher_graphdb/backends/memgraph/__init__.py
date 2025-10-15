"""Memgraph backend implementation for CypherGraphDB.

This package provides the Memgraph backend implementation, enabling
CypherGraphDB to work with Memgraph databases for native graph operations
and Cypher queries.
"""

# Import and register the backend
from cypher_graphdb.backendprovider import backend_provider

from .memgraphdb import MemgraphDB

# Register Memgraph backend - use the backend's name attribute
backend_provider.register(MemgraphDB.name, MemgraphDB)

__all__ = ["MemgraphDB"]
