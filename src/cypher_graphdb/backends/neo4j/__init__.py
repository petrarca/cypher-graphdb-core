"""Neo4j backend for CypherGraphDB: the Neo4j dialect of the Bolt backends."""

from cypher_graphdb.backendprovider import backend_provider

from .neo4jdb import Neo4jDB

backend_provider.register(Neo4jDB.name, Neo4jDB)

__all__ = ["Neo4jDB"]
