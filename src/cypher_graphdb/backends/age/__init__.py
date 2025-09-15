"""AGE (Apache AGE) backend implementation for CypherGraphDB.

This package provides the Apache AGE backend implementation, enabling
CypherGraphDB to work with PostgreSQL databases using the AGE extension
for graph operations and Cypher queries.
"""

# Import and register the backend
from cypher_graphdb.backendprovider import backend_provider

from .agegraphdb import AGEGraphDB

# Register AGE backend
backend_provider.register("AGE", AGEGraphDB)
