import sys
from pprint import pprint

from loguru import logger

import cypher_graphdb
from cypher_graphdb import CypherGraphDB

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

# Connect to Memgraph
cdb = CypherGraphDB(backend="MEMGRAPH").connect(host="127.0.0.1", port=7687)

# Create some test data
cdb.execute("CREATE (p:Product {name: 'Test Product', price: 29.99})-[:BELONGS_TO]->(c:Category {name: 'Test Category'})")

# Query the data
result = cdb.execute(
    """
    MATCH (p:Product)-[b:BELONGS_TO]->(c:Category)
    RETURN p, b, c
    """
)

print("Query results:")
pprint(result)
print("\nExecution statistics:")
pprint(cdb.exec_statistics())

# Clean up test data
cdb.execute("MATCH (n) DETACH DELETE n")
