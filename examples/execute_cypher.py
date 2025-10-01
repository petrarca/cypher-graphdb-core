import sys
from pprint import pprint

from loguru import logger

import cypher_graphdb
from cypher_graphdb import CypherGraphDB

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB().connect()

result = cdb.execute(
    """
    MATCH (p:Product)-[r:USES_TECHNOLOGY]->(t:Technology)
    RETURN p, r, t, id(p) AS product_id, r.version AS tech_version
    LIMIT 5
    """
)

pprint(result)
pprint(cdb.exec_statistics())
