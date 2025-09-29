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
    MATCH (p:Product)-[b:BELONGS_TO]->(c:Category)
    RETURN p,b,c,id(p),label(b)
    """
)

pprint(result)
pprint(cdb.exec_statistics())
