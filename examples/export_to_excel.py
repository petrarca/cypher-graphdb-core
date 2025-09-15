import os
import sys

from loguru import logger

import cypher_graphdb
from cypher_graphdb import CypherGraphDB
from cypher_graphdb.tools.excel_exporter import ExcelExporter

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB(backend="AGE", load_dotenv=True).connect()

# Load all nodes in the graph
ALL_NODES_AND_EDGES = """
    MATCH (a)
    MATCH (b)
    MATCH (a)-[e]->(b)
    RETURN a, b, e
    """

path = os.path.basename(os.getcwd())
path = "." if path == "examples" else "./examples"
# file_name = f"{path}/_output.xlsx"
filename = f"{path}/data/"

exporter = ExcelExporter(cdb)
exporter.export(ALL_NODES_AND_EDGES, filename)
