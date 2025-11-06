import os
import sys

from loguru import logger

import cypher_graphdb
from cypher_graphdb import CypherGraphDB
from cypher_graphdb.tools.excel_importer import ExcelImporter

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB().connect()

# if (
#    input(f"{db.backend.graph_name} will be recreated. Are you sure [yN]? ").lower()
#    != "y"
# ):
#    exit(0)

if cdb._backend.graph_exists():
    cdb._backend.drop_graph()
cdb._backend.create_graph()

INPUT_FILE_NAME = "_data.xlsx"
# INPUT_FILE_NAME = "_output.xlsx"

path = os.path.basename(os.getcwd())
path = "." if path == "examples" else "./examples"
filename = f"{path}/{INPUT_FILE_NAME}"
# filename = f"{path}/data"


def on_import_file(filename, partname):
    print(f"{filename} {partname}")


# Import from excel
importer = ExcelImporter(cdb)
importer.on_import_file = on_import_file
importer.load(filename)
