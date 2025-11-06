import sys

from loguru import logger

from cypher_graphdb import CypherGraphDB
from cypher_graphdb.cli.renderer import ResultRenderer

logger.remove()
logger.add(sys.stderr, level="TRACE")


renderer = ResultRenderer()

with CypherGraphDB(backend="memgraph", connect_params={}) as db:
    result = db.execute("""
        MATCH (p) RETURN 'nodes' AS entity, count(p) AS cnt
        UNION ALL
        MATCH ()-[r]->() RETURN 'relationships' AS entity, count(r) AS cnt
    """)
    renderer.render(result)  # replaced show(result)

    result = db.execute("""
    MATCH (n)
    UNWIND labels(n) AS label
    RETURN 'node' AS entity_type, label AS name, count(*) AS cnt
    UNION ALL
    MATCH ()-[r]->()
    RETURN 'relationship' AS entity_type, type(r) AS name, count(r) AS cnt
    ORDER BY entity_type, name
    """)
    renderer.render(result)  # replaced show(result)
