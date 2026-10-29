"""Bolt: the backends that speak it (Memgraph, Neo4j) and the transports they speak it through.

``backend.BoltBackend`` holds what the Bolt backends share -- running statements, pages, streams,
bulk writes as UNWIND batches, label statistics; each backend adds its dialect (index DDL, catalog
queries, ids, capabilities). A transport carries the statements: pymgclient (Memgraph's default)
or the Neo4j Python driver (Neo4j's; Memgraph's on request), chosen by the connection
(``bolt://host:7687?transport=neo4j`` or ``connect_params={"transport": ...}``). Each
transport turns its own values into cypher-graphdb objects in one pass.
"""

from .transport import BoltTransport, Rows, open_transport

__all__ = ["BoltTransport", "Rows", "open_transport"]
