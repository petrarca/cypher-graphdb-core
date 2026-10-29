"""Memgraph: the Memgraph dialect of the Bolt backends.

One database (``memgraph``); indexes one per property (``CREATE INDEX ON :Label(prop)``), listed by
``SHOW INDEX INFO``; DDL and settings in implicit transactions. Speaks Bolt through pymgclient
(default: faster on large reads) or the Neo4j driver (``?transport=neo4j``: per-transaction
timeouts, metadata, routing).

Example:
    >>> db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")
"""

from __future__ import annotations

from loguru import logger

from cypher_graphdb.backends.bolt.backend import BoltBackend
from cypher_graphdb.statistics import IndexInfo, IndexType


class MemgraphDB(BoltBackend):
    """The Memgraph dialect."""

    name = "MEMGRAPH"
    transports = ("pymgclient", "neo4j")
    default_graph = "memgraph"

    def _after_connect(self) -> None:
        """pymgclient cannot send a transaction timeout: the default goes to the server (``query.timeout``, server-wide)."""
        if self._query_timeout_s and self.transport == "pymgclient":
            timeout_ms = int(self._query_timeout_s * 1000)
            self._run_implicit(f"SET DATABASE SETTING 'query.timeout' TO '{timeout_ms}'")
            logger.debug("Memgraph query timeout set to {}ms (server-wide)", timeout_ms)

    def graphs(self) -> list[str]:
        return ["memgraph"]

    def create_graph(self, graph_name: str | None = None) -> None:
        raise NotImplementedError("Memgraph doesn't support multiple graphs, only the default 'memgraph' graph is available.")

    def drop_graph(self, graph_name: str | None = None) -> None:
        raise NotImplementedError("Memgraph doesn't support dropping graphs, only the default 'memgraph' graph is available.")

    def graph_exists(self, graph_name: str | None = None) -> bool:
        return graph_name == "memgraph"

    def create_property_index(self, label: str, *property_names: str) -> None:
        """One index per property; a label index without properties."""
        for prop in property_names or (None,):
            self._run_implicit(f"CREATE INDEX ON :{label}({prop})" if prop else f"CREATE INDEX ON :{label}")
        logger.debug("Property index(es) created for label '{}' in Memgraph", label)

    def drop_index(self, label: str, *property_names: str) -> None:
        for prop in property_names or (None,):
            self._run_implicit(f"DROP INDEX ON :{label}({prop})" if prop else f"DROP INDEX ON :{label}")
        logger.debug("Index(es) dropped for label '{}' in Memgraph", label)

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:  # noqa: ARG002 -- all of Memgraph's are user-created
        indexes = []
        # SHOW INDEX INFO: (index type, label, property list or None, count)
        for index_type, label, prop_list, _count in self._run_implicit("SHOW INDEX INFO"):
            props = list(prop_list) if prop_list else None
            indexes.append(
                IndexInfo(
                    label=label,
                    property_names=props,
                    index_type=IndexType.UNIQUE if "unique" in str(index_type).lower() else IndexType.PROPERTY,
                    index_name=f"{label}_{'_'.join(props)}" if props else label,
                )
            )
        return indexes
