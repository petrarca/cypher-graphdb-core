"""Neo4j: the Neo4j dialect of the Bolt backends.

The graph name selects the database (``neo4j`` by default); indexes are named range indexes
(``CREATE INDEX <label>_<props> FOR (n:Label) ON (n.p, ...)``), listed by ``SHOW INDEXES``; ids are
the integer part of an element id (``4:<db>:12``). Speaks Bolt through the Neo4j driver; the
connection's default timeout and a statement's own are sent as the Bolt transaction timeout.

Example:
    >>> db = CypherGraphDB(backend="neo4j", connect_url="neo4j://user:pass@localhost:7687/neo4j")
"""

from __future__ import annotations

from loguru import logger

from cypher_graphdb.backends.bolt.backend import BoltBackend
from cypher_graphdb.statistics import IndexInfo, IndexType


class Neo4jDB(BoltBackend):
    """The Neo4j dialect."""

    name = "NEO4J"
    transports = ("neo4j",)
    default_graph = "neo4j"
    selects_database = True

    def graphs(self) -> list[str]:
        return [name for (name,) in self._run_implicit("SHOW DATABASES YIELD name") if name != "system"]

    def create_graph(self, graph_name: str | None = None) -> None:
        self._run_implicit(f"CREATE DATABASE `{graph_name or self._graph_name}` IF NOT EXISTS WAIT")

    def drop_graph(self, graph_name: str | None = None) -> None:
        self._run_implicit(f"DROP DATABASE `{graph_name or self._graph_name}` IF EXISTS WAIT")

    def graph_exists(self, graph_name: str | None = None) -> bool:
        return (graph_name or self._graph_name) in self.graphs()

    def create_property_index(self, label: str, *property_names: str) -> None:
        """A named range index over the properties; a label without properties is already indexed (token lookup)."""
        if not property_names:
            return
        props = ", ".join(f"n.{p}" for p in property_names)
        self._run_implicit(f"CREATE INDEX {_index_name(label, property_names)} IF NOT EXISTS FOR (n:{label}) ON ({props})")
        logger.debug("Property index created for label '{}' in Neo4j", label)

    def drop_index(self, label: str, *property_names: str) -> None:
        if property_names:
            self._run_implicit(f"DROP INDEX {_index_name(label, property_names)} IF EXISTS")

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:
        indexes = []
        for name, index_type, entity_type, labels, props, owning in self._run_implicit(
            "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint"
        ):
            if not include_internal and (index_type == "LOOKUP" or entity_type != "NODE"):
                continue
            indexes.append(
                IndexInfo(
                    label=(labels or [""])[0],
                    property_names=list(props) if props else None,
                    index_type=_INDEX_TYPES.get(index_type, IndexType.UNIQUE if owning else IndexType.PROPERTY),
                    index_name=name,
                )
            )
        return indexes


_INDEX_TYPES = {"FULLTEXT": IndexType.FULLTEXT, "VECTOR": IndexType.VECTOR}


def _index_name(label: str, property_names: tuple[str, ...]) -> str:
    return f"{label}_{'_'.join(property_names)}"
