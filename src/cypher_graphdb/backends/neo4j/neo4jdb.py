"""Neo4j: the Neo4j dialect of the Bolt backends.

The graph name selects the database (``neo4j`` by default); indexes are named range indexes, one per
property (``CREATE INDEX <label>_<prop> FOR (n:Label) ON (n.prop)``), listed by ``SHOW INDEXES``; ids are
the integer part of an element id (``4:<db>:12``). Speaks Bolt through the Neo4j driver; the
connection's default timeout and a statement's own are sent as the Bolt transaction timeout.

Example:
    >>> db = CypherGraphDB(backend="neo4j", connect_url="neo4j://user:pass@localhost:7687/neo4j")
"""

from __future__ import annotations

from loguru import logger

from cypher_graphdb.backends.bolt.backend import BoltBackend
from cypher_graphdb.statistics import GraphObjectType, IndexInfo, IndexType


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
        """One named range index per property; a label without properties is already indexed (token lookup).

        Not one composite index over all of them: Neo4j plans a label scan for a lookup on a single
        property of a composite index, so it would serve none of the lookups it is created for.
        """
        for prop in property_names:
            self._run_implicit(f"CREATE INDEX {_index_name(label, prop)} IF NOT EXISTS FOR (n:{label}) ON (n.{prop})")
        logger.debug("Property index(es) created for label '{}' in Neo4j", label)

    def drop_index(self, label: str, *property_names: str) -> None:
        for prop in property_names:
            self._run_implicit(f"DROP INDEX {_index_name(label, prop)} IF EXISTS")

    def list_indexes(self, include_internal: bool = False) -> list[IndexInfo]:
        """Node and relationship indexes; the token-lookup indexes Neo4j creates itself only with ``include_internal``."""
        indexes = []
        for name, index_type, entity_type, labels, props, owning, state in self._run_implicit(
            "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint, state"
        ):
            if index_type == "LOOKUP" and not include_internal:
                continue
            indexes.append(
                IndexInfo(
                    label=(labels or [""])[0],
                    object_type=GraphObjectType.EDGE if entity_type == "RELATIONSHIP" else GraphObjectType.NODE,
                    property_names=list(props) if props else None,
                    index_type=_index_type(index_type, owning),
                    index_name=name,
                    unique=bool(owning),
                    details={
                        "neo4j_index_type": index_type,
                        "entity_type": entity_type,
                        "owning_constraint": owning,
                        "state": state,
                    },
                )
            )
        return indexes


_INDEX_TYPES = {"FULLTEXT": IndexType.FULLTEXT, "VECTOR": IndexType.VECTOR}


def _index_type(neo4j_type: str, owning_constraint: str | None) -> IndexType:
    """A RANGE index is a property index (unique when a constraint owns it); TEXT, POINT and others are OTHER."""
    if neo4j_type == "RANGE":
        return IndexType.UNIQUE if owning_constraint else IndexType.PROPERTY
    return _INDEX_TYPES.get(neo4j_type, IndexType.OTHER)


def _index_name(label: str, prop: str) -> str:
    return f"{label}_{prop}"
