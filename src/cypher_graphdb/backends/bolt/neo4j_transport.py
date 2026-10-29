"""The Neo4j Python driver (with its Rust extension) as a Bolt transport -- Neo4j's, and Memgraph's on request.

One driver session and at most one explicit transaction. A statement's timeout -- its own, else the
connection's default -- is sent as the Bolt transaction timeout: the server aborts the transaction
when it expires (in an explicit transaction, the timeout of the statement that opened it). Records
are turned into cypher-graphdb objects in one pass: a node's properties are copied once into its
model, primitives pass through, only temporal values become native ones.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import neo4j
from neo4j import graph

from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge, GraphNode, GraphObject, GraphPath

from .transport import BoltTransport

# Records pulled per request: more than the driver's default (1000) -- fewer round trips on large results.
FETCH_SIZE = 5000

_PLAIN = (str, int, float, bool, type(None))


class _Rows:
    """A driver result as rows: no copy, the records themselves (tuples)."""

    __slots__ = ("_result",)

    def __init__(self, result: neo4j.Result):
        self._result = result

    def fetch(self, n: int) -> list[neo4j.Record]:
        return self._result.fetch(n)

    def all(self) -> list[neo4j.Record]:
        return list(self._result)

    def discard(self) -> None:
        self._result.consume()


class Neo4jTransport(BoltTransport):
    """Bolt through the Neo4j Python driver."""

    name = "neo4j"

    def __init__(self, driver: neo4j.Driver, database: str | None, query_timeout_s: float | None):
        self._driver = driver
        self._database = database
        self._timeout_s = query_timeout_s or None  # 0: none
        self._session = driver.session(database=database, fetch_size=FETCH_SIZE)
        self._tx: neo4j.Transaction | None = None

    @classmethod
    def open(
        cls, scheme: str, host: str, port: int, username: str, password: str, database: str | None, query_timeout_s: float | None
    ) -> Neo4jTransport:
        driver = neo4j.GraphDatabase.driver(f"{scheme}://{host}:{port}", auth=(username, password) if username else None)
        try:
            driver.verify_connectivity()
        except neo4j.exceptions.ServiceUnavailable as e:
            driver.close()
            raise ConnectionError(str(e)) from e
        except Exception:
            driver.close()
            raise
        return cls(driver, database, query_timeout_s)

    def run(self, query: str, params: dict | None = None, timeout_s: float | None = None) -> _Rows:
        timeout = self._timeout_s if timeout_s is None else (timeout_s or None)
        if self.autocommit:
            return _Rows(self._session.run(neo4j.Query(query, timeout=timeout), params))
        if self._tx is None:
            self._tx = self._session.begin_transaction(timeout=timeout)
        return _Rows(self._tx.run(query, params))

    def run_implicit(self, query: str, params: dict | None = None) -> list[neo4j.Record]:
        with self._driver.session(database=self._database, fetch_size=FETCH_SIZE) as session:
            return list(session.run(query, params))

    def commit(self) -> None:
        if self._tx is not None:
            tx, self._tx = self._tx, None
            tx.commit()

    def rollback(self) -> None:
        if self._tx is not None:
            tx, self._tx = self._tx, None
            tx.rollback()

    def close(self) -> None:
        try:
            self.rollback()
        finally:
            self._session.close()
            self._driver.close()

    def row_factory(self, exec_stats: ExecStatistics, model_provider: ModelProvider | None) -> Callable[[Sequence[Any]], tuple]:
        def row(record: Sequence[Any]) -> tuple:
            exec_stats.row_count += 1
            return tuple(_value(v, exec_stats, model_provider) for v in record)

        return row


def _id(element_id: str) -> int:
    """An element id as cypher-graphdb's integer id: Memgraph's is the integer as text, Neo4j's ends with it (``4:<db>:12``)."""
    return int(element_id.rpartition(":")[2])


def _property(value: Any) -> Any:
    if isinstance(value, _PLAIN):
        return value
    if isinstance(value, list):
        return [_property(v) for v in value]
    if isinstance(value, dict):
        return {k: _property(v) for k, v in value.items()}
    to_native = getattr(value, "to_native", None)  # neo4j.time.Date, DateTime, Time, Duration
    return to_native() if callable(to_native) else value


def _properties(entity: graph.Entity) -> dict[str, Any]:
    return {k: v if isinstance(v, _PLAIN) else _property(v) for k, v in entity.items()}


def _node(node: graph.Node, model_provider: ModelProvider | None) -> GraphNode:
    labels = sorted(node.labels)  # a frozenset: sorted, so the first label is deterministic
    if model_provider is not None and labels:
        return model_provider.create_node(label=labels[0], props=_properties(node), id_=_id(node.element_id))
    return GraphNode(id_=_id(node.element_id), label_=labels[0] if labels else None, properties_=_properties(node))


def _edge(rel: graph.Relationship, model_provider: ModelProvider | None) -> GraphEdge:
    # A relationship from Bolt always carries its endpoints' ids (as nodes, possibly without properties).
    assert rel.start_node is not None and rel.end_node is not None
    start_id, end_id = _id(rel.start_node.element_id), _id(rel.end_node.element_id)
    if model_provider is not None:
        return model_provider.create_edge(
            label=rel.type, start_id=start_id, end_id=end_id, props=_properties(rel), id_=_id(rel.element_id)
        )
    return GraphEdge(id_=_id(rel.element_id), label_=rel.type, start_id_=start_id, end_id_=end_id, properties_=_properties(rel))


def _path(path: graph.Path, model_provider: ModelProvider | None) -> GraphPath:
    entities: list[GraphObject] = []
    for i, node in enumerate(path.nodes):
        entities.append(_node(node, model_provider))
        if i < len(path.relationships):
            entities.append(_edge(path.relationships[i], model_provider))
    return GraphPath(entities=entities)


def _value(value: Any, stats: ExecStatistics, model_provider: ModelProvider | None) -> Any:
    if isinstance(value, _PLAIN):
        if value is not None:
            stats.values += 1
        return value
    if isinstance(value, graph.Node):
        stats.nodes += 1
        return _node(value, model_provider)
    if isinstance(value, graph.Relationship):
        stats.edges += 1
        return _edge(value, model_provider)
    if isinstance(value, graph.Path):
        stats.paths += 1
        return _path(value, model_provider)
    if isinstance(value, list):
        return [_value(v, stats, model_provider) for v in value]
    if isinstance(value, dict):
        return {k: _value(v, stats, model_provider) for k, v in value.items()}
    stats.values += 1
    return _property(value)
