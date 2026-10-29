"""pymgclient -- Memgraph's own client -- as a Bolt transport (Memgraph's default).

A DB-API connection: with ``autocommit`` on, the connection itself auto-commits (no BEGIN/COMMIT
round trips per statement); with it off, statements join one transaction until ``commit()``.
pymgclient cannot send a transaction timeout: a statement's timeout is not applied (the server's
``query.timeout`` holds). Values are turned into cypher-graphdb objects in one pass.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import mgclient
from loguru import logger

from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge, GraphNode, GraphObject, GraphPath

from .transport import BoltTransport

_PLAIN = (str, int, float, bool, type(None))


class _Rows:
    """A cursor as rows: the tuples pymgclient returns."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: mgclient.Cursor):
        self._cursor = cursor

    def fetch(self, n: int) -> list[tuple]:
        return self._cursor.fetchmany(n)

    def all(self) -> list[tuple]:
        return self._cursor.fetchall()

    def discard(self) -> None:
        self._cursor.close()


class PymgclientTransport(BoltTransport):
    """Bolt through pymgclient, Memgraph's own client."""

    name = "pymgclient"

    def __init__(self, connection: mgclient.Connection):
        self._conn = connection
        self._warned_timeout = False

    @classmethod
    def open(cls, host: str, port: int, username: str, password: str) -> PymgclientTransport:
        try:
            return cls(mgclient.connect(host=host, port=port, username=username, password=password))
        except mgclient.OperationalError as e:
            raise ConnectionError(str(e)) from e

    @property  # type: ignore[override]
    def autocommit(self) -> bool:
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._conn.autocommit = value

    def run(self, query: str, params: dict | None = None, timeout_s: float | None = None) -> _Rows:
        if timeout_s and not self._warned_timeout:
            logger.warning("pymgclient cannot send a transaction timeout: the server's query.timeout applies")
            self._warned_timeout = True
        cursor = self._conn.cursor()
        cursor.execute(query, params or {})
        return _Rows(cursor)

    def run_implicit(self, query: str, params: dict | None = None) -> list[tuple]:
        previous = self._conn.autocommit
        self._conn.autocommit = True
        try:
            cursor = self._conn.cursor()
            cursor.execute(query, params or {})
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            self._conn.autocommit = previous

    def commit(self) -> None:
        if not self._conn.autocommit:
            self._conn.commit()

    def rollback(self) -> None:
        if not self._conn.autocommit:
            self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def row_factory(self, exec_stats: ExecStatistics, model_provider: ModelProvider | None) -> Callable[[Sequence[Any]], tuple]:
        def row(values: Sequence[Any]) -> tuple:
            exec_stats.row_count += 1
            return tuple(_value(v, exec_stats, model_provider) for v in values)

        return row


def _property(value: Any) -> Any:
    if isinstance(value, _PLAIN):
        return value
    if isinstance(value, list):
        return [_property(v) for v in value]
    if isinstance(value, dict):
        return {k: _property(v) for k, v in value.items()}
    return value  # dates, times, durations: pymgclient returns native ones


def _properties(props: dict[str, Any]) -> dict[str, Any]:
    return {k: v if isinstance(v, _PLAIN) else _property(v) for k, v in props.items()}


def _node(node: mgclient.Node, model_provider: ModelProvider | None) -> GraphNode:
    labels = sorted(node.labels)  # sorted, as the neo4j transport does: the same first label on either transport
    if model_provider is not None and labels:
        return model_provider.create_node(label=labels[0], props=_properties(node.properties), id_=node.id)
    return GraphNode(id_=node.id, label_=labels[0] if labels else None, properties_=_properties(node.properties))


def _edge(rel: mgclient.Relationship, model_provider: ModelProvider | None) -> GraphEdge:
    if model_provider is not None:
        return model_provider.create_edge(
            label=rel.type, start_id=rel.start_id, end_id=rel.end_id, props=_properties(rel.properties), id_=rel.id
        )
    return GraphEdge(
        id_=rel.id, label_=rel.type, start_id_=rel.start_id, end_id_=rel.end_id, properties_=_properties(rel.properties)
    )


def _path(path: mgclient.Path, model_provider: ModelProvider | None) -> GraphPath:
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
    if isinstance(value, mgclient.Node):
        stats.nodes += 1
        return _node(value, model_provider)
    if isinstance(value, mgclient.Relationship):
        stats.edges += 1
        return _edge(value, model_provider)
    if isinstance(value, mgclient.Path):
        stats.paths += 1
        return _path(value, model_provider)
    if isinstance(value, list | tuple):
        return [_value(v, stats, model_provider) for v in value]
    if isinstance(value, dict):
        return {k: _value(v, stats, model_provider) for k, v in value.items()}
    stats.values += 1
    return value
