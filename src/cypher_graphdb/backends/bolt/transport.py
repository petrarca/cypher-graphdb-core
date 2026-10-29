"""The transport interface of the Bolt backends, and opening one by name.

A transport runs statements and hands back their rows; its row factory turns its native values
(the driver's nodes, relationships, paths) into cypher-graphdb objects. Transactions: with
``autocommit`` on, every statement is its own transaction; with it off, statements join one
explicit transaction until ``commit()`` or ``rollback()``. ``run_implicit`` runs a statement in
an implicit transaction of its own, also while an explicit one is open (Memgraph requires that of
DDL and settings).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from cypher_graphdb.backend import ExecStatistics
from cypher_graphdb.modelprovider import ModelProvider

TRANSPORTS = ("neo4j", "pymgclient")


class Rows(Protocol):
    """A statement's result, fetched as needed."""

    def fetch(self, n: int) -> Sequence[Sequence[Any]]: ...

    def all(self) -> Sequence[Sequence[Any]]: ...

    def discard(self) -> None: ...


class BoltTransport(ABC):
    """Statements to a Bolt server through one client library."""

    name: str
    autocommit: bool = False

    @abstractmethod
    def run(self, query: str, params: dict | None = None, timeout_s: float | None = None) -> Rows: ...

    @abstractmethod
    def run_implicit(self, query: str, params: dict | None = None) -> Sequence[Sequence[Any]]: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def row_factory(
        self, exec_stats: ExecStatistics, model_provider: ModelProvider | None
    ) -> Callable[[Sequence[Any]], tuple]: ...


def open_transport(
    name: str,
    *,
    scheme: str,
    host: str,
    port: int,
    username: str = "",
    password: str = "",
    database: str | None = None,
    query_timeout_s: float | None = None,
) -> BoltTransport:
    """Open the transport ``name`` ("neo4j" or "pymgclient")."""
    if name == "neo4j":
        from .neo4j_transport import Neo4jTransport

        return Neo4jTransport.open(scheme, host, port, username, password, database, query_timeout_s)
    if name == "pymgclient":
        from .pymgclient_transport import PymgclientTransport

        return PymgclientTransport.open(host, port, username, password)
    raise ValueError(f"transport must be one of {TRANSPORTS}, got {name!r}")
