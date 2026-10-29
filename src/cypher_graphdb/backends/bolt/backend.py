"""What the Bolt backends share: connecting through a transport, running statements, pages, streams,
bulk writes as UNWIND batches, label statistics.

A Bolt backend (Memgraph, Neo4j) adds its dialect: index DDL and catalog queries, its graphs, its
capabilities. The transport is chosen by the connection -- ``bolt://host:7687?transport=neo4j``
or ``connect_params={"transport": ...}`` -- among the backend's ``transports`` (the first is the
default); the URL's scheme (``bolt``, ``bolt+s``, ``neo4j``, ...) goes to the transport.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Generator, Sequence
from typing import Any

from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from cypher_graphdb.backend import BackendCapability, CypherBackend, ExecStatistics
from cypher_graphdb.bulk_refs import as_ref_props, edge_match_pattern, ref_param
from cypher_graphdb.cyphergraphdb.pagination import Page
from cypher_graphdb.cyphergraphdb.pagination_mixin import _resolve_col_names
from cypher_graphdb.cypherparser import ParsedCypherQuery
from cypher_graphdb.models import GraphObject, GraphObjectType, TabularResult
from cypher_graphdb.statistics import LabelStatistics
from cypher_graphdb.utils import chunk_list, parse_connection_uri, validate_protocol

from .transport import BoltTransport, Rows, open_transport

_SCHEMES = ["bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"]


class BoltBackend(CypherBackend):
    """A backend speaking Bolt through a transport; subclasses are the dialects."""

    name = "BOLT"
    transports: tuple[str, ...] = ("neo4j",)  # the first is the default
    default_graph: str | None = None
    selects_database = False  # the graph name selects a database on the server (Neo4j); Memgraph has one

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(args, **kwargs)
        self._connection: BoltTransport | None = None
        self._cinfo: str | None = None
        self._ckwargs: dict[str, Any] | None = None
        if args or kwargs:
            self.connect(*args, **kwargs)

    # -- connection ------------------------------------------------------------

    def connect(self, cinfo: str | None = None, graph_name: str | None = None, **kwargs: Any) -> BoltBackend:
        """Connect: ``cinfo`` a ``bolt://[user:pass@]host[:port][/database][?transport=...]`` URL or ``host=... port=...``."""
        if self._connection is not None:
            return self
        self.autocommit = kwargs.pop("autocommit", self.autocommit)
        self._read_only = kwargs.pop("read_only", self._read_only)
        self._query_timeout_s = kwargs.pop("query_timeout_s", self._query_timeout_s)
        params = self._parse_cinfo(cinfo) if cinfo else {}
        transport = kwargs.pop("transport", None) or params.get("transport") or self.transports[0]
        if transport not in self.transports:
            raise ValueError(f"{self.name} speaks Bolt through {self.transports}, not {transport!r}")
        scheme = params.get("protocol", "bolt")
        host = kwargs.get("host") or params.get("host", "127.0.0.1")
        port = int(kwargs.get("port") or params.get("port", 7687))
        username = kwargs.get("username") or params.get("username", "")
        password = kwargs.get("password") or params.get("password", "")
        self._graph_name = graph_name or params.get("database") or self.default_graph
        self._cinfo, self._ckwargs = cinfo, {**kwargs, "transport": transport, "graph_name": self._graph_name}
        logger.debug(
            "{} connect host={} port={} transport={} autocommit={} query_timeout_s={}",
            self.name,
            host,
            port,
            transport,
            self.autocommit,
            self._query_timeout_s,
        )
        try:
            self._connection = self._open(transport, scheme, host, port, username, password)
            self._connection.autocommit = self.autocommit
            self._after_connect()
        except Exception as e:
            logger.error("Failed to connect to {}: {}", self.name, e)
            self._connection = None
            raise
        return self

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
    )
    def _open(self, transport: str, scheme: str, host: str, port: int, username: str, password: str) -> BoltTransport:
        return open_transport(
            transport,
            scheme=scheme,
            host=host,
            port=port,
            username=username,
            password=password,
            database=self._graph_name if self.selects_database else None,
            query_timeout_s=self._query_timeout_s,
        )

    def _after_connect(self) -> None:
        """A dialect's setup on a new connection (Memgraph: the server's default query timeout)."""

    def _parse_cinfo(self, cinfo: str) -> dict[str, Any]:
        params = parse_connection_uri(cinfo)
        if "protocol" in params:
            validate_protocol(params, _SCHEMES)
        return params

    def disconnect(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
                logger.debug("Disconnected from {}", self.name)
            except Exception as e:
                logger.error("Error disconnecting from {}: {}", self.name, e)
            finally:
                self._connection = None
        self._cinfo = None
        self._ckwargs = None

    def reconnect(self) -> None:
        assert self._ckwargs is not None, "Can only reconnect if already successfully connected!"
        logger.debug("Try to reconnect to {}", self.name)
        if self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.close()
            self._connection = None
        self.connect(self._cinfo, **self._ckwargs)

    def _check_connection(self) -> bool:
        rows = self._run("RETURN 1").fetch(1)
        return bool(rows) and rows[0][0] == 1

    def _require_connection(self) -> None:
        if self._connection is None and self._ckwargs is not None:
            self.reconnect()
        if self._connection is None:
            raise RuntimeError(f"Not connected to {self.name}")

    @property
    def _conn(self) -> BoltTransport:
        if self._connection is None:
            raise RuntimeError(f"Not connected to {self.name}")
        return self._connection

    @property
    def transport(self) -> str:
        """The transport the connection speaks Bolt through."""
        return self._conn.name

    def commit(self) -> None:
        self._require_connection()
        self._conn.commit()

    def rollback(self) -> None:
        self._require_connection()
        self._conn.rollback()

    def get_capability(self, capability: BackendCapability) -> Any:
        match capability:
            case BackendCapability.LABEL_FUNCTION:
                return "labels({$node})[0]"  # use with .format(node_var)
            case (
                BackendCapability.SUPPORT_MULTIPLE_LABELS
                | BackendCapability.STREAMING_SUPPORT
                | BackendCapability.PAGINATION_SUPPORT
            ):
                return True
            case BackendCapability.EXACT_COUNT:
                return False  # an arbitrary query is not cheaply counted: pages probe limit + 1 for has_more
            case BackendCapability.PROPERTY_INDEX:
                return True
            case BackendCapability.REQUIRES_GRAPH_NAME:
                return False  # the server's default database serves without one
            case _:
                return super().get_capability(capability)

    # -- statements ------------------------------------------------------------

    def execute_cypher(
        self, cypher_query: ParsedCypherQuery, fetch_one: bool = False, raw_data: bool = False, params: dict | None = None
    ) -> tuple[TabularResult, ExecStatistics]:
        assert isinstance(cypher_query, ParsedCypherQuery)
        self._validate_read_only(cypher_query)
        return self._execute_query(cypher_query.parsed_query, fetch_one, raw_data, params)

    def execute_cypher_page(
        self,
        cypher_query: ParsedCypherQuery,
        offset: int = 0,
        limit: int = 100,
        want_total: bool = True,  # noqa: ARG002 -- an arbitrary query is not cheaply counted
        raw_data: bool = False,
        params: dict | None = None,
    ) -> Page:
        """A window by Cypher SKIP/LIMIT, fetching ``limit + 1`` rows for ``has_more``; ``total`` is None."""
        self._validate_read_only(cypher_query)
        query = f"{cypher_query.parsed_query.rstrip('; \t\n')} SKIP $__cypher_page_skip__ LIMIT $__cypher_page_limit__"
        page_params = {**(params or {}), "__cypher_page_skip__": offset, "__cypher_page_limit__": limit + 1}
        rows, exec_stats = self._execute_query(query, fetch_one=False, raw_data=raw_data, params=page_params)
        rows = list(rows) if rows else []
        return Page(
            rows=rows[:limit],
            offset=offset,
            limit=limit,
            total=None,
            has_more=len(rows) > limit,
            col_names=_resolve_col_names(cypher_query, rows[:limit], exec_stats),
        )

    def fulltext_search(
        self, cypher_query: ParsedCypherQuery, fts_query: str, language: str | None = None
    ) -> tuple[list[tuple[GraphObject]], ExecStatistics]:
        return ([], ExecStatistics())

    def execute_cypher_stream(
        self, cypher_query: ParsedCypherQuery, chunk_size: int = 1000, raw_data: bool = False, params: dict | None = None
    ) -> Generator[TabularResult]:
        """The rows in chunks of ``chunk_size``; a caller stopping early discards the rest."""
        assert isinstance(cypher_query, ParsedCypherQuery)
        self._validate_read_only(cypher_query)
        self._require_connection()
        query = cypher_query.parsed_query
        logger.debug("{} execute stream (chunk_size={}): {}", self.name, chunk_size, query)
        rows = self._run(query, params)
        row = self._conn.row_factory(ExecStatistics(), self._model_provider)
        try:
            while chunk := rows.fetch(chunk_size):
                yield [tuple(r) for r in chunk] if raw_data else [row(r) for r in chunk]
        except GeneratorExit:
            rows.discard()
            raise

    def _execute_query(
        self, query: str, fetch_one: bool = False, raw_data: bool = False, params: dict | None = None
    ) -> tuple[TabularResult, ExecStatistics]:
        self._require_connection()
        start = time.perf_counter()
        logger.debug("{} execute: {}", self.name, query)
        exec_stats = ExecStatistics()
        try:
            rows = self._run(query, params)
            fetched = rows.fetch(1) if fetch_one else rows.all()
            rows.discard()  # the rest, when only one row was asked for
        except Exception as e:
            logger.error("{} query error: {}", self.name, e)
            raise
        if raw_data:
            result: list[Any] = [tuple(r) for r in fetched] if fetched or not fetch_one else [None]
        else:
            row = self._conn.row_factory(exec_stats, self._model_provider)
            result = [row(r) for r in fetched]
        exec_stats.exec_time = time.perf_counter() - start
        logger.debug("exec_time={:.4f}s", exec_stats.exec_time)
        if result and isinstance(result[0], tuple):
            exec_stats.col_count = len(result[0])
        return (result, exec_stats)

    def _run(self, query: str, params: dict | None = None) -> Rows:
        """A statement through the transport, with the statement timeout when one is active."""
        return self._conn.run(query, params, self._statement_timeout_s)

    def is_timeout_error(self, error: Exception) -> bool:
        """The server aborted the transaction for its timeout: Neo4j's ``...TransactionTimedOut...`` code, Memgraph's message."""
        for cause in (error, error.__cause__):
            if cause is None:
                continue
            message = str(cause).lower()
            if "TimedOut" in (getattr(cause, "code", "") or "") or ("abort" in message and "timeout" in message):
                return True
        return False

    def _run_implicit(self, statement: str) -> Sequence[Sequence[Any]]:
        """A statement in an implicit transaction of its own (DDL, settings, catalog queries)."""
        self._require_connection()
        return self._conn.run_implicit(statement)

    # -- catalog ---------------------------------------------------------------

    def labels(self) -> list[LabelStatistics]:
        """The node labels and relationship types of the graph, with counts."""
        query = """
        MATCH (n) UNWIND labels(n) AS label RETURN 'node' AS entity_type, label AS name, count(*) AS cnt
        UNION ALL
        MATCH ()-[r]->() RETURN 'relationship' AS entity_type, type(r) AS name, count(r) AS cnt
        """
        try:
            result, _ = self._execute_query(query, raw_data=True)
        except Exception as e:
            logger.error("Failed to get label statistics: {}", e)
            return []
        return [
            LabelStatistics(
                graph_name=self.graph_name,
                label_=name,
                type_=GraphObjectType.NODE if entity_type == "node" else GraphObjectType.EDGE,
                count=count,
            )
            for entity_type, name, count in sorted(result, key=lambda r: (r[0], r[1] or ""))
            if name
        ]

    # -- bulk writes -----------------------------------------------------------

    def bulk_create_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        self._require_connection()
        total = 0
        for batch in chunk_list(rows, batch_size):
            self._run(f"UNWIND $rows AS props CREATE (n:{label}) SET n = props", {"rows": batch}).discard()
            total += len(batch)
        return total

    def bulk_create_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str,
        dst_label: str,
        src_ref_prop: str | Sequence[str] = "id",
        dst_ref_prop: str | Sequence[str] = "id",
        batch_size: int = 500,
    ) -> int:
        """Edges by parameterized UNWIND: each dict has "src" and "dst" (a composite key: a sequence), the rest are properties."""
        self._require_connection()
        if not edges:
            return 0
        src_props, dst_props = as_ref_props(src_ref_prop), as_ref_props(dst_ref_prop)
        src_pat = edge_match_pattern("a", src_label, src_props, "src")
        dst_pat = edge_match_pattern("b", dst_label, dst_props, "dst")
        edge_keys = [k for k in edges[0] if k not in ("src", "dst")]
        set_clause = (" SET " + ", ".join(f"r.{k} = e.{k}" for k in edge_keys)) if edge_keys else ""
        cypher = f"UNWIND $edges AS e MATCH {src_pat} MATCH {dst_pat} CREATE (a)-[r:{label}]->(b){set_clause}"
        # A composite key is a tuple: coerced per endpoint by its own key arity (source and destination may differ).
        edges = [{**e, "src": ref_param(e["src"], len(src_props)), "dst": ref_param(e["dst"], len(dst_props))} for e in edges]
        total = 0
        for batch in chunk_list(edges, batch_size):
            self._run(cypher, {"edges": batch}).discard()
            total += len(batch)
        return total

    def bulk_delete_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str,
        dst_label: str,
        src_ref_prop: str | Sequence[str] = "id",
        dst_ref_prop: str | Sequence[str] = "id",
        batch_size: int = 500,
    ) -> int:
        """Every ``label`` edge between each matched pair, by parameterized UNWIND; the number deleted."""
        self._require_connection()
        if not edges:
            return 0
        src_props, dst_props = as_ref_props(src_ref_prop), as_ref_props(dst_ref_prop)
        src_pat = edge_match_pattern("a", src_label, src_props, "src")
        dst_pat = edge_match_pattern("b", dst_label, dst_props, "dst")
        cypher = f"UNWIND $edges AS e MATCH {src_pat}-[r:{label}]->{dst_pat} DELETE r RETURN count(r) AS deleted"
        pairs = [{"src": ref_param(e["src"], len(src_props)), "dst": ref_param(e["dst"], len(dst_props))} for e in edges]
        return sum(self._count(cypher, {"edges": batch}) for batch in chunk_list(pairs, batch_size))

    def bulk_update_nodes(self, label: str, rows: list[dict], ref_prop: str | Sequence[str] = "id", batch_size: int = 500) -> int:
        """Set properties on matched nodes by parameterized UNWIND; the number updated."""
        self._require_connection()
        if not rows:
            return 0
        props = as_ref_props(ref_prop)
        set_keys = [k for k in rows[0] if k != "ref"]
        if not set_keys:
            return 0
        match = edge_match_pattern("n", label, props, "ref")
        cypher = f"UNWIND $rows AS e MATCH {match} SET {', '.join(f'n.{k} = e.{k}' for k in set_keys)} RETURN count(n) AS updated"
        params = [{**r, "ref": ref_param(r["ref"], len(props))} for r in rows]
        return sum(self._count(cypher, {"rows": batch}) for batch in chunk_list(params, batch_size))

    def _count(self, cypher: str, params: dict) -> int:
        rows = self._run(cypher, params)
        first = rows.fetch(1)
        rows.discard()
        return first[0][0] if first else 0
