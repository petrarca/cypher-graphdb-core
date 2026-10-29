"""Direct SQL bulk writer for Apache AGE label tables.

Bypasses the Cypher parser by INSERTing directly into AGE's PostgreSQL label
tables using _graphid() + nextval() for ID generation and ::agtype for
properties. This is the same pattern AGE's own test suite uses
(regress/sql/age_global_graph.sql) and is 10-30x faster than Cypher UNWIND
for large bulk loads.

The writer is stateless: it receives a connection and graph_name, performs
the INSERT, and returns. Transaction control (commit/rollback) is the
caller's responsibility, with one exception: when a label does not yet exist,
``_ensure_label`` creates it via DDL inside a savepoint so that the DDL is
committed immediately (DDL cannot run inside a user transaction in PostgreSQL)
without affecting any surrounding data already written by the caller.
"""

from collections.abc import Sequence
from typing import Literal

import psycopg
from loguru import logger
from psycopg.sql import SQL, Composed, Identifier
from psycopg.sql import Literal as SQLLiteral

from cypher_graphdb.bulk_refs import as_ref_props, ref_key
from cypher_graphdb.utils import chunk_list

from .ageserializer import escape_value
from .agesqlbuilder import SQLBuilder


def _build_agtype_conditions(filters: dict[str, str], alias: str | None = None) -> str:
    """Build a SQL WHERE expression matching AGE vertex properties.

    Uses agtype_access_operator so the expression btree index is used.
    Values are escaped via SQLLiteral to prevent injection.

    Args:
        filters: Property key=value pairs (all ANDed together).
        alias: Optional table alias prefix (e.g. "n" produces "n.properties").
    """
    prefix = f"{alias}." if alias else ""
    return " AND ".join(
        f"ag_catalog.agtype_access_operator(VARIADIC ARRAY[{prefix}properties, "
        f"{SQLLiteral(chr(34) + k + chr(34)).as_string(None)}::ag_catalog.agtype])::text = {SQLLiteral(v).as_string(None)}"
        for k, v in filters.items()
    )


class AGEBulkWriter:
    """Direct SQL bulk writer for AGE label tables.

    Args:
        connection: An open psycopg connection to the AGE database.
        graph_name: The AGE graph name (= PostgreSQL schema name).
    """

    def __init__(self, connection: psycopg.Connection, graph_name: str) -> None:
        self._conn = connection
        self._graph_name = graph_name
        # Cache for {(label, ref_prop): {ref_value: graphid}} maps.
        # Avoids repeated full-table scans when bulk_insert_edges is called
        # many times with the same src/dst labels (e.g. 400+ batches of CALLS
        # edges all resolving against Method.qualified_name).
        # Invalidated by invalidate_graphid_cache() after node inserts that
        # change the graphid mapping.
        self._graphid_cache: dict[tuple[str, tuple[str, ...]], dict[tuple, int]] = {}
        # Cached list of edge label names for bulk_delete_nodes.
        # Edge labels are stable within a connection lifetime -- populated on
        # first call to _list_edge_labels() and reused for all subsequent deletes.
        self._edge_labels_cache: list[str] | None = None

    # -- Public API -----------------------------------------------------------

    def bulk_insert_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        """Insert nodes via direct SQL INSERT into the AGE label table.

        Creates the vlabel if it doesn't exist yet (AGE normally does this
        lazily on first Cypher CREATE).

        Uses multi-row INSERT with inline agtype literals (not parameterized)
        because AGE's agtype_in text-input function does not interpret JSON
        escape sequences correctly when received as psycopg text parameters.
        The agtype values are built using the same escape_value function as
        the Cypher UNWIND path, ensuring identical storage.

        Args:
            label: Vertex label name (e.g. "Method").
            rows: List of property dicts, one per node.
            batch_size: Number of rows per multi-row INSERT.

        Returns:
            Total number of nodes inserted.
        """
        if not rows:
            return 0

        label_id = self._ensure_label(label, kind="v")

        total = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(rows, batch_size):
                values = ", ".join(
                    f"(ag_catalog._graphid({label_id}, nextval('{self._seq_name(label)}'::regclass)), "
                    f"{self._to_agtype_literal(row)}::ag_catalog.agtype)"
                    for row in batch
                )
                cursor.execute(
                    SQL("INSERT INTO {schema}.{table} (id, properties) VALUES {vals}").format(
                        schema=Identifier(self._graph_name),
                        table=Identifier(label),
                        vals=SQL(values),
                    )
                )
                total += len(batch)

        logger.debug("Direct SQL: {} {} nodes inserted", total, label)
        # Invalidate cached graphid maps for this label so subsequent edge
        # inserts see the newly created nodes.
        self.invalidate_graphid_cache(label)
        return total

    def bulk_insert_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str,
        dst_label: str,
        src_ref_prop: str | Sequence[str] = "id",
        dst_ref_prop: str | Sequence[str] = "id",
        batch_size: int = 500,
    ) -> int:
        """Insert edges via direct SQL INSERT into the AGE edge label table.

        Pre-resolves source and destination graphids via a single SQL query
        per label, then INSERTs edges with known start_id/end_id -- no Cypher
        MATCH at write time.

        Edges whose src or dst reference cannot be resolved are silently
        skipped (same behaviour as Cypher MATCH -- unmatched patterns produce
        no rows).

        Args:
            label: Edge label name (e.g. "CALLS").
            edges: List of dicts with at least "src" and "dst" keys.
            src_label: Vertex label of source nodes.
            dst_label: Vertex label of destination nodes.
            src_ref_prop: Property (or ordered sequence of properties) on source
                nodes to match "src" values. A sequence forms a composite key;
                each edge's "src" is then a parallel sequence of values.
            dst_ref_prop: As ``src_ref_prop``, for destination nodes.
            batch_size: Number of rows per executemany batch.

        Returns:
            Total number of edges inserted (excludes skipped).
        """
        if not edges:
            return 0

        label_id = self._ensure_label(label, kind="e")

        src_props = as_ref_props(src_ref_prop)
        dst_props = as_ref_props(dst_ref_prop)

        # Collect the unique (composite) ref keys we actually need -- avoids
        # loading the entire label table (200K+ rows in a shared graph).
        src_refs = {ref_key(e["src"], len(src_props)) for e in edges}
        dst_refs = {ref_key(e["dst"], len(dst_props)) for e in edges}

        # Build {ref_key: graphid} maps for only the referenced nodes
        src_map = self._build_graphid_index(src_label, src_props, ref_values=src_refs)
        if src_label == dst_label and src_props == dst_props:
            dst_map = self._build_graphid_index(dst_label, dst_props, ref_values=src_refs | dst_refs)
        else:
            dst_map = self._build_graphid_index(dst_label, dst_props, ref_values=dst_refs)

        total = 0
        skipped = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(edges, batch_size):
                value_parts = []
                for edge in batch:
                    src_gid = src_map.get(ref_key(edge["src"], len(src_props)))
                    dst_gid = dst_map.get(ref_key(edge["dst"], len(dst_props)))
                    if src_gid is None or dst_gid is None:
                        skipped += 1
                        continue
                    props = {k: v for k, v in edge.items() if k not in ("src", "dst")}
                    value_parts.append(
                        f"(ag_catalog._graphid({label_id}, nextval('{self._seq_name(label)}'::regclass)), "
                        f"'{src_gid}'::ag_catalog.graphid, '{dst_gid}'::ag_catalog.graphid, "
                        f"{self._to_agtype_literal(props)}::ag_catalog.agtype)"
                    )
                if value_parts:
                    values = ", ".join(value_parts)
                    cursor.execute(
                        SQL("INSERT INTO {schema}.{table} (id, start_id, end_id, properties) VALUES {vals}").format(
                            schema=Identifier(self._graph_name),
                            table=Identifier(label),
                            vals=SQL(values),
                        )
                    )
                    total += len(value_parts)

        if skipped:
            logger.debug("Direct SQL: {} {} edges inserted, {} skipped (unresolved endpoints)", total, label, skipped)
        else:
            logger.debug("Direct SQL: {} {} edges inserted", total, label)
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
        """Delete edges via direct SQL DELETE on the AGE edge label table.

        The counterpart of :meth:`bulk_insert_edges`: resolves the source and
        destination graphids with one SQL query per label, then deletes by
        ``(start_id, end_id)`` -- no Cypher MATCH ... DELETE, which is slow on AGE
        for large sets. Graphids are looked up fresh, not from the insert cache:
        nodes deleted and recreated since would otherwise resolve to stale ids.

        Pairs whose src or dst reference cannot be resolved are skipped, and a
        label whose table does not exist yet deletes nothing.

        Returns:
            Total number of edges deleted.
        """
        if (
            not edges
            or not self._label_exists(label, "e")
            or not self._label_exists(src_label)
            or not self._label_exists(dst_label)
        ):
            return 0

        src_props = as_ref_props(src_ref_prop)
        dst_props = as_ref_props(dst_ref_prop)
        src_map = self._query_graphids(src_label, src_props, {ref_key(e["src"], len(src_props)) for e in edges})
        dst_map = self._query_graphids(dst_label, dst_props, {ref_key(e["dst"], len(dst_props)) for e in edges})
        pairs = {
            (src_map[s], dst_map[d])
            for s, d in ((ref_key(e["src"], len(src_props)), ref_key(e["dst"], len(dst_props))) for e in edges)
            if s in src_map and d in dst_map
        }

        total = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(sorted(pairs), batch_size):
                values = ", ".join(f"('{src}'::ag_catalog.graphid, '{dst}'::ag_catalog.graphid)" for src, dst in batch)
                cursor.execute(
                    SQL("DELETE FROM {schema}.{table} WHERE (start_id, end_id) IN (VALUES {vals})").format(
                        schema=Identifier(self._graph_name),
                        table=Identifier(label),
                        vals=SQL(values),
                    )
                )
                total += cursor.rowcount
        logger.debug("Direct SQL: {} {} edges deleted ({} pairs resolved of {})", total, label, len(pairs), len(edges))
        return total

    def bulk_delete_nodes(self, label: str, filters: dict[str, str]) -> int:
        """Delete nodes matching property filters via direct SQL, cascading to edges.

        Uses server-side joined DELETEs -- PostgreSQL resolves the node filter
        via the expression index on the vertex table, then joins to each edge
        table via the indexed start_id/end_id columns. No large ID arrays are
        transferred to Python; the entire operation runs inside the database.

        On a fresh graph where the label table does not yet exist, returns 0.

        Args:
            label: Vertex label to delete from (e.g. "Method").
            filters: Property key=value filters (AND semantics).

        Returns:
            Number of nodes deleted.
        """
        if not filters:
            raise ValueError("filters must not be empty (would delete all nodes of the label)")

        # Build filter conditions in two forms:
        # - with alias "n." for the USING join (edge DELETE references vertex as n)
        # - without alias for the plain vertex DELETE
        join_conditions = _build_agtype_conditions(filters, alias="n")
        node_conditions = _build_agtype_conditions(filters, alias=None)
        edge_labels = self._list_edge_labels()

        try:
            with self._conn.cursor() as cursor:
                # Step 1: delete edges via JOIN -- lets PG use expression indexes
                # on the vertex table + start_id/end_id indexes on edge tables.
                for elabel in edge_labels:
                    cursor.execute(
                        SQL(
                            "DELETE FROM {egraph}.{etable} e "
                            "USING {vgraph}.{vtable} n "
                            "WHERE (e.start_id = n.id OR e.end_id = n.id) "
                            "AND {cond}"
                        ).format(
                            egraph=Identifier(self._graph_name),
                            etable=Identifier(elabel),
                            vgraph=Identifier(self._graph_name),
                            vtable=Identifier(label),
                            cond=SQL(join_conditions),
                        )
                    )
                # Step 2: delete the nodes
                cursor.execute(
                    SQL("DELETE FROM {schema}.{table} WHERE {cond} RETURNING id").format(
                        schema=Identifier(self._graph_name),
                        table=Identifier(label),
                        cond=SQL(node_conditions),
                    )
                )
                deleted = len(cursor.fetchall())
        except psycopg.errors.UndefinedTable:
            # Label table does not exist (fresh graph, never written to).
            self._conn.rollback()
            return 0

        self.invalidate_graphid_cache(label)
        logger.debug("Direct SQL: {} {} nodes deleted (+ edges from {} edge tables)", deleted, label, len(edge_labels))
        return deleted

    def bulk_delete_orphans(
        self, label: str, edge_label: str, *, incoming: bool = True, filters: dict[str, str] | None = None
    ) -> int:
        """Delete orphan nodes via a direct SQL anti-join, cascading to edges.

        An orphan is a vertex of ``label`` with no edge of ``edge_label``
        attached in the requested direction. PostgreSQL resolves this via the
        ``start_id``/``end_id`` btree index on the edge table, so the scan is
        index-backed instead of a full relationship traversal.

        On a fresh graph where the vertex label table does not yet exist,
        returns 0.

        Args:
            label: Vertex label to scan for orphans (e.g. "Dependency").
            edge_label: Edge label whose absence marks an orphan
                (e.g. "HAS_DEPENDENCY").
            incoming: If True, the edge points INTO the node -- match on
                ``edge.end_id``. If False, the edge points OUT -- match on
                ``edge.start_id``.
            filters: Optional property key=value filters (AND semantics) that
                restrict the orphan scan to matching nodes only.

        Returns:
            Number of orphan nodes deleted.
        """
        # The graphid equality operator lives in ag_catalog. Qualify it inline
        # via OPERATOR(ag_catalog.=) so the anti-join works regardless of the
        # connection's search_path (autocommit makes SET LOCAL unreliable).
        ref_col = "end_id" if incoming else "start_id"
        edge_labels = self._list_edge_labels()

        # When the edge label table does not exist, no node can have such an
        # edge -- every node of the label is an orphan. Use an always-true
        # predicate instead of an anti-join against a missing table.
        no_edge_pred: SQL | Composed
        if edge_label in edge_labels:
            no_edge_pred = SQL(
                "NOT EXISTS (SELECT 1 FROM {egraph}.{eref} ref WHERE ref.{refcol} OPERATOR(ag_catalog.=) n.id)"
            ).format(
                egraph=Identifier(self._graph_name),
                eref=Identifier(edge_label),
                refcol=SQL(ref_col),
            )
        else:
            no_edge_pred = SQL("TRUE")

        # Optionally restrict the scan to nodes matching property filters.
        orphan_pred: SQL | Composed
        if filters:
            orphan_pred = SQL("({no_edge}) AND ({props})").format(
                no_edge=no_edge_pred,
                props=SQL(_build_agtype_conditions(filters, alias="n")),
            )
        else:
            orphan_pred = no_edge_pred

        try:
            with self._conn.cursor() as cursor:
                # Step 1: delete edges attached to the soon-to-be-orphan nodes.
                # A node is an orphan w.r.t. edge_label; other edge types may
                # still reference it, so detach across all edge tables first.
                for elabel in edge_labels:
                    cursor.execute(
                        SQL(
                            "DELETE FROM {egraph}.{etable} e "
                            "USING {vgraph}.{vtable} n "
                            "WHERE (e.start_id OPERATOR(ag_catalog.=) n.id "
                            "OR e.end_id OPERATOR(ag_catalog.=) n.id) AND {pred}"
                        ).format(
                            egraph=Identifier(self._graph_name),
                            etable=Identifier(elabel),
                            vgraph=Identifier(self._graph_name),
                            vtable=Identifier(label),
                            pred=orphan_pred,
                        )
                    )

                # Step 2: delete the orphan vertices themselves.
                cursor.execute(
                    SQL("DELETE FROM {vgraph}.{vtable} n WHERE {pred} RETURNING n.id").format(
                        vgraph=Identifier(self._graph_name),
                        vtable=Identifier(label),
                        pred=orphan_pred,
                    )
                )
                deleted = len(cursor.fetchall())
        except psycopg.errors.UndefinedTable:
            # Vertex label table does not exist yet -- nothing to delete.
            self._conn.rollback()
            return 0

        self.invalidate_graphid_cache(label)
        logger.debug("Direct SQL: {} orphaned {} nodes deleted (no {} edge)", deleted, label, edge_label)
        return deleted

    def _list_edge_labels(self) -> list[str]:
        """Return all edge label names in the graph, cached for the connection lifetime.

        Edge labels are stable within a session -- they grow monotonically as new
        labels are created, never shrink. Caching avoids repeated catalog queries
        when bulk_delete_nodes is called many times in the same cleanup pass.
        """
        if self._edge_labels_cache is not None:
            return self._edge_labels_cache
        with self._conn.cursor() as cursor:
            cursor.execute(
                SQL(
                    "SELECT name FROM ag_catalog.ag_label "
                    "WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = %s) "
                    "AND kind = 'e' AND name != '_ag_label_edge'"
                ),
                (self._graph_name,),
            )
            self._edge_labels_cache = [row[0] for row in cursor.fetchall()]
        return self._edge_labels_cache

    # -- Internal helpers -----------------------------------------------------

    def _ensure_label(self, label: str, kind: Literal["v", "e"] = "v") -> int:
        """Look up the numeric label_id, creating the label if needed.

        When the label does not exist, creates it via a savepoint so that the
        DDL commit is isolated. PostgreSQL DDL (CREATE TABLE, which AGE's
        create_vlabel/create_elabel issue internally) cannot run inside an open
        transaction; committing via savepoint releases the DDL without
        discarding any data already written by the caller in the outer
        transaction.

        Args:
            label: Label name (e.g. "Method", "CALLS").
            kind: ``"v"`` for vertex label, ``"e"`` for edge label.

        Returns:
            The integer label_id for the label.

        Raises:
            ValueError: If the label cannot be created or looked up.
        """
        lookup_sql = SQLBuilder.lookup_label_id_sql()
        with self._conn.cursor() as cursor:
            cursor.execute(lookup_sql, (self._graph_name, label))
            row = cursor.fetchone()
            if row:
                return row[0]

            # Label doesn't exist -- create it inside a savepoint so that the
            # DDL commit does not affect the caller's surrounding transaction.
            cursor.execute("SAVEPOINT _age_ensure_label")
            try:
                if kind == "v":
                    cursor.execute(SQLBuilder.create_vlabel(self._graph_name, label))
                else:
                    cursor.execute(SQLBuilder.create_elabel(self._graph_name, label))
                    # A new edge label invalidates the cached edge-label list so
                    # subsequent bulk_delete_orphans/bulk_delete_nodes see it.
                    self._edge_labels_cache = None
                cursor.execute("RELEASE SAVEPOINT _age_ensure_label")
                self._conn.commit()
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT _age_ensure_label")
                raise

            cursor.execute(lookup_sql, (self._graph_name, label))
            row = cursor.fetchone()

        if not row:
            raise ValueError(f"Failed to create label '{label}' in graph '{self._graph_name}'")
        return row[0]

    def _seq_name(self, label: str) -> str:
        """Return the sequence name for a label's entry IDs."""
        return f'{self._graph_name}."{label}_id_seq"'

    @staticmethod
    def _to_agtype_literal(props: dict) -> str:
        """Serialize a dict to a PostgreSQL string literal for ``::agtype`` cast.

        The agtype text-input parser interprets ``\\"`` as an escaped double
        quote inside strings (same as JSON). When the literal is embedded in a
        standard PostgreSQL ``'...'`` string, backslashes are passed through
        verbatim — so the ``\\"`` from ``escape_value`` reaches the agtype
        parser intact. This produces identical stored values to the Cypher
        UNWIND path.

        Single quotes inside values are escaped as ``''`` (PostgreSQL standard).

        Note: standard ``'...'`` strings (not ``E'...'``) are used intentionally.
        ``E'...'`` would consume the ``\\`` escape layer before agtype sees it,
        breaking ``\\"`` → ``"`` prematurely.
        """
        parts = ", ".join(f"{escape_value(k)}: {escape_value(v)}" for k, v in props.items())
        inner = "{" + parts + "}"
        # Escape single quotes for PostgreSQL standard string literal
        inner = inner.replace("'", "''")
        return "'" + inner + "'"

    def _query_graphids(self, label: str, props: Sequence[str], ref_values: set[tuple] | None) -> dict[tuple, int]:
        """Query {ref_key_tuple: graphid} for nodes of a label, uncached.

        ``ref_values`` limits the query to those keys (an empty set queries
        nothing); None loads the whole label table. Values returned by
        agtype_access_operator are agtype-quoted strings (e.g. '"mth:foo.bar"'),
        so the surrounding quotes are stripped.
        """
        if ref_values is not None and not ref_values:
            return {}
        if ref_values is not None:
            sql_stmt = SQLBuilder.lookup_node_graphids_filtered_sql(self._graph_name, label, props, ref_values)
        else:
            sql_stmt = SQLBuilder.lookup_node_graphids_sql(self._graph_name, label, props)

        with self._conn.cursor() as cursor:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
        # Each row is (val_0, ..., val_n, graphid); the key is the tuple of values.
        n = len(props)
        return {tuple(str(v).strip('"') for v in r[:n]): r[n] for r in rows}

    def _label_exists(self, label: str, kind: Literal["v", "e"] = "v") -> bool:
        """Whether the graph has a label table of this kind (checked without risking a failed statement)."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                SQL(
                    "SELECT 1 FROM ag_catalog.ag_label "
                    "WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = %s) AND name = %s AND kind = %s"
                ),
                (self._graph_name, label, kind),
            )
            return cursor.fetchone() is not None

    def invalidate_graphid_cache(self, label: str | None = None) -> None:
        """Drop cached graphid maps so the next lookup re-reads from the database.

        Call after inserting nodes into a label that is later used as an edge
        endpoint, so the graphid map includes the newly inserted nodes.

        Args:
            label: If given, only invalidate caches for this label.
                   If None, clear the entire cache.
        """
        if label is None:
            self._graphid_cache.clear()
        else:
            keys_to_drop = [k for k in self._graphid_cache if k[0] == label]
            for k in keys_to_drop:
                del self._graphid_cache[k]

    def _build_graphid_index(
        self, label: str, ref_props: Sequence[str], ref_values: set[tuple] | None = None
    ) -> dict[tuple, int]:
        """Load a {ref_key_tuple: graphid} mapping for nodes of a label.

        ``ref_props`` is the ordered key: one property for a simple key, several
        for a composite one. Keys in the returned map are value tuples aligned
        with ``ref_props`` (always a tuple, even for a single prop), so a composite
        key never collapses two nodes that merely share one property value.

        When ``ref_values`` is provided (a set of value tuples), only loads
        graphids for those specific keys (WHERE ... IN). Critical for shared
        graphs where a label table may hold hundreds of thousands of rows across
        all sources, but only a small subset is needed for the current edge batch.

        When ``ref_values`` is None, loads the entire table (used for small
        labels like structural edges).

        Results are cached per (label, ref_props) and incrementally extended
        when new ref_values are requested.

        Values returned by agtype_access_operator are agtype-quoted strings
        (e.g. '"mth:foo.bar"'), so we strip the surrounding quotes.
        """
        props = tuple(ref_props)
        cache_key = (label, props)
        cached = self._graphid_cache.get(cache_key)

        if ref_values is not None and cached is not None:
            # Check if all requested keys are already in the cache
            missing = ref_values - cached.keys()
            if not missing:
                return cached
            # Only query the missing keys
            ref_values = missing

        result = self._query_graphids(label, props, ref_values or None)

        # Merge into cache
        if cached is not None:
            cached.update(result)
            logger.debug("Graphid index extended for {}.{}: +{} entries (total {})", label, props, len(result), len(cached))
            return cached

        self._graphid_cache[cache_key] = result
        logger.debug("Graphid index built for {}.{}: {} entries", label, props, len(result))
        return result
