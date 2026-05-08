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

from typing import Literal

import psycopg
from loguru import logger
from psycopg.sql import SQL, Identifier
from psycopg.sql import Literal as SQLLiteral

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
        self._graphid_cache: dict[tuple[str, str], dict[str, int]] = {}
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
        src_ref_prop: str = "id",
        dst_ref_prop: str = "id",
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
            src_ref_prop: Property name on source nodes to match "src" values.
            dst_ref_prop: Property name on destination nodes to match "dst" values.
            batch_size: Number of rows per executemany batch.

        Returns:
            Total number of edges inserted (excludes skipped).
        """
        if not edges:
            return 0

        label_id = self._ensure_label(label, kind="e")

        # Collect the unique ref values we actually need -- avoids loading
        # the entire label table (which can be 200K+ rows in a shared graph).
        src_refs = {e["src"] for e in edges}
        dst_refs = {e["dst"] for e in edges}

        # Build {ref_value: graphid} maps for only the referenced nodes
        src_map = self._build_graphid_index(src_label, src_ref_prop, ref_values=src_refs)
        if src_label == dst_label and src_ref_prop == dst_ref_prop:
            dst_map = self._build_graphid_index(dst_label, dst_ref_prop, ref_values=src_refs | dst_refs)
        else:
            dst_map = self._build_graphid_index(dst_label, dst_ref_prop, ref_values=dst_refs)

        total = 0
        skipped = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(edges, batch_size):
                value_parts = []
                for edge in batch:
                    src_gid = src_map.get(edge["src"])
                    dst_gid = dst_map.get(edge["dst"])
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

    def _build_graphid_index(self, label: str, ref_prop: str, ref_values: set[str] | None = None) -> dict[str, int]:
        """Load a {ref_prop_value: graphid} mapping for nodes of a label.

        When ``ref_values`` is provided, only loads graphids for those specific
        property values (using a WHERE IN clause). This is critical for shared
        graphs where a label table may contain hundreds of thousands of rows
        across all sources, but only a small subset is needed for the current
        edge batch.

        When ``ref_values`` is None, loads the entire table (used for small
        labels like structural edges).

        Results are cached per (label, ref_prop) and incrementally extended
        when new ref_values are requested.

        Values returned by agtype_access_operator are agtype-quoted strings
        (e.g. '"mth:foo.bar"'), so we strip the surrounding quotes.
        """
        cache_key = (label, ref_prop)
        cached = self._graphid_cache.get(cache_key)

        if ref_values is not None and cached is not None:
            # Check if all requested values are already in the cache
            missing = ref_values - cached.keys()
            if not missing:
                return cached
            # Only query the missing values
            ref_values = missing

        if ref_values is not None and ref_values:
            sql_stmt = SQLBuilder.lookup_node_graphids_filtered_sql(self._graph_name, label, ref_prop, ref_values)
        else:
            sql_stmt = SQLBuilder.lookup_node_graphids_sql(self._graph_name, label, ref_prop)

        with self._conn.cursor() as cursor:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
        result = {str(r[0]).strip('"'): r[1] for r in rows}

        # Merge into cache
        if cached is not None:
            cached.update(result)
            logger.debug("Graphid index extended for {}.{}: +{} entries (total {})", label, ref_prop, len(result), len(cached))
            return cached

        self._graphid_cache[cache_key] = result
        logger.debug("Graphid index built for {}.{}: {} entries", label, ref_prop, len(result))
        return result
